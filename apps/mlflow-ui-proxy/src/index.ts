// Run with bun. Wrangler entry; re-exports the worker default and Container DO.
import { Container } from "@cloudflare/containers";
import { readSyncOutput } from "./exec-output";
import worker from "./worker";
import { readSourceFingerprints } from "./source-fingerprint";
import { syncChangedDays } from "./sync-checkpoint";
import type { SyncCheckpoint } from "./sync-checkpoint";
import type { Env, MlflowSyncResult } from "./types";

const DEFAULT_PORT = 8080;
const SLEEP_AFTER = "5m";
const MLFLOW_BUCKET_PREFIX_SEPARATOR = "/";
const PYTHON_WORKDIR = "/app/mlflow";
const CONTAINER_START_TIMEOUT_MS = 120_000;
const PORT_CHECK_INTERVAL_MS = 500;
const SYNC_IDLE_GRACE: string = "30s";
const UI_IDLE_GRACE_MS: number = 5 * 60 * 1000;

const buildArtifactDestination = (env: Env): string =>
  `s3://${env.HORSE_RACING_MLFLOW_R2_BUCKET}${MLFLOW_BUCKET_PREFIX_SEPARATOR}${env.HORSE_RACING_MLFLOW_R2_PREFIX}`;

export class MlflowContainer extends Container<Env> {
  override defaultPort = DEFAULT_PORT;
  override sleepAfter = SLEEP_AFTER;
  override enableInternet = true;
  private pendingSync: Promise<MlflowSyncResult> | undefined;
  private processCompletion: Promise<void> | undefined;
  private syncActive: boolean = false;
  private lastUiActivity: number = Number.NEGATIVE_INFINITY;

  override async onActivityExpired(): Promise<void> {
    if (this.syncActive || this.processCompletion !== undefined) {
      this.sleepAfter = SLEEP_AFTER;
      this.renewActivityTimeout();
      return;
    }
    await super.onActivityExpired();
  }

  private configureEnv(): void {
    const r2Endpoint = `https://${this.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
    this.envVars = {
      AWS_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID,
      AWS_DEFAULT_REGION: "auto",
      AWS_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY,
      HORSE_RACING_MLFLOW_ARTIFACTS_MODE: "r2",
      HORSE_RACING_MLFLOW_BACKEND_URI: this.env.HORSE_RACING_MLFLOW_BACKEND_URI,
      HORSE_RACING_MLFLOW_R2_BUCKET: this.env.HORSE_RACING_MLFLOW_R2_BUCKET,
      HORSE_RACING_MLFLOW_R2_PREFIX: this.env.HORSE_RACING_MLFLOW_R2_PREFIX,
      MLFLOW_ARTIFACTS_DESTINATION: buildArtifactDestination(this.env),
      MLFLOW_BACKEND_STORE_URI: this.env.HORSE_RACING_MLFLOW_BACKEND_URI,
      MLFLOW_S3_ENDPOINT_URL: r2Endpoint,
      MLFLOW_SERVER_ALLOWED_HOSTS: "*",
      MLFLOW_SERVER_ENABLE_JOB_EXECUTION: "false",
      NEON_PRIMARY_URL: this.env.NEON_PRIMARY_URL,
      PYTHONUNBUFFERED: "1",
      R2_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID,
      R2_ACCOUNT_ID: this.env.R2_ACCOUNT_ID,
      R2_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY,
    };
  }

  private waitUntilReady(): Promise<void> {
    return this.startAndWaitForPorts({
      cancellationOptions: {
        instanceGetTimeoutMS: CONTAINER_START_TIMEOUT_MS,
        portReadyTimeoutMS: CONTAINER_START_TIMEOUT_MS,
        waitInterval: PORT_CHECK_INTERVAL_MS,
      },
    });
  }

  override async fetch(request: Request): Promise<Response> {
    this.lastUiActivity = Date.now();
    this.sleepAfter = SLEEP_AFTER;
    this.configureEnv();
    try {
      await this.waitUntilReady();
      return await this.containerFetch(request);
    } finally {
      this.lastUiActivity = Date.now();
      this.sleepAfter = SLEEP_AFTER;
      this.renewActivityTimeout();
    }
  }

  syncProductionPreview(dateFrom: string, dateTo: string): Promise<MlflowSyncResult> {
    const previous = this.pendingSync ?? Promise.resolve();
    const next = previous
      .catch(() => undefined)
      // A timeout bounds the caller, not the lifetime of an unconfirmed process.
      .then(() => this.processCompletion)
      .then(() => this.syncChangedPreview(dateFrom, dateTo));
    this.pendingSync = next;
    return next;
  }

  private async syncChangedPreview(dateFrom: string, dateTo: string): Promise<MlflowSyncResult> {
    this.syncActive = true;
    const outputs: string[] = [];
    try {
      if (this.env.MLFLOW_SOURCE_GATE_ENABLED !== "1") {
        return await this.executePreview(dateFrom, dateTo);
      }
      await syncChangedDays({
        dateFrom,
        dateTo,
        now: Date.now(),
        probe: () =>
          readSourceFingerprints({ connectionString: this.env.NEON_PRIMARY_URL, dateFrom, dateTo }),
        store: {
          get: (key) => this.ctx.storage.get<SyncCheckpoint>(key),
          put: (key, value) => this.ctx.storage.put(key, value),
        },
        sync: async (from, to) => {
          const result = await this.executePreview(from, to);
          if (result.exitCode !== 0)
            throw new Error(`MLflow preview sync failed: ${result.stderr}`);
          outputs.push(result.stdout);
        },
      });
      return { exitCode: 0, stdout: outputs.join("\n"), stderr: "" };
    } finally {
      this.syncActive = false;
      this.sleepAfter =
        Date.now() - this.lastUiActivity < UI_IDLE_GRACE_MS ? SLEEP_AFTER : SYNC_IDLE_GRACE;
      this.renewActivityTimeout();
    }
  }

  private async executePreview(dateFrom: string, dateTo: string): Promise<MlflowSyncResult> {
    this.configureEnv();
    await this.waitUntilReady();
    const container = this.ctx.container;
    if (container === undefined) {
      throw new Error("Cloudflare Container runtime is unavailable");
    }
    const process = await container.exec(
      [
        "python",
        "-m",
        "mlflow_tracking.cli",
        "sync-production-preview",
        "--date-from",
        dateFrom,
        "--date-to",
        dateTo,
        "--categories",
        "jra,nar,banei",
      ],
      {
        cwd: PYTHON_WORKDIR,
        env: this.envVars,
        stderr: "pipe",
        stdout: "pipe",
      },
    );
    const pendingOutput = process.output();
    const completion = pendingOutput.then(
      () => undefined,
      () => undefined,
    );
    this.processCompletion = completion;
    void completion.then(() => {
      if (this.processCompletion === completion) this.processCompletion = undefined;
    });
    const output = await readSyncOutput({
      output: () => pendingOutput,
      kill: (signal) => process.kill(signal),
    });
    const decoder = new TextDecoder();
    return {
      exitCode: output.exitCode,
      stderr: decoder.decode(output.stderr),
      stdout: decoder.decode(output.stdout),
    };
  }
}

export default worker;
