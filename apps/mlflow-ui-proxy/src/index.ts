// Run with bun. Wrangler entry; re-exports the worker default and Container DO.
import { Container } from "@cloudflare/containers";
import worker from "./worker";
import type { Env, MlflowSyncResult } from "./types";

const DEFAULT_PORT = 8080;
const SLEEP_AFTER = "5m";
const MLFLOW_BUCKET_PREFIX_SEPARATOR = "/";
const PYTHON_WORKDIR = "/app/mlflow";
const CONTAINER_START_TIMEOUT_MS = 120_000;
const PORT_CHECK_INTERVAL_MS = 500;

const buildArtifactDestination = (env: Env): string =>
  `s3://${env.HORSE_RACING_MLFLOW_R2_BUCKET}${MLFLOW_BUCKET_PREFIX_SEPARATOR}${env.HORSE_RACING_MLFLOW_R2_PREFIX}`;

export class MlflowContainer extends Container<Env> {
  override defaultPort = DEFAULT_PORT;
  override sleepAfter = SLEEP_AFTER;
  override enableInternet = true;

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
    this.configureEnv();
    await this.waitUntilReady();
    return this.containerFetch(request);
  }

  async syncProductionPreview(dateFrom: string, dateTo: string): Promise<MlflowSyncResult> {
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
    const output = await process.output();
    const decoder = new TextDecoder();
    return {
      exitCode: output.exitCode,
      stderr: decoder.decode(output.stderr),
      stdout: decoder.decode(output.stdout),
    };
  }
}

export default worker;
