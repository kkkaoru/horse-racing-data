// Run with bun. Durable-Object-backed Container class for the predictor image.
// Queue/admin callers hit stub.fetch("/predict?...") and the DO proxies through
// containerFetch. Focused full returns "accepted" quickly while the Python
// process continues in a detached thread; sleepAfter keeps the category
// container alive long enough for Neon completion polling to observe the write.
// container-class.ts is excluded from the coverage gate (see vitest.config.ts).
//
// Detached day-base builds must survive the complete DAY_CHAIN, not only the
// RS foundation step.  In production the foundation can land near the
// twenty-minute boundary while the remaining layers are still running; when
// the activity lease expired at 20m the Container disappeared with no final
// feat-daybase object, causing repeated foundation-only pickups. 45m is a
// bounded upper limit for the build deadline. Successful paths hand off an
// explicit destroy through container-cleanup.ts, so this does not turn an
// otherwise idle instance into a permanent charge.

import { checkContainerExecution } from "./do-state";
import { waitForContainerStopped } from "./container-control";
import { Container, type StopParams } from "@cloudflare/containers";
import { shouldRenewFocusedFullActivity } from "./container-status-activity";
import {
  buildFocusedFullCompletionCallbackUrl,
  FOCUSED_FULL_CALLBACK_HEADER,
} from "./focused-full-callback";
import { proxyDayBaseParquetFromNdjson, proxyParquetFromNdjson } from "./container-ndjson-proxy";
import {
  createFocusedFullWatchTickMessage,
  FOCUSED_FULL_WATCH_POLL_SECONDS,
  hasAcceptedResult,
  isFocusedFullPredictUrl,
  parseFocusedFullWatchHeader,
  sendFocusedFullWatchMessageDurably,
  WATCH_RESPONSE_HEADER,
} from "./focused-full-watch";
import type { ValidatedFocusedFullWatchPayload } from "./focused-full-watch";
import { PREDICT_DO_INTERNAL_PURGE_PATH } from "./predict-do-state-purge";
import type { Env, PredictionContainerRole as PredictContainerRole } from "./types";

type PredictContainerEnvironment = Pick<
  Env,
  | "DAY_BASE_SPLIT_ENABLED"
  | "NAR_TRANSFORMER_BLEND_ENABLED"
  | "NEON_DATABASE_URL"
  | "PIPELINE_TOTAL_TIMEOUT_SECONDS"
  | "PREDICT_DAYS_AHEAD"
  | "PROPHET_SCORE_ADJUSTMENT_ENABLED"
  | "PROPHET_SCORE_ADJUSTMENT_WEIGHT"
  | "RACE_CHAIN_FUSED_ENABLED"
  | "R2_ACCESS_KEY_ID"
  | "R2_ACCOUNT_ID"
  | "R2_BUCKET"
  | "R2_CATALOG_TOKEN"
  | "R2_CATALOG_URI"
  | "R2_CATALOG_WAREHOUSE"
  | "R2_SECRET_ACCESS_KEY"
  | "SOURCE_DATABASE_URL"
  | "STAGE1_PRESERVED_ODDS_GATE_ENABLED"
  | "VENUE_WEATHER_URL"
  | "WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED"
>;

export interface BuildPredictContainerEnvVarsOptions {
  env: PredictContainerEnvironment;
  inheritedEnvVars: Readonly<Record<string, string>>;
}

interface ContainerExecutionDisposition {
  legacy?: boolean;
  executionId: string;
  disposition: "preparing" | "dispatching" | "active" | "stopping" | "stopped";
}

const EXECUTION_KEY: string = "prediction-execution";
const EXECUTION_HEADER: string = "x-prediction-execution-id";
const DEFAULT_PORT = 8080;
const CONTAINER_INSTANCE_GET_TIMEOUT_MS = 20_000;
const CONTAINER_PORT_READY_TIMEOUT_MS = 60_000;
const CONTAINER_DELIVERY_HARD_TIMEOUT_MS = 65_000;
// Day-base SQL is the only long container call. A Workflow step holds this
// socket and commits R2 from the stream; the container must not detach and
// then stay warm for a pickup that can miss the in-memory payload.
export const SYNC_DAY_BASE_TIMEOUT_MS = 25 * 60 * 1000;

export const containerDeliveryTimeoutMs = (url: URL): number =>
  url.pathname === PREWARM_DAY_BASE_PATH && url.searchParams.get("sync") === "1"
    ? SYNC_DAY_BASE_TIMEOUT_MS
    : CONTAINER_DELIVERY_HARD_TIMEOUT_MS;
const CONTAINER_DESTROY_HARD_TIMEOUT_MS = 15_000;
const CONTAINER_STATUS_HARD_TIMEOUT_MS = 5_000;
// 20m covers a detached first-day day-base build (10–15m) plus race-chain.
const SLEEP_AFTER = "45m";
// Race-chain status is polled every 30 seconds while work is active, but a
// focused-full /predict answers `accepted` immediately and detaches the real
// run, so polling can end while the container is still building. A 地方 NAR run
// measured 986s (16.4m) end-to-end on 2026-09-22 while this bound was two
// minutes: the container slept mid-run, no prediction row reached Neon and
// every 地方 race lost its 着順予測. Keep the idle fallback at the pipeline's own
// ceiling (PIPELINE_TOTAL_TIMEOUT_SECONDS = 1800s) so a container can never be
// reclaimed before the run it is executing has given up on itself.
export const RACE_CHAIN_SLEEP_AFTER = "30m";
const MODELS_DIR_DEFAULT = "/models";
const PIPELINE_TOTAL_TIMEOUT_SECONDS_DEFAULT = "1800";
const EMPTY_ENV_VALUE = "";
const EMPTY_ENV_VARS: Readonly<Record<string, string>> = Object.freeze({});
const ADMIN_STOP_PATH = "/__admin/stop-container";
const PREWARM_DAY_BASE_PATH = "/prewarm-day-base";
const PREWARM_DAY_BASE_STATUS_PATH = "/prewarm-day-base-status";
const PREWARM_DAY_BASE_CACHE_PATH = "/prewarm-day-base-cache";
const FOCUSED_FULL_STATUS_PATH = "/focused-full-status";
const FOCUSED_FULL_CACHE_PATH = "/focused-full-cache";
const AUTH_HEADER = "authorization";
const BEARER_PREFIX = "Bearer ";
const LEGACY_CONTAINER_ROLE: PredictContainerRole = "legacy";
const RACE_CHAIN_CONTAINER_ROLE: PredictContainerRole = "race-chain";
const RESCORE_CONTAINER_ROLE: PredictContainerRole = "rescore";
const RESCORE_SLEEP_AFTER: string = "10s";
const RESCORE_ONLY_ERROR: string = "RESCORE_ONLY_REQUIRED";
const PREDICT_PATH: string = "/predict";
const RESCORE_MODE: string = "rescore";
const HTTP_BAD_REQUEST: number = 400;

const withHardTimeout = async <T>(operation: Promise<T>, timeoutMs: number): Promise<T> => {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timeoutId = setTimeout(
      () => reject(new Error(`Container delivery readiness exceeded ${timeoutMs}ms`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([operation, timeout]);
  } finally {
    if (timeoutId !== undefined) clearTimeout(timeoutId);
  }
};

const withReadOnlyStatusTimeout = async <T>(operation: Promise<T>): Promise<T> =>
  withHardTimeout(operation, CONTAINER_STATUS_HARD_TIMEOUT_MS);

const mergePredictContainerEnvVars = (
  { env, inheritedEnvVars }: BuildPredictContainerEnvVarsOptions,
  role: PredictContainerRole,
): Record<string, string> => ({
  ...inheritedEnvVars,
  MODELS_DIR: MODELS_DIR_DEFAULT,
  NEON_DATABASE_URL: env.NEON_DATABASE_URL,
  PIPELINE_TOTAL_TIMEOUT_SECONDS:
    env.PIPELINE_TOTAL_TIMEOUT_SECONDS ?? PIPELINE_TOTAL_TIMEOUT_SECONDS_DEFAULT,
  PREDICT_DAYS_AHEAD: env.PREDICT_DAYS_AHEAD,
  PREDICT_SERVE_MODE: "http",
  NAR_TRANSFORMER_BLEND_ENABLED: env.NAR_TRANSFORMER_BLEND_ENABLED ?? EMPTY_ENV_VALUE,
  PROPHET_SCORE_ADJUSTMENT_ENABLED: env.PROPHET_SCORE_ADJUSTMENT_ENABLED ?? EMPTY_ENV_VALUE,
  PROPHET_SCORE_ADJUSTMENT_WEIGHT: env.PROPHET_SCORE_ADJUSTMENT_WEIGHT ?? EMPTY_ENV_VALUE,
  STAGE1_PRESERVED_ODDS_GATE_ENABLED: env.STAGE1_PRESERVED_ODDS_GATE_ENABLED ?? EMPTY_ENV_VALUE,
  DAY_BASE_SPLIT_ENABLED: env.DAY_BASE_SPLIT_ENABLED ?? EMPTY_ENV_VALUE,
  SOURCE_DATABASE_URL: env.SOURCE_DATABASE_URL ?? EMPTY_ENV_VALUE,
  R2_ACCOUNT_ID: env.R2_ACCOUNT_ID ?? EMPTY_ENV_VALUE,
  R2_ACCESS_KEY_ID: env.R2_ACCESS_KEY_ID ?? EMPTY_ENV_VALUE,
  R2_SECRET_ACCESS_KEY: env.R2_SECRET_ACCESS_KEY ?? EMPTY_ENV_VALUE,
  R2_BUCKET: env.R2_BUCKET ?? EMPTY_ENV_VALUE,
  R2_CATALOG_TOKEN: env.R2_CATALOG_TOKEN ?? EMPTY_ENV_VALUE,
  R2_CATALOG_URI: env.R2_CATALOG_URI ?? EMPTY_ENV_VALUE,
  R2_CATALOG_WAREHOUSE: env.R2_CATALOG_WAREHOUSE ?? EMPTY_ENV_VALUE,
  VENUE_WEATHER_URL: env.VENUE_WEATHER_URL ?? EMPTY_ENV_VALUE,
  // The Queue consumer materializes live odds/market-derived columns from
  // Cloudflare edge data into an attested R2 artifact before Container start.
  // Forward the same gate so the race-chain consumes that warm artifact and
  // skips its local market-signal subprocess.
  WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED:
    env.WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED ?? EMPTY_ENV_VALUE,
  RACE_CHAIN_FUSED_ENABLED: env.RACE_CHAIN_FUSED_ENABLED ?? EMPTY_ENV_VALUE,
  PYTHONUNBUFFERED: "1",
  // Role is authoritative class configuration. Keep it after inherited vars
  // so per-instance Container options cannot impersonate another resource role.
  PREDICT_CONTAINER_ROLE: role,
});

export const buildLegacyPredictContainerEnvVars = (
  options: BuildPredictContainerEnvVarsOptions,
): Record<string, string> => mergePredictContainerEnvVars(options, LEGACY_CONTAINER_ROLE);

export const buildRaceChainPredictContainerEnvVars = (
  options: BuildPredictContainerEnvVarsOptions,
): Record<string, string> => mergePredictContainerEnvVars(options, RACE_CHAIN_CONTAINER_ROLE);

export const buildRescorePredictContainerEnvVars = (
  options: BuildPredictContainerEnvVarsOptions,
): Record<string, string> => mergePredictContainerEnvVars(options, RESCORE_CONTAINER_ROLE);

const isDebugRequest = (url: URL): boolean => {
  const value = url.searchParams.get("debug");
  if (value === null) return false;
  return ["1", "true", "yes", "on", "debug"].includes(value.trim().toLowerCase());
};

const describePredictRequest = (url: URL): string => {
  const searchParams = url.searchParams;
  return [
    `path=${url.pathname}`,
    `category=${searchParams.get("category") ?? "-"}`,
    `runDate=${searchParams.get("runDate") ?? "-"}`,
    `mode=${searchParams.get("mode") ?? "-"}`,
    `keibajo=${searchParams.get("keibajoCode") ?? "-"}`,
    `race=${searchParams.get("raceBango") ?? "-"}`,
  ].join(" ");
};

const isAuthorizedAdmin = (request: Request, token: string): boolean => {
  const header = request.headers.get(AUTH_HEADER);
  if (!header?.startsWith(BEARER_PREFIX)) return false;
  return header.slice(BEARER_PREFIX.length) === token;
};

const parseWatchPayload = (
  request: Request,
  enabled: boolean,
): ValidatedFocusedFullWatchPayload | Response | undefined => {
  if (!enabled) return undefined;
  try {
    return parseFocusedFullWatchHeader(request);
  } catch (error) {
    return Response.json({ error: String(error) }, { status: 400 });
  }
};

const buildMissingPrewarmStatusResponse = (url: URL): Response => {
  const category = url.searchParams.get("category") ?? "";
  const runDate = url.searchParams.get("runDate") ?? "";
  return Response.json({
    error: null,
    finishedAtMs: null,
    flightKey: `${category}:${runDate}`,
    generation: 0,
    startedAtMs: null,
    status: "missing",
  });
};

export class FinishPositionPredictContainer extends Container<Env> {
  private stoppingExecutionId?: string;
  private dispatchingExecutionId?: string;
  private pendingReadinessExecutionId?: string;
  override defaultPort = DEFAULT_PORT;
  override sleepAfter = SLEEP_AFTER;
  override enableInternet = true;

  override async onActivityExpired(): Promise<void> {
    // The SDK logs expiry even when already stopped; record the runtime flag
    // before delegating, without renewing activity or changing its signal policy.
    console.log("[predict-container-lifecycle] idle-expired", {
      running: this.ctx.container?.running === true,
    });
    await super.onActivityExpired();
  }

  override onStop(params: StopParams): void {
    // Worker log metadata supplies the DO and version identity for correlation.
    // An expiry callback alone is not evidence of a successful process exit.
    console.log("[predict-container-lifecycle] stopped", params);
  }

  protected buildContainerEnvVars(): Record<string, string> {
    return buildLegacyPredictContainerEnvVars({
      env: this.env,
      inheritedEnvVars: this.envVars ?? EMPTY_ENV_VARS,
    });
  }

  private async admitLegacyDispatch(): Promise<string | undefined> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const current = await this.ctx.storage.get<ContainerExecutionDisposition>(EXECUTION_KEY);
      if (
        this.dispatchingExecutionId !== undefined ||
        this.stoppingExecutionId !== undefined ||
        this.pendingReadinessExecutionId !== undefined
      )
        return undefined;
      const physicallyStopped = this.ctx.container?.running === false;
      if (current === undefined && !physicallyStopped) return undefined;
      if (
        current !== undefined &&
        current.legacy !== true &&
        (current.disposition !== "stopped" || !physicallyStopped)
      )
        return undefined;
      if (current?.legacy === true && !physicallyStopped && current.disposition !== "active")
        return undefined;
      const executionId = crypto.randomUUID();
      await this.ctx.storage.put<ContainerExecutionDisposition>(EXECUTION_KEY, {
        executionId,
        legacy: true,
        disposition: "preparing",
      });
      this.dispatchingExecutionId = executionId;
      return executionId;
    });
  }

  private async admitExecution(executionId: string): Promise<"dispatch" | "duplicate" | "blocked"> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const current = await this.ctx.storage.get<ContainerExecutionDisposition>(EXECUTION_KEY);
      if (
        this.dispatchingExecutionId !== undefined ||
        this.stoppingExecutionId !== undefined ||
        this.pendingReadinessExecutionId !== undefined
      )
        return "blocked";
      if (current?.executionId === executionId)
        return current.disposition === "active" ? "duplicate" : "blocked";
      // An unknown live process has no trustworthy owner. Let it drain.
      if (current === undefined && this.ctx.container?.running === true) return "blocked";
      // Only a stopped physical legacy process can cross into a fenced owner.
      // Fenced owners must finish exact-E cleanup before replacement admission.
      if (
        current !== undefined &&
        !(current.legacy === true && this.ctx.container?.running === false) &&
        (current.disposition !== "stopped" || this.ctx.container?.running !== false)
      )
        return "blocked";
      await this.ctx.storage.put<ContainerExecutionDisposition>(EXECUTION_KEY, {
        executionId,
        disposition: "preparing",
      });
      this.dispatchingExecutionId = executionId;
      return "dispatch";
    });
  }

  private async setExecutionDisposition(
    executionId: string,
    disposition: ContainerExecutionDisposition["disposition"],
  ): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const current = await this.ctx.storage.get<ContainerExecutionDisposition>(EXECUTION_KEY);
      if (current?.executionId !== executionId)
        throw new Error("Container execution ownership lost");
      await this.ctx.storage.put<ContainerExecutionDisposition>(EXECUTION_KEY, {
        ...current,
        executionId,
        disposition,
      });
    });
  }

  private async stopExecution(request: Request): Promise<Response> {
    const expectedId = request.headers.get(EXECUTION_HEADER);
    const decision = await this.ctx.blockConcurrencyWhile(async () => {
      const stored = await this.ctx.storage.get<ContainerExecutionDisposition>(EXECUTION_KEY);
      if (stored === undefined && this.ctx.container?.running !== false)
        return { state: "blocked" };
      const current: ContainerExecutionDisposition = stored ?? {
        executionId: crypto.randomUUID(),
        legacy: true,
        disposition: "active",
      };
      const matches =
        expectedId === null ? current.legacy === true : current.executionId === expectedId;
      if (!matches) return { state: "blocked" };
      if (
        request.headers.get("x-prediction-require-stopped") === "1" &&
        this.ctx.container?.running !== false
      )
        return { state: "pending" };
      if (
        this.pendingReadinessExecutionId === current.executionId ||
        this.dispatchingExecutionId === current.executionId ||
        this.stoppingExecutionId === current.executionId
      )
        return { state: "pending" };
      // Delivery may have reached Python. Timeouts are not terminal evidence.
      if (
        (current.disposition === "dispatching" || current.disposition === "stopped") &&
        this.ctx.container?.running !== false
      )
        return { state: "pending" };
      await this.ctx.storage.put<ContainerExecutionDisposition>(EXECUTION_KEY, {
        ...current,
        disposition: "stopping",
      });
      this.stoppingExecutionId = current.executionId;
      return {
        state: this.ctx.container?.running === false ? "confirm" : "destroy",
        executionId: current.executionId,
      };
    });
    if (decision.state === "blocked") return Response.json({ ok: false }, { status: 409 });
    if (decision.executionId === undefined)
      return Response.json({ ok: false, state: "unresolved" }, { status: 425 });
    try {
      if (decision.state === "destroy") await this.destroy();
      await waitForContainerStopped("owned-execution", this);
      if (this.ctx.container?.running !== false)
        throw new Error("Container runtime has not stopped");
      await this.setExecutionDisposition(decision.executionId, "stopped");
      return Response.json({ ok: true, executionId: decision.executionId });
    } finally {
      // Persisted stopping remains authoritative after rejection or write failure.
      // A later exact-E call can retry; a concurrent destroy still holds this guard.
      if (this.stoppingExecutionId === decision.executionId) this.stoppingExecutionId = undefined;
    }
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const requestSummary = describePredictRequest(url);
    if (url.pathname === ADMIN_STOP_PATH) {
      if (!isAuthorizedAdmin(request, this.env.TRIGGER_TOKEN)) {
        console.warn(`[predict-container-do] admin-stop unauthorized ${requestSummary}`);
        return Response.json({ error: "unauthorized", ok: false }, { status: 401 });
      }
      console.log(`[predict-container-do] admin-stop requested ${requestSummary}`);
      return this.stopExecution(request);
    }
    if (url.pathname === PREDICT_DO_INTERNAL_PURGE_PATH) {
      if (!isAuthorizedAdmin(request, this.env.TRIGGER_TOKEN)) {
        console.warn(`[predict-container-do] admin-purge unauthorized ${requestSummary}`);
        return Response.json({ error: "unauthorized", ok: false }, { status: 401 });
      }
      console.log(`[predict-container-do] admin-purge requested ${requestSummary}`);
      const stopped = await this.stopExecution(request);
      if (!stopped.ok) return stopped;
      console.log(`[predict-container-do] admin-purge completed ${requestSummary}`);
      return Response.json({ ok: true, purged: true });
    }
    if (url.pathname === PREWARM_DAY_BASE_STATUS_PATH) {
      const runtimeContainer = this.ctx.container;
      if (runtimeContainer?.running !== true) return buildMissingPrewarmStatusResponse(url);
      try {
        // This endpoint is observational. Calling the SDK's containerFetch or
        // startAndWaitForPorts here would start a stopped standard-4 merely to
        // report "missing". The runtime handle's running flag is the source of
        // truth even when the SDK's persisted lifecycle state is stale; when
        // it is live, address that existing process directly without renewing
        // the activity lease.
        const statusRequest = new Request(request, {
          signal: AbortSignal.timeout(CONTAINER_STATUS_HARD_TIMEOUT_MS),
        });
        return await withReadOnlyStatusTimeout(
          runtimeContainer.getTcpPort(DEFAULT_PORT).fetch(statusRequest),
        );
      } catch (error) {
        return Response.json({ error: String(error), status: "unavailable" }, { status: 503 });
      }
    }
    if (
      url.pathname === FOCUSED_FULL_STATUS_PATH ||
      url.pathname === FOCUSED_FULL_CACHE_PATH ||
      url.pathname === PREWARM_DAY_BASE_CACHE_PATH
    ) {
      const runtimeContainer = this.ctx.container;
      const raceKey = `${url.searchParams.get("category")}:${url.searchParams.get("runDate")?.replaceAll("-", "")}:${url.searchParams.get("keibajoCode")}:${url.searchParams.get("raceBango")}`;
      if (runtimeContainer?.running !== true) {
        return url.pathname === FOCUSED_FULL_STATUS_PATH
          ? Response.json({ error: null, raceKey, status: "missing" })
          : Response.json({ error: "Container is not running" }, { status: 404 });
      }
      try {
        // A late watch/cache retry must not boot a fresh placement or refresh
        // idle activity just to observe a completed/missing race.
        const response = await withReadOnlyStatusTimeout(
          runtimeContainer.getTcpPort(DEFAULT_PORT).fetch(
            new Request(request, {
              signal: AbortSignal.timeout(CONTAINER_STATUS_HARD_TIMEOUT_MS),
            }),
          ),
        );
        if (response.ok && url.pathname === FOCUSED_FULL_STATUS_PATH) {
          const payload: unknown = await withReadOnlyStatusTimeout(response.clone().json());
          if (shouldRenewFocusedFullActivity({ payload, raceKey, nowMs: Date.now() }))
            this.renewActivityTimeout();
        }
        return response;
      } catch (error: unknown) {
        // Observation failure is not authority to destroy another active job.
        return Response.json({ error: String(error), status: "unavailable" }, { status: 503 });
      }
    }
    const watchEnabled =
      this.env.FOCUSED_FULL_WATCH_ENABLED === "1" &&
      this.env.FOCUSED_FULL_COMPLETION_QUEUE !== undefined &&
      isFocusedFullPredictUrl(url);
    const watchPayload = parseWatchPayload(request, watchEnabled);
    if (watchPayload instanceof Response) return watchPayload;
    const executionId = watchPayload?.executionId;
    const legacyDispatchId =
      executionId === undefined ? await this.admitLegacyDispatch() : undefined;
    if (executionId === undefined && legacyDispatchId === undefined)
      return Response.json(
        { error: "Execution identity required or dispatch pending" },
        { status: 409 },
      );
    if (executionId !== undefined && watchPayload !== undefined) {
      if (
        !(await checkContainerExecution({
          env: this.env,
          doName: watchPayload.doName,
          workKey: watchPayload.workKey,
          executionId,
        }))
      ) {
        return Response.json({ error: "Stale execution" }, { status: 409 });
      }
      const admission = await this.admitExecution(executionId);
      if (admission === "blocked")
        return Response.json({ error: "Execution disposition unknown or busy" }, { status: 425 });
      if (admission === "duplicate") {
        await sendFocusedFullWatchMessageDurably(
          this.env,
          createFocusedFullWatchTickMessage(watchPayload, Date.now()),
          FOCUSED_FULL_WATCH_POLL_SECONDS,
        );
        return new Response(JSON.stringify({ type: "result", status: "accepted" }), {
          headers: { [WATCH_RESPONSE_HEADER]: watchPayload.watchId },
        });
      }
    }
    this.envVars = this.buildContainerEnvVars();
    const startedAt = Date.now();
    const debug = isDebugRequest(url);
    try {
      if (executionId !== undefined && watchPayload !== undefined) {
        // Register before readiness/delivery: interrupted dispatch also has an outbox watchdog.
        await sendFocusedFullWatchMessageDurably(
          this.env,
          createFocusedFullWatchTickMessage(watchPayload, startedAt),
          FOCUSED_FULL_WATCH_POLL_SECONDS,
        );
      }
      // `destroy()` does not synchronously rewrite the SDK's persisted
      // `healthy` state. A rapid same-DO restart can therefore observe
      // `container.running=true` plus stale `healthy` and make
      // `containerFetch()` skip its readiness check. The ensuing tcpPort.fetch
      // is unbounded and was observed holding a standard-4 for 32 minutes
      // without delivering the request. Force a bounded port check on every
      // application request; for an already healthy placement this is one
      // cheap ping, while a replacement cannot receive work before :8080 is
      // actually listening.
      console.log(`[predict-container-do] delivery start ${requestSummary}`);
      const readinessId = executionId ?? legacyDispatchId;
      this.pendingReadinessExecutionId = readinessId;
      await withHardTimeout(
        this.startAndWaitForPorts([DEFAULT_PORT], {
          abort: AbortSignal.timeout(CONTAINER_PORT_READY_TIMEOUT_MS),
          instanceGetTimeoutMS: CONTAINER_INSTANCE_GET_TIMEOUT_MS,
          portReadyTimeoutMS: CONTAINER_PORT_READY_TIMEOUT_MS,
        }).finally(() => {
          if (this.pendingReadinessExecutionId === readinessId)
            this.pendingReadinessExecutionId = undefined;
        }),
        CONTAINER_DELIVERY_HARD_TIMEOUT_MS,
      );
      console.log(
        `[predict-container-do] delivery port-ready ${requestSummary} durationMs=${
          Date.now() - startedAt
        }`,
      );
      const callbackUrl =
        watchPayload === undefined
          ? undefined
          : await buildFocusedFullCompletionCallbackUrl(this.env, watchPayload);
      const forwardedRequest =
        callbackUrl === undefined
          ? request
          : new Request(request, {
              headers: new Headers([
                ...request.headers.entries(),
                [FOCUSED_FULL_CALLBACK_HEADER, callbackUrl],
              ]),
            });
      if (
        executionId !== undefined &&
        watchPayload !== undefined &&
        !(await checkContainerExecution({
          env: this.env,
          doName: watchPayload.doName,
          workKey: watchPayload.workKey,
          executionId,
        }))
      ) {
        // Readiness may have crossed a coordinator ownership change. Never dispatch.
        return Response.json({ error: "Stale execution after readiness" }, { status: 409 });
      }
      const dispatchedId = executionId ?? legacyDispatchId;
      if (dispatchedId !== undefined)
        await this.setExecutionDisposition(dispatchedId, "dispatching");
      const response = await withHardTimeout(
        this.containerFetch(forwardedRequest),
        containerDeliveryTimeoutMs(url),
      );
      console.log(
        `[predict-container-do] delivery headers ${requestSummary} status=${response.status} durationMs=${
          Date.now() - startedAt
        }`,
      );
      const accepted =
        watchPayload === undefined ? false : await hasAcceptedResult(response.clone());
      if (dispatchedId !== undefined && (executionId === undefined || response.ok))
        await this.setExecutionDisposition(dispatchedId, "active");
      const proxied =
        url.pathname === PREWARM_DAY_BASE_PATH
          ? proxyDayBaseParquetFromNdjson({
              debug,
              env: this.env,
              renewActivityTimeout: this.renewActivityTimeout.bind(this),
              response,
              waitUntil: this.ctx.waitUntil.bind(this.ctx),
            })
          : proxyParquetFromNdjson(
              response,
              watchPayload === undefined
                ? this.env
                : {
                    ...this.env,
                    focusedFullCommit: {
                      env: this.env,
                      executionId,
                      workKey: watchPayload.workKey,
                    },
                  },
              this.ctx.waitUntil.bind(this.ctx),
              this.renewActivityTimeout.bind(this),
              debug,
            );
      if (!accepted || watchPayload === undefined) return proxied;
      await sendFocusedFullWatchMessageDurably(
        this.env,
        createFocusedFullWatchTickMessage(watchPayload, Date.now()),
        FOCUSED_FULL_WATCH_POLL_SECONDS,
      );
      const watchedResponse = new Response(proxied.body, proxied);
      watchedResponse.headers.set(WATCH_RESPONSE_HEADER, watchPayload.watchId);
      return watchedResponse;
    } catch (err) {
      console.error(
        `[predict-container-do] fetch failed ${requestSummary} durationMs=${
          Date.now() - startedAt
        }: ${String(err)}`,
      );
      // An unresolved dispatch may still be executing after a timeout. Keep its
      // durable disposition; never let an exception destroy a different owner.
      const canDestroy =
        legacyDispatchId === undefined
          ? false
          : await this.ctx.blockConcurrencyWhile(async () => {
              const current =
                await this.ctx.storage.get<ContainerExecutionDisposition>(EXECUTION_KEY);
              if (
                current?.executionId !== legacyDispatchId ||
                current.disposition !== "preparing" ||
                this.stoppingExecutionId !== undefined
              )
                return false;
              await this.ctx.storage.put<ContainerExecutionDisposition>(EXECUTION_KEY, {
                ...current,
                disposition: "stopping",
              });
              this.stoppingExecutionId = legacyDispatchId;
              return true;
            });
      if (canDestroy)
        await withHardTimeout(
          this.destroy().finally(() => {
            if (this.stoppingExecutionId === legacyDispatchId) this.stoppingExecutionId = undefined;
          }),
          CONTAINER_DESTROY_HARD_TIMEOUT_MS,
        ).catch((destroyError: unknown) => {
          console.error(
            `[predict-container-do] failed-delivery destroy failed ${requestSummary}: ${String(
              destroyError,
            )}`,
          );
        });
      return Response.json(
        { error: "Container start failed", detail: String(err) },
        { status: 502 },
      );
    } finally {
      const dispatchedId = executionId ?? legacyDispatchId;
      if (this.dispatchingExecutionId === dispatchedId) this.dispatchingExecutionId = undefined;
    }
  }
}

// Distinct Durable Object class gives Wrangler a separate Container
// application/resource profile while preserving the proven request proxy.
export class FinishPositionRescoreContainer extends FinishPositionPredictContainer {
  override sleepAfter = RESCORE_SLEEP_AFTER;

  protected override buildContainerEnvVars(): Record<string, string> {
    return buildRescorePredictContainerEnvVars({
      env: this.env,
      inheritedEnvVars: this.envVars ?? EMPTY_ENV_VARS,
    });
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (
      url.pathname === PREWARM_DAY_BASE_PATH ||
      (url.pathname === PREDICT_PATH && url.searchParams.get("mode") !== RESCORE_MODE)
    )
      return Response.json({ error: RESCORE_ONLY_ERROR, ok: false }, { status: HTTP_BAD_REQUEST });
    return super.fetch(request);
  }
}

export class FinishPositionRaceChainContainer extends FinishPositionPredictContainer {
  override sleepAfter = RACE_CHAIN_SLEEP_AFTER;

  protected override buildContainerEnvVars(): Record<string, string> {
    return buildRaceChainPredictContainerEnvVars({
      env: this.env,
      inheritedEnvVars: this.envVars ?? EMPTY_ENV_VARS,
    });
  }
}
