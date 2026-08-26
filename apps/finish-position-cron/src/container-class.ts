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

import { Container } from "@cloudflare/containers";
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
import { PREDICT_DO_INTERNAL_PURGE_PATH, purgePredictDoStorage } from "./predict-do-state-purge";
import type { Env } from "./types";

type PredictContainerRole = "legacy" | "race-chain";

type PredictContainerEnvironment = Pick<
  Env,
  | "DAY_BASE_SPLIT_ENABLED"
  | "NAR_TRANSFORMER_BLEND_ENABLED"
  | "NEON_DATABASE_URL"
  | "PIPELINE_TOTAL_TIMEOUT_SECONDS"
  | "PREDICT_DAYS_AHEAD"
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
>;

export interface BuildPredictContainerEnvVarsOptions {
  env: PredictContainerEnvironment;
  inheritedEnvVars: Readonly<Record<string, string>>;
}

const DEFAULT_PORT = 8080;
const CONTAINER_INSTANCE_GET_TIMEOUT_MS = 20_000;
const CONTAINER_PORT_READY_TIMEOUT_MS = 60_000;
const CONTAINER_DELIVERY_HARD_TIMEOUT_MS = 65_000;
const CONTAINER_DESTROY_HARD_TIMEOUT_MS = 15_000;
const CONTAINER_STATUS_HARD_TIMEOUT_MS = 5_000;
// 20m covers a detached first-day day-base build (10–15m) plus race-chain.
const SLEEP_AFTER = "45m";
const MODELS_DIR_DEFAULT = "/models";
const PIPELINE_TOTAL_TIMEOUT_SECONDS_DEFAULT = "1800";
const EMPTY_ENV_VALUE = "";
const EMPTY_ENV_VARS: Readonly<Record<string, string>> = Object.freeze({});
const ADMIN_STOP_PATH = "/__admin/stop-container";
const PREWARM_DAY_BASE_PATH = "/prewarm-day-base";
const PREWARM_DAY_BASE_STATUS_PATH = "/prewarm-day-base-status";
const AUTH_HEADER = "authorization";
const BEARER_PREFIX = "Bearer ";
const LEGACY_CONTAINER_ROLE: PredictContainerRole = "legacy";
const RACE_CHAIN_CONTAINER_ROLE: PredictContainerRole = "race-chain";

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
  override defaultPort = DEFAULT_PORT;
  override sleepAfter = SLEEP_AFTER;
  override enableInternet = true;

  protected buildContainerEnvVars(): Record<string, string> {
    return buildLegacyPredictContainerEnvVars({
      env: this.env,
      inheritedEnvVars: this.envVars ?? EMPTY_ENV_VARS,
    });
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
      await this.destroy();
      console.log(`[predict-container-do] admin-stop destroyed ${requestSummary}`);
      return Response.json({ ok: true });
    }
    if (url.pathname === PREDICT_DO_INTERNAL_PURGE_PATH) {
      if (!isAuthorizedAdmin(request, this.env.TRIGGER_TOKEN)) {
        console.warn(`[predict-container-do] admin-purge unauthorized ${requestSummary}`);
        return Response.json({ error: "unauthorized", ok: false }, { status: 401 });
      }
      console.log(`[predict-container-do] admin-purge requested ${requestSummary}`);
      await purgePredictDoStorage({
        deleteApplicationState: async () => undefined,
        destroy: () => this.destroy(),
      });
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
    const watchEnabled =
      this.env.FOCUSED_FULL_WATCH_ENABLED === "1" &&
      this.env.FOCUSED_FULL_COMPLETION_QUEUE !== undefined &&
      isFocusedFullPredictUrl(url);
    const watchPayload = parseWatchPayload(request, watchEnabled);
    if (watchPayload instanceof Response) return watchPayload;
    this.envVars = this.buildContainerEnvVars();
    const startedAt = Date.now();
    const debug = isDebugRequest(url);
    try {
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
      await withHardTimeout(
        this.startAndWaitForPorts([DEFAULT_PORT], {
          abort: AbortSignal.timeout(CONTAINER_PORT_READY_TIMEOUT_MS),
          instanceGetTimeoutMS: CONTAINER_INSTANCE_GET_TIMEOUT_MS,
          portReadyTimeoutMS: CONTAINER_PORT_READY_TIMEOUT_MS,
        }),
        CONTAINER_DELIVERY_HARD_TIMEOUT_MS,
      );
      console.log(
        `[predict-container-do] delivery port-ready ${requestSummary} durationMs=${
          Date.now() - startedAt
        }`,
      );
      const response = await withHardTimeout(
        this.containerFetch(request),
        CONTAINER_DELIVERY_HARD_TIMEOUT_MS,
      );
      console.log(
        `[predict-container-do] delivery headers ${requestSummary} status=${response.status} durationMs=${
          Date.now() - startedAt
        }`,
      );
      const accepted =
        watchPayload === undefined ? false : await hasAcceptedResult(response.clone());
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
              this.env,
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
      await withHardTimeout(this.destroy(), CONTAINER_DESTROY_HARD_TIMEOUT_MS).catch(
        (destroyError: unknown) => {
          console.error(
            `[predict-container-do] failed-delivery destroy failed ${requestSummary}: ${String(
              destroyError,
            )}`,
          );
        },
      );
      return Response.json(
        { error: "Container start failed", detail: String(err) },
        { status: 502 },
      );
    }
  }
}

// Distinct Durable Object class gives Wrangler a separate Container
// application/resource profile while preserving the proven request proxy.
export class FinishPositionRaceChainContainer extends FinishPositionPredictContainer {
  protected override buildContainerEnvVars(): Record<string, string> {
    return buildRaceChainPredictContainerEnvVars({
      env: this.env,
      inheritedEnvVars: this.envVars ?? EMPTY_ENV_VARS,
    });
  }
}
