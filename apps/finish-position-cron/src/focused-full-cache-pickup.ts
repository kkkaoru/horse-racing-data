// Run with bun. Retry-safe pickup of a completed focused-full race's R2
// feat-cache payload from the container's bounded in-process cache store.
//
// Why this exists: the container's R2 credentials are read-only (direct
// SigV4 PUT was removed in commit 41adee27 after it failed in production
// under that token). The only working R2 write path is the container
// embedding parquetBase64/parquetKey in the last line of a LIVE NDJSON HTTP
// response, which container-ndjson-proxy.ts reads off the stream and proxies
// to R2. A focused-full request detaches its pipeline into a background
// thread and returns "accepted" immediately (predict_lib.serve's focused-full
// branch) -- by the time that detached pipeline finishes, the HTTP response
// that would have carried the payload bytes has already ended, so the R2
// feat-cache was never populated for focused-full requests at all.
//
// The container now holds a completed run's payload in a small in-process
// store (predict_lib.focused_full_cache.FocusedFullCacheStore) and exposes it
// via GET /focused-full-cache. This module fetches that endpoint exactly
// after queue-consumer.ts's redelivery poll confirms Neon
// completion, and proxies any payload found through the SAME R2-write path
// the live NDJSON stream uses (container-ndjson-proxy.ts's
// proxyResultParquetsToR2) -- so a Stage-2 rescore for this race can hit the
// cache instead of always falling back to a full pipeline.

import { withFocusedFullCommit } from "./focused-full-commit";
import { proxyResultParquetsToR2 } from "./container-ndjson-proxy";
import type { PredictResultLine } from "./ndjson-stream";
import { resolvePredictDoName } from "./predict-do-shard";
import {
  qualifyPredictionContainerDoName,
  resolveContainerNamespaceForRole,
  type PredictionContainerRole,
} from "./race-container-routing";
import type { Env, PredictCategory } from "./types";

const FOCUSED_FULL_CACHE_PATH = "/focused-full-cache";
const PREDICT_HOST = "http://do";
const RESULT_LINE_TYPE = "result";
// The R2-proxy path only reads parquetBase64 / parquetKey / perRaceParquets
// off this synthetic result line -- racesPredicted is irrelevant to it but
// required by PredictResultLine's shape.
const PLACEHOLDER_RACES_PREDICTED = 0;

interface FocusedFullCachePickupParams {
  executionId?: string;
  env: Env;
  category: PredictCategory;
  runYmd: string;
  keibajoCode: string;
  raceBango: string;
  debug?: boolean;
  containerRole?: PredictionContainerRole;
}

interface FocusedFullCacheResponseBody {
  found: boolean;
  parquetBase64?: string | null;
  parquetKey?: string | null;
  perRaceParquets?: { parquetBase64: string; parquetKey: string }[] | null;
}

const buildFocusedFullCacheUrl = (params: FocusedFullCachePickupParams): string => {
  const searchParams = new URLSearchParams({
    category: params.category,
    runDate: params.runYmd,
    keibajoCode: params.keibajoCode,
    raceBango: params.raceBango,
  });
  return `${PREDICT_HOST}${FOCUSED_FULL_CACHE_PATH}?${searchParams.toString()}`;
};

const toResultLine = (
  body: FocusedFullCacheResponseBody,
  category: PredictCategory,
): PredictResultLine => ({
  type: RESULT_LINE_TYPE,
  category,
  racesPredicted: PLACEHOLDER_RACES_PREDICTED,
  parquetBase64: body.parquetBase64 ?? undefined,
  parquetKey: body.parquetKey ?? undefined,
  perRaceParquets: body.perRaceParquets ?? undefined,
});

// Reads are best effort. An uncertain external commit propagates and retains
// its durable admission barrier; it must never be reported as completed pickup.
export const pickUpFocusedFullCache = async (
  params: FocusedFullCachePickupParams,
): Promise<void> => {
  const { env, category, runYmd, keibajoCode, raceBango, debug } = params;
  const doName = qualifyPredictionContainerDoName(
    resolvePredictDoName({ category, env, keibajoCode, raceBango }),
    params.containerRole ?? "legacy",
  );
  const url = buildFocusedFullCacheUrl(params);
  try {
    const namespace = resolveContainerNamespaceForRole(env, params.containerRole);
    const doId = namespace.idFromName(doName);
    const stub = namespace.get(doId);
    const response = await stub.fetch(new Request(url));
    if (!response.ok) {
      console.warn(
        `[focused-full-cache-pickup] non-ok status=${response.status} doName=${doName} url=${url}`,
      );
      return;
    }
    const body = (await response.json()) as FocusedFullCacheResponseBody;
    if (!body.found) return;
    await withFocusedFullCommit({
      enforce: true,
      env,
      executionId: params.executionId,
      workKey: `focused-full:${runYmd}:${category}:${keibajoCode}:${raceBango}`,
      commit: () => proxyResultParquetsToR2(toResultLine(body, category), env, debug === true),
    });
  } catch (err) {
    if (err instanceof Error && err.message === "FOCUSED_FULL_COMMIT_UNRESOLVED") throw err;
    console.warn(
      `[focused-full-cache-pickup] failed category=${category} runYmd=${runYmd} keibajo=${keibajoCode} race=${raceBango}: ${String(err)}`,
    );
  }
};
