// Run with bun. Assembles per-race Worker caches only while the canonical
// preaggregate source and its exact Catalog snapshot are simultaneously fresh.
// Category-level running-style drift is permitted here because every consumer
// separately proves the target race's exact running-style feature rows.

import { materializeDayBasePerRaceCache } from "./day-base-race-materializer";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";
import { fetchCatalogRaceSourceSnapshots } from "./race-source-snapshot";
import type { Env, PredictCategory } from "./types";

interface AssembleAttestedRaceCachesParams {
  category: PredictCategory;
  env: Env;
  force?: boolean;
  runYmd: string;
}

interface AssembledRaceCaches {
  featureHash: string;
  manifestKey: string;
  raceCount: number;
  rowCount: number;
  status: "materialized";
}

interface RaceCacheAssemblyFallback {
  reason: string;
  status: "fallback";
}

export type AttestedRaceCacheAssemblyResult = AssembledRaceCaches | RaceCacheAssemblyFallback;

const errorReason = (error: unknown): string =>
  error instanceof Error && error.message !== "" ? error.message : "race-cache-assembly-failed";

const RUNNING_STYLE_COUNT_MISMATCH_PATTERN =
  /^(?:rs-row-count|running-style-race-count)-\d+-of-\d+$/;

const hasStablePreaggregateSource = (readiness: { ready: boolean; reason: string }): boolean =>
  readiness.ready ||
  readiness.reason === "rs-predicted-at-max-mismatch" ||
  RUNNING_STYLE_COUNT_MISMATCH_PATTERN.test(readiness.reason);

export const assembleAttestedRaceCaches = async (
  params: AssembleAttestedRaceCachesParams,
): Promise<AttestedRaceCacheAssemblyResult> => {
  try {
    if (params.env.PC_KEIBA_R2_CATALOG === undefined) {
      return { reason: "catalog-binding-unavailable", status: "fallback" };
    }
    const before = await getFocusedFullDayBaseReadiness(params);
    if (!hasStablePreaggregateSource(before)) {
      return { reason: `preaggregate-not-ready:${before.reason}`, status: "fallback" };
    }
    const sourceSnapshots = await fetchCatalogRaceSourceSnapshots({
      catalog: params.env.PC_KEIBA_R2_CATALOG,
      category: params.category,
      runYmd: params.runYmd,
    });
    const after = await getFocusedFullDayBaseReadiness(params);
    if (!hasStablePreaggregateSource(after)) {
      return { reason: `source-changed-during-assembly:${after.reason}`, status: "fallback" };
    }
    return materializeDayBasePerRaceCache({
      category: params.category,
      env: params.env,
      ...(params.force === undefined ? {} : { force: params.force }),
      runYmd: params.runYmd,
      sourceSnapshots,
    });
  } catch (error) {
    return { reason: errorReason(error), status: "fallback" };
  }
};
