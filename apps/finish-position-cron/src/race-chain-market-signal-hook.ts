// Run with bun. Best-effort Queue-consumer hook that creates the attested
// Worker market-signal foundation before a focused full Container dispatch.
// Any fetch, validation, or R2 failure deliberately returns no attestation so
// the Container executes its legacy market layer.

import {
  marketSignalFoundationEnabled,
  materializeMarketSignalFoundation,
  type MarketSignalFoundationEnv,
} from "./race-chain-market-signal-foundation";
import {
  fetchOddsForRace,
  sourceForCategory,
  type FetchRaceInput,
  type RealtimeOdds,
} from "./scoring/rescore-realtime";
import type { PredictCategory } from "./types";

export interface MarketSignalDispatchAttestation {
  baseGenerationId: string;
  etag: string;
  key: string;
  oddsSnapshotHash: string;
  version: string;
}

export interface PrepareMarketSignalFoundationInput {
  category: PredictCategory;
  env: MarketSignalFoundationEnv;
  fetchImpl: typeof fetch;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
}

interface ReadyMarketSignalHookResult {
  attestation: MarketSignalDispatchAttestation;
  cacheHit: boolean;
  status: "ready";
}

interface UnavailableMarketSignalHookResult {
  reason: string;
  status: "unavailable";
}

export type MarketSignalHookResult =
  | ReadyMarketSignalHookResult
  | UnavailableMarketSignalHookResult;

export const addMarketSignalAttestationToUrl = (
  url: string,
  attestation: MarketSignalDispatchAttestation,
): string => {
  const value = new URL(url);
  value.searchParams.set("marketSignalFoundationKey", attestation.key);
  value.searchParams.set("marketSignalFoundationEtag", attestation.etag);
  value.searchParams.set("marketSignalFoundationVersion", attestation.version);
  value.searchParams.set("marketSignalOddsSnapshotHash", attestation.oddsSnapshotHash);
  value.searchParams.set("marketSignalBaseGenerationId", attestation.baseGenerationId);
  return value.toString();
};

interface MarketSignalHookDependencies {
  fetchOdds: (input: FetchRaceInput) => Promise<Map<number, RealtimeOdds>>;
  materialize: typeof materializeMarketSignalFoundation;
}

const defaultDependencies: MarketSignalHookDependencies = {
  fetchOdds: fetchOddsForRace,
  materialize: materializeMarketSignalFoundation,
};

const logOutcome = (
  input: PrepareMarketSignalFoundationInput,
  fields: Record<string, string | number | boolean>,
): void => {
  console.log(
    JSON.stringify({
      category: input.category,
      event: "worker-market-signal-foundation",
      keibajoCode: input.keibajoCode,
      raceBango: input.raceBango,
      runYmd: input.runYmd,
      ...fields,
    }),
  );
};

export const prepareMarketSignalFoundationBestEffort = async (
  input: PrepareMarketSignalFoundationInput,
  dependencies: MarketSignalHookDependencies = defaultDependencies,
): Promise<MarketSignalHookResult> => {
  if (!marketSignalFoundationEnabled(input.env.WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED)) {
    return { reason: "disabled", status: "unavailable" };
  }
  if (input.category !== "jra") {
    return { reason: "unsupported-category", status: "unavailable" };
  }
  const startedAt: number = performance.now();
  try {
    const liveOddsByHorseNumber = await dependencies.fetchOdds({
      fetchImpl: input.fetchImpl,
      keibajoCode: input.keibajoCode,
      raceBango: input.raceBango,
      runYmd: input.runYmd,
      source: sourceForCategory(input.category),
    });
    const result = await dependencies.materialize({
      category: input.category,
      env: input.env,
      keibajoCode: input.keibajoCode,
      liveOddsByHorseNumber,
      raceBango: input.raceBango,
      runYmd: input.runYmd,
    });
    if (result.status !== "materialized") {
      logOutcome(input, {
        durationMs: performance.now() - startedAt,
        reason: result.reason,
        status: result.status,
      });
      return { reason: result.reason, status: "unavailable" };
    }
    logOutcome(input, {
      cacheHit: result.cacheHit,
      computeMs: result.computeMs,
      durationMs: performance.now() - startedAt,
      rowCount: result.rowCount,
      status: "ready",
      totalMs: result.totalMs,
    });
    return {
      attestation: {
        baseGenerationId: result.baseGenerationId,
        etag: result.artifactEtag,
        key: result.key,
        oddsSnapshotHash: result.oddsSnapshotHash,
        version: result.artifactVersion,
      },
      cacheHit: result.cacheHit,
      status: "ready",
    };
  } catch (error) {
    console.warn(
      JSON.stringify({
        category: input.category,
        error: String(error),
        event: "worker-market-signal-foundation",
        keibajoCode: input.keibajoCode,
        raceBango: input.raceBango,
        runYmd: input.runYmd,
        durationMs: performance.now() - startedAt,
        status: "fallback",
      }),
    );
    return { reason: "hook-error", status: "unavailable" };
  }
};
