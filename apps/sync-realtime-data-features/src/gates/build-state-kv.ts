// Run with bun. Replaces probeDailyRaceEntriesFreshness from old worker.
// Tracks per-race build completion time in KV instead of selecting daily_race_entries.

import { raceDayEndJstMs } from "../time";
import type { Env } from "../types";

const BUILD_STATE_KV_KEY_PREFIX = "features:build-state";
const DEFAULT_BUILD_STATE_TTL_SECONDS = 86_400;

export interface BuildStateRecord {
  lastBuiltAt: string;
  rowCount: number;
  // Count of rows carrying an actual finish position (see
  // DailyRaceEntryRow.finish_position), i.e. rows built AFTER race results
  // landed. Optional so KV entries written before this field existed can be
  // told apart from genuinely-ranked ones by isPast14BuildStateFresh.
  rankedRowCount?: number;
}

const buildBuildStateKey = (raceKey: string): string => `${BUILD_STATE_KV_KEY_PREFIX}:${raceKey}`;

export const getBuildStateFromKv = async (
  env: Env,
  raceKey: string,
): Promise<BuildStateRecord | null> => {
  const json = await env.FEATURES_KV.get(buildBuildStateKey(raceKey));
  return json ? (JSON.parse(json) as BuildStateRecord) : null;
};

export const putBuildStateToKv = async (
  env: Env,
  raceKey: string,
  record: BuildStateRecord,
): Promise<void> => {
  await env.FEATURES_KV.put(buildBuildStateKey(raceKey), JSON.stringify(record), {
    expirationTtl: DEFAULT_BUILD_STATE_TTL_SECONDS,
  });
};

export interface ShouldSkipBuildInput {
  freshnessThresholdMs: number;
  now: Date;
  state: BuildStateRecord | null;
}

export const shouldSkipBuild = ({
  freshnessThresholdMs,
  now,
  state,
}: ShouldSkipBuildInput): boolean => {
  if (!state) {
    return false;
  }
  const lastMs = Date.parse(state.lastBuiltAt);
  if (!Number.isFinite(lastMs)) {
    return false;
  }
  return now.getTime() - lastMs < freshnessThresholdMs;
};

// Generic freshness check used by adaptive multi-scope scheduler: returns true
// only when the recorded build is non-empty AND younger than freshnessMs.
// rowCount === 0 is intentionally treated as stale so the next tick re-builds.
export const isBuildStateFresh = (
  state: BuildStateRecord | null,
  freshnessMs: number,
  now: Date,
): boolean => {
  if (!state) {
    return false;
  }
  if (state.rowCount <= 0) {
    return false;
  }
  const lastMs = Date.parse(state.lastBuiltAt);
  if (!Number.isFinite(lastMs)) {
    return false;
  }
  return now.getTime() - lastMs < freshnessMs;
};

export interface Past14BuildStateFreshInput {
  state: BuildStateRecord | null;
  raceDateJst: string;
  now: Date;
  freshnessMs: number;
}

// Stricter freshness for past14 rebuild candidates: a build recorded DURING
// race day (before results exist) must never count as fresh, no matter its
// rowCount, because the Parquet it produced has no finish positions.
// Fresh requires ALL of:
//   (a) lastBuiltAt is after the race day's JST 23:59:59.999 end, AND
//   (b) the recorded build actually produced ranked (result) rows, AND
//   (c) lastBuiltAt is within the outer freshnessMs cap, so a ranked build
//       still gets refreshed periodically (e.g. late corrections).
// A record missing rankedRowCount predates this fix and is treated as not
// satisfying (b), forcing exactly one rebuild.
export const isPast14BuildStateFresh = ({
  freshnessMs,
  now,
  raceDateJst,
  state,
}: Past14BuildStateFreshInput): boolean => {
  if (!state) {
    return false;
  }
  if (state.rowCount <= 0) {
    return false;
  }
  if (!state.rankedRowCount) {
    return false;
  }
  const lastMs = Date.parse(state.lastBuiltAt);
  if (!Number.isFinite(lastMs)) {
    return false;
  }
  if (lastMs <= raceDayEndJstMs(raceDateJst)) {
    return false;
  }
  return now.getTime() - lastMs < freshnessMs;
};
