// Run with bun. Replaces probeDailyRaceEntriesFreshness from old worker.
// Tracks per-race build completion time in KV instead of selecting daily_race_entries.

import type { Env } from "../types";

const BUILD_STATE_KV_KEY_PREFIX = "features:build-state";
const DEFAULT_BUILD_STATE_TTL_SECONDS = 86_400;

export interface BuildStateRecord {
  lastBuiltAt: string;
  rowCount: number;
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
