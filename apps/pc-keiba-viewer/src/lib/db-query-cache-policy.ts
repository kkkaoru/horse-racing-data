// Run with bun. Bound race-list staleness across both KV and edge Cache API tiers.
interface DbQueryCachePolicyInput {
  keyParts: readonly unknown[];
  nowMs: number;
  ttlSeconds: number;
}

interface DbQueryCachePolicy {
  keyParts: readonly unknown[];
  ttlSeconds: number;
}

const RACE_LIST_QUERY_NAMES: ReadonlySet<unknown> = new Set([
  "getRacesByDate",
  "getRacesByDateWithoutJockeyNames",
]);
const RACE_LIST_TTL_SECONDS = 60;
const MILLISECONDS_PER_SECOND = 1000;

export const getDbQueryCachePolicy = (input: DbQueryCachePolicyInput): DbQueryCachePolicy =>
  RACE_LIST_QUERY_NAMES.has(input.keyParts[0])
    ? {
        keyParts: [
          ...input.keyParts,
          "race-list-minute-v1",
          Math.floor(input.nowMs / (RACE_LIST_TTL_SECONDS * MILLISECONDS_PER_SECOND)),
        ],
        ttlSeconds: Math.min(input.ttlSeconds, RACE_LIST_TTL_SECONDS),
      }
    : { keyParts: input.keyParts, ttlSeconds: input.ttlSeconds };
