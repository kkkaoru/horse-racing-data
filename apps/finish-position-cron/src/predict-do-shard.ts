// Run with bun. Single source of truth for FinishPositionPredictContainer DO
// name resolution. Every call site that constructs a Container DO name for a
// (possibly race-scoped) predict request -- queue-consumer.ts (both the
// focused-full/full path and the per-race rescore path),
// focused-full-cache-pickup.ts, and worker.ts's admin run-focused-full-race
// -- must go through resolvePredictDoName so a rollback only ever requires
// unsetting one env var (RACE_SHARDED_DO), and so a cache pickup or admin
// re-run always targets the exact DO the original race-scoped request used.
//
// See docs/finish-position-prediction-system.md §5.4 for the design
// rationale (USER decision 11) and
// docs/probes/serving-latency-architecture-2026-07-17.md §2 for the
// underlying argument: `_claim_focused_full_slot()` /
// `_PIPELINE_EXEC_LOCK` in the Python container serialize every pipeline
// execution per container process because the DuckDB base build + layer
// chain writes to category-scoped (not race-scoped) work directories.
// Spreading a category's races across several DOs gives each one a
// physically separate container filesystem, so the collision this lock
// guards against cannot occur across shards -- true parallelism is safe by
// construction, while the existing single-slot guard stays untouched and
// still necessary within one shard.

import type { Env } from "./types";

export const PREDICT_DO_NAME_PREFIX = "predict-";
const RACE_SHARDED_DO_FLAG_VALUE = "1";
// wrangler.jsonc defines exactly one `containers` entry for
// FinishPositionPredictContainer with max_instances: 12, shared across every
// category that uses this binding -- not per-category. jra/nar/ban-ei can
// all race the same weekend, so the software cap in
// container-slot-cap.ts (general 10 + Ban-ei reserved 2) is what keeps
// starts at or under 12; shardCount 3 remains the default so focused-full
// and per-race rescore can fan out without exceeding the bounded pool.
const DEFAULT_RACE_SHARD_MAX_CONCURRENT = 3;
const SHARD_KEY_SEPARATOR = ":";

// FNV-1a-style string hash over UTF-16 code units masked to a byte: pure,
// deterministic, no external dependency. Only used to spread a bounded
// number of races across shard buckets -- not a cryptographic or
// collision-resistant hash, and does not need to be one.
const FNV_OFFSET_BASIS = 0x811c9dc5;
const FNV_PRIME = 0x01000193;
const BYTE_MASK = 0xff;

interface ResolvePredictDoNameParams {
  category: string;
  env: Env;
  keibajoCode?: string;
  raceBango?: string;
}

const hashString = (value: string): number =>
  Array.from(value).reduce(
    (hash, char) => Math.imul(hash ^ (char.charCodeAt(0) & BYTE_MASK), FNV_PRIME) >>> 0,
    FNV_OFFSET_BASIS,
  );

const isRaceShardingEnabled = (env: Env): boolean =>
  env.RACE_SHARDED_DO === RACE_SHARDED_DO_FLAG_VALUE;

const resolveShardMaxConcurrent = (env: Env): number => {
  const parsed = Number(env.RACE_SHARD_MAX_CONCURRENT);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : DEFAULT_RACE_SHARD_MAX_CONCURRENT;
};

// keibajoCode and raceBango are hashed TOGETHER, never raceBango alone --
// raceBango repeats across every venue racing the same day (every card has
// a "race 1"), so hashing raceBango alone would collide every venue's race 1
// onto the same shard and defeat the point of spreading load. runYmd is
// deliberately EXCLUDED from the hash input -- this keeps a given
// (keibajoCode, raceBango) pair's shard assignment stable across days, a
// debugging/reasoning convenience, not a correctness requirement.
const resolveShardIndex = (keibajoCode: string, raceBango: string, maxConcurrent: number): number =>
  hashString(`${keibajoCode}${SHARD_KEY_SEPARATOR}${raceBango}`) % maxConcurrent;

// The single source of truth for every FinishPositionPredictContainer DO
// name in this Worker.
//
// Flag off (env.RACE_SHARDED_DO unset/not "1"), OR no race scope on this
// request (keibajoCode/raceBango absent -- the legacy whole-category full
// path, which processes every race of a category in one sequential
// /predict call and therefore cannot benefit from sharding regardless):
// returns byte-identical output to the pre-sharding `predict-{category}`
// scheme -- the instant-rollback path.
//
// Flag on AND race-scoped: returns `predict-{category}-{shardIndex}`, where
// shardIndex = FNV-1a(keibajoCode:raceBango) % RACE_SHARD_MAX_CONCURRENT
// (default 3, see resolveShardMaxConcurrent).
export const listDayBasePickupDoNames = (params: {
  category: string;
  env: Env;
}): readonly string[] => [`${PREDICT_DO_NAME_PREFIX}${params.category}`];

export const resolvePredictDoName = (params: ResolvePredictDoNameParams): string => {
  const { category, env, keibajoCode, raceBango } = params;
  const unshardedName = `${PREDICT_DO_NAME_PREFIX}${category}`;
  if (!isRaceShardingEnabled(env) || keibajoCode === undefined || raceBango === undefined) {
    return unshardedName;
  }
  const shardIndex = resolveShardIndex(keibajoCode, raceBango, resolveShardMaxConcurrent(env));
  return `${unshardedName}-${shardIndex}`;
};
