// Run with bun. Computes the expected race count for a given JST date by
// querying Hyperdrive (`jvd_ra` + `nvd_ra`) once and caching the result in
// `ODDS_HOT_KV` for a short TTL. Used by `runScheduledPlan` so the polling
// gate compares actual `odds_fetch_state` rows against the expected total
// instead of the legacy `stateCount === 0` check, which silently froze NAR
// venues whose `keiba.go` RaceList HTML was published after the 05:55 JST
// initial populate (Ban'ei / Mizusawa morning publishing lag).
//
// Zero-cache guard (task F2 B): when Hyperdrive transiently returns 0 for a
// known race day (JST 09:00-22:00 polling window), we previously baked that
// `total=0` into KV for `KV_TTL_SECONDS` minutes, which froze
// `stateCount < expectedCount` at `0 < 0 == false` and kept populate from
// re-running. Now we skip the KV write when the date is inside the race-day
// window unless the operator has set `REPLICA_SYNC_HOT_TRUST_ZERO_COUNT=1`.

import type { Pool } from "pg";

import { getHotPool } from "./postgres-pool";
import { getJstDateParts } from "./time";
import type { Env } from "./types";

const KV_KEY_PREFIX = "expected-race-count:";
const KV_TTL_SECONDS = 300;
const RACE_DAY_HOUR_START = 9;
const RACE_DAY_HOUR_END = 22;
const TRUST_ZERO_ENV_FLAG = "1";
const SELECT_EXPECTED_COUNT_SQL = `
  select
    (select count(*) from jvd_ra where kaisai_nen = $1 and kaisai_tsukihi = $2) as jra,
    (select count(*) from nvd_ra where kaisai_nen = $1 and kaisai_tsukihi = $2) as nar
`;

interface ExpectedCountQueryRow {
  jra: number | string | null;
  nar: number | string | null;
}

interface ExpectedRaceCountContext {
  pool?: Pool;
  now?: Date;
}

const buildKvKey = (ymd: string): string => `${KV_KEY_PREFIX}${ymd}`;

const toCount = (value: number | string | null): number => {
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? 0 : parsed;
  }
  return 0;
};

const parseCachedValue = (raw: string | null): number | null => {
  if (raw === null) {
    return null;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isNaN(parsed) ? null : parsed;
};

const queryHyperdriveExpectedCount = async (pool: Pool, ymd: string): Promise<number> => {
  const result = await pool.query<ExpectedCountQueryRow>(SELECT_EXPECTED_COUNT_SQL, [
    ymd.slice(0, 4),
    ymd.slice(4, 8),
  ]);
  const row = result.rows[0];
  if (!row) {
    return 0;
  }
  return toCount(row.jra) + toCount(row.nar);
};

// Inside the JST race-day window (default 09:00-22:00) a Hyperdrive `total=0`
// is far more likely to be a transient query failure / replica lag than a
// genuine "zero races today". Withholding the KV write lets the next cron tick
// re-query rather than freezing the planner on a bogus zero for 5 minutes.
const isInsideRaceDayWindow = (now: Date): boolean => {
  const hourString = getJstDateParts(now).hour;
  if (!hourString) {
    return false;
  }
  const hour = Number.parseInt(hourString, 10);
  return hour >= RACE_DAY_HOUR_START && hour < RACE_DAY_HOUR_END;
};

const shouldSkipZeroCache = (total: number, env: Env, now: Date): boolean => {
  if (total > 0) {
    return false;
  }
  if (env.REPLICA_SYNC_HOT_TRUST_ZERO_COUNT === TRUST_ZERO_ENV_FLAG) {
    return false;
  }
  return isInsideRaceDayWindow(now);
};

const resolveNow = (now: Date | undefined): Date => now ?? new Date();

export const getExpectedRaceCountForDate = async (
  env: Env,
  ymd: string,
  context: ExpectedRaceCountContext = {},
): Promise<number> => {
  const kvKey = buildKvKey(ymd);
  const cached = parseCachedValue(await env.ODDS_HOT_KV.get(kvKey));
  if (cached !== null) {
    return cached;
  }
  const pool = context.pool ?? getHotPool(env);
  const total = await queryHyperdriveExpectedCount(pool, ymd);
  if (shouldSkipZeroCache(total, env, resolveNow(context.now))) {
    return total;
  }
  await env.ODDS_HOT_KV.put(kvKey, total.toString(), { expirationTtl: KV_TTL_SECONDS });
  return total;
};
