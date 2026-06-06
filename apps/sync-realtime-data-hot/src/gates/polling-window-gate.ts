// Run with bun. Race-window-aware polling gate. Suspends the `* * * * *`
// planner cron whenever no race in `odds_fetch_state` falls inside the active
// window (`now - OFFSET_BEFORE_MINUTES` ... `now + OFFSET_AFTER_HOURS`), so
// the backend-replica R2 / Hyperdrive path can idle into suspend and shave
// compute cost during off-hours.
//
// Window rationale:
//   - `OFFSET_BEFORE_MINUTES = 30` covers the 30 min after a race start where
//     viewer / finishing-position consumers still poll closing odds.
//   - `OFFSET_AFTER_HOURS = 3` covers the 180 min weight-fetch lead time
//     (`WEIGHT_FETCH_LEAD_MINUTES`) plus the 120 min paddock window with
//     headroom; odds lead is typically T-60 min so 3 h is safe.
//
// KV cache trade-off (per `feedback_cache_design_tradeoff`):
//   - Window evaluation is bound by the cached result for up to 60 s, so a
//     newly inserted late-night race takes at most 60 s to flip the gate on.
//   - In exchange we drop the planner's D1 read from 1/min to 1/min/colo per
//     gate miss, keeping the suspend-eligible idle window contiguous.
import type { Env } from "../types";

const KV_CACHE_KEY = "odds-polling-window:active";
const KV_CACHE_TTL_SECONDS = 60;
const KV_VALUE_TRUE = "true";
const KV_VALUE_FALSE = "false";
const OFFSET_BEFORE_MINUTES = 30;
const OFFSET_AFTER_HOURS = 3;
const MS_PER_MINUTE = 60_000;
const MS_PER_HOUR = 3_600_000;
const ACTIVE_RACE_WINDOW_SQL = `
  SELECT 1
  FROM odds_fetch_state
  WHERE race_start_at_jst >= ?
    AND race_start_at_jst <= ?
  LIMIT 1
`;

interface ActiveWindowBounds {
  fromIso: string;
  toIso: string;
}

const buildBounds = (now: Date): ActiveWindowBounds => ({
  fromIso: new Date(now.getTime() - OFFSET_BEFORE_MINUTES * MS_PER_MINUTE).toISOString(),
  toIso: new Date(now.getTime() + OFFSET_AFTER_HOURS * MS_PER_HOUR).toISOString(),
});

const parseCachedFlag = (raw: string | null): boolean | null => {
  if (raw === KV_VALUE_TRUE) {
    return true;
  }
  if (raw === KV_VALUE_FALSE) {
    return false;
  }
  return null;
};

const queryActiveRaceWindow = async (env: Env, bounds: ActiveWindowBounds): Promise<boolean> => {
  const row = await env.REALTIME_HOT_DB.prepare(ACTIVE_RACE_WINDOW_SQL)
    .bind(bounds.fromIso, bounds.toIso)
    .first<{ "1": number }>();
  return row !== null;
};

const writeCachedFlag = async (env: Env, active: boolean): Promise<void> => {
  await env.ODDS_HOT_KV.put(KV_CACHE_KEY, active ? KV_VALUE_TRUE : KV_VALUE_FALSE, {
    expirationTtl: KV_CACHE_TTL_SECONDS,
  });
};

export const shouldRunOddsCron = async (env: Env, now: Date): Promise<boolean> => {
  const cached = parseCachedFlag(await env.ODDS_HOT_KV.get(KV_CACHE_KEY));
  if (cached !== null) {
    return cached;
  }
  const active = await queryActiveRaceWindow(env, buildBounds(now));
  await writeCachedFlag(env, active);
  return active;
};
