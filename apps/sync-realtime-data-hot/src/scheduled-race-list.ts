// Run with bun. Hyperdrive-direct read of today's races for the hot worker's
// self-discovery path. Lists `jvd_ra` (JRA) and `nvd_ra` (NAR) for the JST
// today date and upserts each row into `odds_fetch_state` so the per-minute
// polling cron has something to plan against, even when the legacy worker is
// down and `forwardRaceSourceToHot` never fires.
//
// This is purely SELECT against Hyperdrive; no INSERT/UPDATE/DELETE flows
// into Postgres. The hot D1 upsert uses `on conflict(race_key) do update`
// so the legacy worker's later forwarded payload still wins when it lands.

import type { Pool } from "pg";

import { invalidateRaceListInKv } from "./gates/race-list-kv-cache";
import { buildRaceListUrl } from "./keiba-go";
import { getHotPool } from "./postgres-pool";
import { upsertOddsFetchState } from "./storage";
import { formatRaceStartJst, getTodayJst } from "./time";
import type { Env, OddsSource } from "./types";

export interface TodayRaceRow {
  source: OddsSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceKey: string;
  raceStartAtJst: string;
  debaUrl: string;
  oddsLinksJson: string;
}

export interface ListTodayRacesContext {
  pool?: Pool;
}

export interface PopulateTodayContext {
  pool?: Pool;
}

interface SourcedRaceRow {
  [key: string]: unknown;
  source: OddsSource;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  hasso_jikoku: string | null;
  kaisai_kai: string | null;
  kaisai_nichime: string | null;
}

const KEIBAJO_CODE_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
const HHMM_PATTERN = /^\d{4}$/u;
const JRA_PLACEHOLDER_URL = "https://www.jra.go.jp/";
const EMPTY_ODDS_LINKS_JSON = "{}";

const SELECT_TODAY_RACES_SQL = `
  select 'jra' as source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, hasso_jikoku, kaisai_kai, kaisai_nichime
  from jvd_ra
  where kaisai_nen = $1 and kaisai_tsukihi = $2
  union all
  select 'nar' as source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, hasso_jikoku, null as kaisai_kai, null as kaisai_nichime
  from nvd_ra
  where kaisai_nen = $1 and kaisai_tsukihi = $2
  order by source, keibajo_code, race_bango
`;

const isOddsSource = (value: unknown): value is OddsSource => value === "jra" || value === "nar";

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0;

const isNullableString = (value: unknown): value is string | null =>
  value === null || typeof value === "string";

const isCompleteRow = (row: Record<string, unknown>): row is SourcedRaceRow =>
  isOddsSource(row.source) &&
  isNonEmptyString(row.kaisai_nen) &&
  isNonEmptyString(row.kaisai_tsukihi) &&
  isNonEmptyString(row.keibajo_code) &&
  isNonEmptyString(row.race_bango) &&
  isNullableString(row.hasso_jikoku) &&
  isNullableString(row.kaisai_kai) &&
  isNullableString(row.kaisai_nichime);

const normaliseCode = (value: string, width: number): string => value.padStart(width, "0");

const buildRaceKey = (row: SourcedRaceRow): string => {
  const keibajoCode = normaliseCode(row.keibajo_code, KEIBAJO_CODE_PAD_WIDTH);
  const raceBango = normaliseCode(row.race_bango, RACE_BANGO_PAD_WIDTH);
  return `${row.source}:${row.kaisai_nen}:${row.kaisai_tsukihi}:${keibajoCode}:${raceBango}`;
};

const buildRaceStartAtJst = (row: SourcedRaceRow): string | null => {
  const hhmm = row.hasso_jikoku;
  if (!hhmm || !HHMM_PATTERN.test(hhmm)) {
    return null;
  }
  return formatRaceStartJst(row.kaisai_nen, row.kaisai_tsukihi, hhmm);
};

const buildDebaUrl = (row: SourcedRaceRow): string => {
  if (row.source === "nar") {
    const yyyymmdd = `${row.kaisai_nen}${row.kaisai_tsukihi}`;
    return buildRaceListUrl(yyyymmdd, normaliseCode(row.keibajo_code, KEIBAJO_CODE_PAD_WIDTH)).url;
  }
  return JRA_PLACEHOLDER_URL;
};

const toTodayRaceRow = (row: SourcedRaceRow): TodayRaceRow | null => {
  const raceStartAtJst = buildRaceStartAtJst(row);
  if (!raceStartAtJst) {
    return null;
  }
  return {
    debaUrl: buildDebaUrl(row),
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    keibajoCode: normaliseCode(row.keibajo_code, KEIBAJO_CODE_PAD_WIDTH),
    oddsLinksJson: EMPTY_ODDS_LINKS_JSON,
    raceBango: normaliseCode(row.race_bango, RACE_BANGO_PAD_WIDTH),
    raceKey: buildRaceKey(row),
    raceStartAtJst,
    source: row.source,
  };
};

const splitYyyymmdd = (yyyymmdd: string): { kaisaiNen: string; kaisaiTsukihi: string } => ({
  kaisaiNen: yyyymmdd.slice(0, 4),
  kaisaiTsukihi: yyyymmdd.slice(4, 8),
});

export const listTodayRacesFromHyperdrive = async (
  env: Env,
  yyyymmdd: string,
  context: ListTodayRacesContext = {},
): Promise<TodayRaceRow[]> => {
  const pool = context.pool ?? getHotPool(env);
  const { kaisaiNen, kaisaiTsukihi } = splitYyyymmdd(yyyymmdd);
  const result = await pool.query<Record<string, unknown>>(SELECT_TODAY_RACES_SQL, [
    kaisaiNen,
    kaisaiTsukihi,
  ]);
  return result.rows
    .filter(isCompleteRow)
    .map(toTodayRaceRow)
    .filter((entry): entry is TodayRaceRow => entry !== null);
};

export interface PopulateTodayOddsFetchStateResult {
  inserted: number;
  total: number;
}

interface InvalidationTarget {
  source: OddsSource;
  yyyymmdd: string;
}

const collectInvalidationTargets = (rows: TodayRaceRow[]): InvalidationTarget[] => {
  const seen = new Map<string, InvalidationTarget>();
  rows.forEach((row) => {
    const yyyymmdd = `${row.kaisaiNen}${row.kaisaiTsukihi}`;
    seen.set(`${row.source}:${yyyymmdd}`, { source: row.source, yyyymmdd });
  });
  return Array.from(seen.values());
};

export const populateTodayOddsFetchState = async (
  env: Env,
  now: Date,
  context: PopulateTodayContext = {},
): Promise<PopulateTodayOddsFetchStateResult> => {
  const yyyymmdd = getTodayJst(now);
  const rows = await listTodayRacesFromHyperdrive(env, yyyymmdd, { pool: context.pool });
  await Promise.all(
    rows.map((row) =>
      upsertOddsFetchState(env.REALTIME_HOT_DB, {
        debaUrl: row.debaUrl,
        kaisaiNen: row.kaisaiNen,
        kaisaiTsukihi: row.kaisaiTsukihi,
        keibajoCode: row.keibajoCode,
        oddsLinksJson: row.oddsLinksJson,
        raceBango: row.raceBango,
        raceKey: row.raceKey,
        raceStartAtJst: row.raceStartAtJst,
        source: row.source,
      }),
    ),
  );
  await Promise.all(
    collectInvalidationTargets(rows).map((target) =>
      invalidateRaceListInKv(env, target.source, target.yyyymmdd),
    ),
  );
  return { inserted: rows.length, total: rows.length };
};
