import { toJstIsoString } from "./time";
import type {
  HorseOddsTrend,
  OddsData,
  OddsFetchStateRow,
  OddsFetchStateUpsertInput,
  OddsHistoryPoint,
  OddsSource,
  OddsTrend,
  OddsTrendPoint,
  OddsType,
  RaceListEntry,
} from "./types";

const D1_BATCH_SIZE = 100;

interface OddsFetchStateD1Row {
  deba_url: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  last_odds_fetch_at: string | null;
  last_odds_queued_at: string | null;
  odds_fetch_lock_until: string | null;
  odds_links_json: string;
  race_bango: string;
  race_key: string;
  race_start_at_jst: string;
  source: "jra" | "nar";
  updated_at: string;
}

const toOddsFetchStateRow = (row: OddsFetchStateD1Row): OddsFetchStateRow => ({
  debaUrl: row.deba_url,
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  lastOddsFetchAt: row.last_odds_fetch_at,
  lastOddsQueuedAt: row.last_odds_queued_at,
  oddsFetchLockUntil: row.odds_fetch_lock_until,
  oddsLinksJson: row.odds_links_json,
  raceBango: row.race_bango,
  raceKey: row.race_key,
  raceStartAtJst: row.race_start_at_jst,
  source: row.source,
  updatedAt: row.updated_at,
});

export const runD1Batches = async (
  db: D1Database,
  statements: D1PreparedStatement[],
): Promise<void> => {
  if (statements.length === 0) {
    return;
  }
  for (let i = 0; i < statements.length; i += D1_BATCH_SIZE) {
    await db.batch(statements.slice(i, i + D1_BATCH_SIZE));
  }
};

// True when there is no matching stored row (new combination) or any key
// field differs from the stored row. `?? null` normalizes undefined/missing so
// undefined-vs-undefined compares equal and a no-change race yields no writes.
export const hasOddsRowChanged = (next: OddsData, prev: OddsData | undefined): boolean => {
  if (!prev) {
    return true;
  }
  return (
    (next.odds ?? null) !== (prev.odds ?? null) ||
    (next.minOdds ?? null) !== (prev.minOdds ?? null) ||
    (next.maxOdds ?? null) !== (prev.maxOdds ?? null) ||
    (next.averageOdds ?? null) !== (prev.averageOdds ?? null) ||
    (next.rank ?? null) !== (prev.rank ?? null)
  );
};

// Returns only the rows that differ from the stored snapshot, keyed by
// combination. Odds types whose rows are all unchanged are omitted entirely so
// the R2 writer can skip "no-change" rows while still recording the latest
// full payload.
export const filterChangedOdds = (
  next: Partial<Record<OddsType, OddsData[]>>,
  stored: Partial<Record<OddsType, OddsData[]>>,
): Partial<Record<OddsType, OddsData[]>> => {
  const entries = Object.entries(next) as [OddsType, OddsData[]][];
  return Object.fromEntries(
    entries
      .map(([oddsType, rows]) => {
        const storedByCombination = new Map(
          (stored[oddsType] ?? []).map((row) => [row.combination, row]),
        );
        const changedRows = rows.filter((row) =>
          hasOddsRowChanged(row, storedByCombination.get(row.combination)),
        );
        return [oddsType, changedRows] satisfies [OddsType, OddsData[]];
      })
      .filter(([, changedRows]) => changedRows.length > 0),
  );
};

// Total number of odds rows across all types; used to distinguish a failed
// scrape (zero rows) from a legitimate no-change scrape (rows present but all
// already stored).
export const countOddsRows = (odds: Partial<Record<OddsType, OddsData[]>>): number =>
  Object.values(odds).reduce((total, rows) => total + (rows?.length ?? 0), 0);

export const toHorseTrends = (history: OddsHistoryPoint[]): HorseOddsTrend[] => {
  const byHorse = new Map<string, OddsHistoryPoint[]>();
  for (const point of history) {
    byHorse.set(point.horseNumber, [...(byHorse.get(point.horseNumber) ?? []), point]);
  }
  return Array.from(byHorse.entries()).map(([horseNumber, points]) => ({ horseNumber, points }));
};

export const toOddsTrendsByType = (
  historyByType: Partial<Record<OddsType, OddsTrendPoint[]>>,
): Partial<Record<OddsType, OddsTrend[]>> => {
  const result: Partial<Record<OddsType, OddsTrend[]>> = {};
  const entries = Object.entries(historyByType) as [OddsType, OddsTrendPoint[]][];
  for (const [oddsType, history] of entries) {
    const byCombination = new Map<string, OddsTrendPoint[]>();
    for (const point of history) {
      byCombination.set(point.combination, [
        ...(byCombination.get(point.combination) ?? []),
        point,
      ]);
    }
    result[oddsType] = Array.from(byCombination.entries()).map(([combination, points]) => ({
      combination,
      points,
    }));
  }
  return result;
};

export const upsertOddsFetchState = async (
  db: D1Database,
  input: OddsFetchStateUpsertInput,
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `insert into odds_fetch_state (race_key, source, race_start_at_jst, deba_url, odds_links_json, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, updated_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict(race_key) do update set source = excluded.source, race_start_at_jst = excluded.race_start_at_jst, deba_url = excluded.deba_url, odds_links_json = excluded.odds_links_json, kaisai_nen = excluded.kaisai_nen, kaisai_tsukihi = excluded.kaisai_tsukihi, keibajo_code = excluded.keibajo_code, race_bango = excluded.race_bango, updated_at = excluded.updated_at`,
    )
    .bind(
      input.raceKey,
      input.source,
      input.raceStartAtJst,
      input.debaUrl,
      input.oddsLinksJson,
      input.kaisaiNen,
      input.kaisaiTsukihi,
      input.keibajoCode,
      input.raceBango,
      now,
    )
    .run();
};

export const getOddsFetchState = async (
  db: D1Database,
  raceKey: string,
): Promise<OddsFetchStateRow | null> => {
  const row = await db
    .prepare(`select * from odds_fetch_state where race_key = ?`)
    .bind(raceKey)
    .first<OddsFetchStateD1Row>();
  return row ? toOddsFetchStateRow(row) : null;
};

export const countOddsFetchStateForDate = async (
  db: D1Database,
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<number> => {
  const row = await db
    .prepare(
      `select count(*) as count from odds_fetch_state where kaisai_nen = ? and kaisai_tsukihi = ?`,
    )
    .bind(kaisaiNen, kaisaiTsukihi)
    .first<{ count: number }>();
  return row?.count ?? 0;
};

export const listRaceKeysForDate = async (
  db: D1Database,
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<string[]> => {
  const result = await db
    .prepare(
      `select race_key from odds_fetch_state where kaisai_nen = ? and kaisai_tsukihi = ? order by race_key asc`,
    )
    .bind(kaisaiNen, kaisaiTsukihi)
    .all<{ race_key: string }>();
  return result.results.map((row) => row.race_key);
};

export const markOddsFetchStateDiscardedForRaceKeys = async (
  db: D1Database,
  raceKeys: string[],
  discardedAt: string,
): Promise<void> => {
  if (raceKeys.length === 0) {
    return;
  }
  await runD1Batches(
    db,
    raceKeys.map((raceKey) =>
      db
        .prepare(
          `update odds_fetch_state set last_odds_fetch_at = ?, last_odds_queued_at = null, odds_fetch_lock_until = null, updated_at = ? where race_key = ?`,
        )
        .bind(discardedAt, toJstIsoString(), raceKey),
    ),
  );
};

// Closing-odds safety net: select races whose last successful poll was more
// than 5 minutes before race start. The 5-minute threshold mirrors the
// betting-close gate — any race poll older than that missed the final
// finalOddsSlot window and should be re-fetched once after the day's races
// are done (idx_odds_fetch_state_date covers the kaisai_nen + kaisai_tsukihi
// + race_start_at_jst order-by).
export const listClosingBackfillCandidates = async (
  db: D1Database,
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<string[]> => {
  const result = await db
    .prepare(
      `select race_key from odds_fetch_state
       where kaisai_nen = ? and kaisai_tsukihi = ?
         and (
           last_odds_fetch_at is null
           or datetime(last_odds_fetch_at) < datetime(race_start_at_jst, '-5 minutes')
         )
       order by race_start_at_jst asc`,
    )
    .bind(kaisaiNen, kaisaiTsukihi)
    .all<{ race_key: string }>();
  return result.results.map((row) => row.race_key);
};

export const listOddsFetchStateForDate = async (
  db: D1Database,
  source: OddsSource,
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<RaceListEntry[]> => {
  const result = await db
    .prepare(
      `select race_key, source, race_start_at_jst, last_odds_fetch_at from odds_fetch_state where source = ? and kaisai_nen = ? and kaisai_tsukihi = ? order by race_start_at_jst asc`,
    )
    .bind(source, kaisaiNen, kaisaiTsukihi)
    .all<
      Pick<OddsFetchStateD1Row, "race_key" | "source" | "race_start_at_jst" | "last_odds_fetch_at">
    >();
  return result.results.map((row) => ({
    lastOddsFetchAt: row.last_odds_fetch_at,
    raceKey: row.race_key,
    raceStartAtJst: row.race_start_at_jst,
    source: row.source,
  }));
};

export const updateOddsLinks = async (
  db: D1Database,
  raceKey: string,
  oddsLinks: Partial<Record<OddsType, string>>,
): Promise<void> => {
  await db
    .prepare(`update odds_fetch_state set odds_links_json = ?, updated_at = ? where race_key = ?`)
    .bind(JSON.stringify(oddsLinks), toJstIsoString(), raceKey)
    .run();
};

export const markOddsFetchQueued = async (
  db: D1Database,
  raceKeys: string[],
  queuedAt: string,
): Promise<void> => {
  if (raceKeys.length === 0) {
    return;
  }
  await runD1Batches(
    db,
    raceKeys.map((raceKey) =>
      db
        .prepare(
          `update odds_fetch_state set last_odds_queued_at = ?, updated_at = ? where race_key = ? and (last_odds_fetch_at is null or last_odds_fetch_at <= ?)`,
        )
        .bind(queuedAt, queuedAt, raceKey, queuedAt),
    ),
  );
};

export const claimOddsFetch = async (
  db: D1Database,
  raceKey: string,
  lockUntil: string,
  now: string,
): Promise<boolean> => {
  const result = await db
    .prepare(
      `update odds_fetch_state set odds_fetch_lock_until = ?, updated_at = ? where race_key = ? and (odds_fetch_lock_until is null or odds_fetch_lock_until <= ?)`,
    )
    .bind(lockUntil, now, raceKey, now)
    .run();
  return result.meta.changes > 0;
};

export const completeOddsFetch = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
): Promise<void> => {
  await db
    .prepare(
      `update odds_fetch_state set last_odds_fetch_at = ?, last_odds_queued_at = null, odds_fetch_lock_until = null, updated_at = ? where race_key = ?`,
    )
    .bind(fetchedAt, toJstIsoString(), raceKey)
    .run();
};

export const failOddsFetch = async (db: D1Database, raceKey: string): Promise<void> => {
  await db
    .prepare(
      `update odds_fetch_state set last_odds_queued_at = null, odds_fetch_lock_until = null, updated_at = ? where race_key = ?`,
    )
    .bind(toJstIsoString(), raceKey)
    .run();
};

export const logFetch = async (
  db: D1Database,
  jobType: string,
  status: string,
  raceKey: string | null,
  message: string | null,
): Promise<void> => {
  await db
    .prepare(
      `insert into fetch_logs (race_key, job_type, status, message, created_at) values (?, ?, ?, ?, ?)`,
    )
    .bind(raceKey, jobType, status, message, toJstIsoString())
    .run();
};

export interface ImportOddsSnapshotRow {
  race_key: string;
  fetched_at: string;
  odds_type: string;
  combination: string;
  odds: number | null;
  min_odds: number | null;
  max_odds: number | null;
  average_odds: number | null;
  rank: number | null;
}

export const getNarVenueLastRaceStartAtJst = async (
  db: D1Database,
  kaisaiNen: string,
  kaisaiTsukihi: string,
  keibajoCode: string,
): Promise<string | null> => {
  const row = await db
    .prepare(
      `select max(race_start_at_jst) as last_race_start_at_jst from odds_fetch_state where source = 'nar' and kaisai_nen = ? and kaisai_tsukihi = ? and keibajo_code = ?`,
    )
    .bind(kaisaiNen, kaisaiTsukihi, keibajoCode)
    .first<{ last_race_start_at_jst: string | null }>();
  return row?.last_race_start_at_jst ?? null;
};
