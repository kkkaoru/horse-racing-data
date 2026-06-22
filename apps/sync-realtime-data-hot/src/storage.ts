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
const MAX_SAFE_RANK = Number.MAX_SAFE_INTEGER;
// Phase-1 fan-out controls how many distinct `fetched_at` values we pull per
// archive cron tick. The aggregator (Phase 2) scans ~200 rows per fetched_at
// (one row per horse-combination per odds_type), so 50 keeps the total work
// at ~10K rows — well inside D1's per-query CPU budget — while still moving
// the archive backlog meaningfully every cron tick (every fetched_at becomes
// one or more R2 objects).
// Per-tick cap on the Phase-1 `DISTINCT fetched_at` walk. Each distinct value
// fans out to roughly 6 (race_key, odds_type) aggregate groups in Phase 2,
// each of which becomes a single R2 put in `runScheduledArchive`. The cap is
// chosen so the total subrequest count per cron tick (~150 * 6 + 2 D1 reads +
// 1 fetch_logs write = ~903) stays under Cloudflare's 1000 paid subrequest
// budget. Raising this further requires either batching the puts or
// trimming D1 reads.
const DISTINCT_FETCHED_AT_LIMIT = 150;

const ODDS_TREND_LIMITS: Record<OddsType, number> = {
  "3renpuku": 30,
  "3rentan": 30,
  fukusho: 32,
  tansho: 32,
  umaren: 30,
  umatan: 30,
  wakuren: 18,
  wide: 30,
};

interface OddsSnapshotRow {
  average_odds: number | null;
  combination: string;
  fetched_at: string;
  max_odds: number | null;
  min_odds: number | null;
  odds: number | null;
  odds_type?: string;
  rank: number | null;
}

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

// ON CONFLICT DO UPDATE keeps retries (catch-up sweep, backfill scripts) idempotent
// after the (race_key, fetched_at, odds_type, combination) UNIQUE index lands in
// migration 0003.
export const insertOddsSnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  odds: Partial<Record<OddsType, OddsData[]>>,
): Promise<number> => {
  const statements = Object.entries(odds).flatMap(([type, rows]) =>
    (rows ?? []).map((row) =>
      db
        .prepare(
          `insert into odds_snapshots (race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank) values (?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict(race_key, fetched_at, odds_type, combination) do update set odds = excluded.odds, min_odds = excluded.min_odds, max_odds = excluded.max_odds, average_odds = excluded.average_odds, rank = excluded.rank`,
        )
        .bind(
          raceKey,
          fetchedAt,
          type,
          row.combination,
          row.odds ?? null,
          row.minOdds ?? null,
          row.maxOdds ?? null,
          row.averageOdds ?? null,
          row.rank ?? null,
        ),
    ),
  );
  await runD1Batches(db, statements);
  return statements.length;
};

export const getLatestOddsFromD1 = async (
  db: D1Database,
  raceKey: string,
): Promise<{ fetchedAt: string; latest: Partial<Record<OddsType, OddsData[]>> } | null> => {
  const result = await db
    .prepare(
      `select odds_type, fetched_at, combination, odds, min_odds, max_odds, average_odds, rank from odds_snapshots where race_key = ? and fetched_at = (select max(fetched_at) from odds_snapshots where race_key = ?) order by odds_type asc, coalesce(rank, 999999) asc`,
    )
    .bind(raceKey, raceKey)
    .all<OddsSnapshotRow>();
  const firstFetchedAt = result.results[0]?.fetched_at;
  if (!firstFetchedAt) {
    return null;
  }
  const grouped: Partial<Record<OddsType, OddsData[]>> = {};
  for (const row of result.results) {
    const oddsType = row.odds_type as OddsType | undefined;
    if (!oddsType) {
      continue;
    }
    grouped[oddsType] = [
      ...(grouped[oddsType] ?? []),
      {
        averageOdds: row.average_odds ?? undefined,
        combination: row.combination,
        maxOdds: row.max_odds ?? undefined,
        minOdds: row.min_odds ?? undefined,
        odds: row.odds ?? undefined,
        rank: row.rank ?? undefined,
      },
    ];
  }
  return { fetchedAt: firstFetchedAt, latest: grouped };
};

export const listTanshoHistory = async (
  db: D1Database,
  raceKey: string,
): Promise<OddsHistoryPoint[]> => {
  const result = await db
    .prepare(
      `select fetched_at, combination, odds, rank from odds_snapshots where race_key = ? and odds_type = 'tansho' order by fetched_at asc, cast(combination as integer) asc`,
    )
    .bind(raceKey)
    .all<OddsSnapshotRow>();
  return result.results.map((row) => ({
    fetchedAt: row.fetched_at,
    horseNumber: row.combination,
    odds: row.odds,
    popularity: row.rank,
  }));
};

const sortOddsRowsForTrend = (left: OddsSnapshotRow, right: OddsSnapshotRow): number => {
  const rankDiff = (left.rank ?? MAX_SAFE_RANK) - (right.rank ?? MAX_SAFE_RANK);
  if (rankDiff !== 0) {
    return rankDiff;
  }
  const oddsDiff = (left.odds ?? MAX_SAFE_RANK) - (right.odds ?? MAX_SAFE_RANK);
  if (oddsDiff !== 0) {
    return oddsDiff;
  }
  return left.combination.localeCompare(right.combination, "ja-JP", { numeric: true });
};

export const listOddsHistoryByType = async (
  db: D1Database,
  raceKey: string,
): Promise<Partial<Record<OddsType, OddsTrendPoint[]>>> => {
  const result = await db
    .prepare(
      `select odds_type, fetched_at, combination, odds, rank from odds_snapshots where race_key = ? order by odds_type asc, fetched_at asc, coalesce(rank, 999999) asc, combination asc`,
    )
    .bind(raceKey)
    .all<OddsSnapshotRow>();
  const rowsByType = new Map<OddsType, OddsSnapshotRow[]>();
  for (const row of result.results) {
    const oddsType = row.odds_type as OddsType | undefined;
    if (!oddsType) {
      continue;
    }
    rowsByType.set(oddsType, [...(rowsByType.get(oddsType) ?? []), row]);
  }
  const historyByType: Partial<Record<OddsType, OddsTrendPoint[]>> = {};
  for (const [oddsType, rows] of rowsByType) {
    const latestFetchedAt = rows.at(-1)?.fetched_at;
    if (!latestFetchedAt) {
      continue;
    }
    const selectedCombinations = new Set(
      rows
        .filter((row) => row.fetched_at === latestFetchedAt)
        .toSorted(sortOddsRowsForTrend)
        .slice(0, ODDS_TREND_LIMITS[oddsType])
        .map((row) => row.combination),
    );
    historyByType[oddsType] = rows
      .filter((row) => selectedCombinations.has(row.combination))
      .map((row) => ({
        combination: row.combination,
        fetchedAt: row.fetched_at,
        odds: row.odds,
        rank: row.rank,
      }));
  }
  return historyByType;
};

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

interface ListOddsSnapshotsCutoffOptions {
  cutoffIso: string;
  limit: number;
}

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

// ON CONFLICT DO UPDATE keeps F1 backfill retries idempotent against the
// (race_key, fetched_at, odds_type, combination) UNIQUE index (migration 0003).
export const bulkInsertOddsSnapshotRows = async (
  db: D1Database,
  rows: ImportOddsSnapshotRow[],
): Promise<number> => {
  if (rows.length === 0) {
    return 0;
  }
  const statements = rows.map((row) =>
    db
      .prepare(
        `insert into odds_snapshots (race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank) values (?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict(race_key, fetched_at, odds_type, combination) do update set odds = excluded.odds, min_odds = excluded.min_odds, max_odds = excluded.max_odds, average_odds = excluded.average_odds, rank = excluded.rank`,
      )
      .bind(
        row.race_key,
        row.fetched_at,
        row.odds_type,
        row.combination,
        row.odds,
        row.min_odds,
        row.max_odds,
        row.average_odds,
        row.rank,
      ),
  );
  await runD1Batches(db, statements);
  return rows.length;
};

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

export const listOddsSnapshotsBeforeCutoff = async (
  db: D1Database,
  options: ListOddsSnapshotsCutoffOptions,
): Promise<OddsSnapshotRow[]> => {
  const result = await db
    .prepare(
      `select average_odds, combination, fetched_at, max_odds, min_odds, odds, odds_type, rank from odds_snapshots where fetched_at < ? order by fetched_at asc limit ?`,
    )
    .bind(options.cutoffIso, options.limit)
    .all<OddsSnapshotRow>();
  return result.results;
};

interface AggregateArchiveRow {
  fetched_at: string;
  odds_type: string;
  race_key: string;
  snapshot_json: string;
}

interface DistinctFetchedAtRow {
  fetched_at: string;
}

// Builds the `?, ?, ?, ...` placeholder string for the Phase-2 `IN (...)`
// clause. Required because D1's prepared-statement binding does not accept
// an array directly for IN expressions.
const buildInPlaceholders = (count: number): string =>
  Array.from({ length: count }, () => "?").join(", ");

// Phase 1: pick the oldest `DISTINCT fetched_at` values that fall before the
// cutoff. This walks `idx_odds_snapshots_fetched_at` as a covering index
// (`SEARCH ... USING COVERING INDEX (fetched_at<?)`), so even with 16M+
// qualifying rows the read is bounded to a few thousand entries before the
// LIMIT kicks in.
export const listDistinctArchiveFetchedAtBeforeCutoff = async (
  db: D1Database,
  options: ListOddsSnapshotsCutoffOptions,
): Promise<string[]> => {
  const result = await db
    .prepare(
      `select distinct fetched_at from odds_snapshots where fetched_at < ? order by fetched_at asc limit ?`,
    )
    .bind(options.cutoffIso, options.limit)
    .all<DistinctFetchedAtRow>();
  return result.results.map((row) => row.fetched_at);
};

// Phase 2: aggregate `odds_snapshots` rows whose `fetched_at` is in the
// bounded list produced by Phase 1. SQLite uses
// `idx_odds_snapshots_fetched_at (fetched_at=?)` per IN value, so the scan
// touches only the ~200 rows per timestamp and the GROUP BY operates on a
// tiny working set.
export const aggregateArchiveRowsForFetchedAtList = async (
  db: D1Database,
  fetchedAtList: string[],
): Promise<AggregateArchiveRow[]> => {
  if (fetchedAtList.length === 0) {
    return [];
  }
  const placeholders = buildInPlaceholders(fetchedAtList.length);
  const result = await db
    .prepare(
      `select race_key, odds_type, fetched_at, json_group_array(json_object('combination', combination, 'odds', odds, 'min_odds', min_odds, 'max_odds', max_odds, 'average_odds', average_odds, 'rank', rank)) as snapshot_json from odds_snapshots where fetched_at in (${placeholders}) group by race_key, odds_type, fetched_at order by fetched_at asc`,
    )
    .bind(...fetchedAtList)
    .all<AggregateArchiveRow>();
  return result.results;
};

// Two-phase archive lookup. The legacy single-query version
// (`where fetched_at < ? group by ... order by fetched_at asc limit ?`) ran
// `SCAN odds_snapshots USING INDEX (race_key, fetched_at, odds_type, combination)`
// followed by `USE TEMP B-TREE FOR ORDER BY`, which forced SQLite to scan
// every qualifying row (~16M+) and materialize all groups before applying
// the LIMIT — every cron tick tripped D1's per-query CPU/timeout limit and
// the bucket stayed empty for 25+ days. The split avoids that path entirely:
// Phase 1 is a covering-index walk, Phase 2 is a small IN-bounded aggregate.
export const listArchiveCandidatesBeforeCutoff = async (
  db: D1Database,
  options: ListOddsSnapshotsCutoffOptions,
): Promise<AggregateArchiveRow[]> => {
  const fetchedAtList = await listDistinctArchiveFetchedAtBeforeCutoff(db, {
    cutoffIso: options.cutoffIso,
    limit: Math.min(options.limit, DISTINCT_FETCHED_AT_LIMIT),
  });
  return aggregateArchiveRowsForFetchedAtList(db, fetchedAtList);
};
