import { BABA_CODE_TO_LOCAL_KEIBAJO, buildRaceKey, type KeibaGoRaceLink } from "./keiba-go";
import { formatRaceStartJst, toJstIsoString } from "./time";
import type {
  HorseOddsTrend,
  HorseWeight,
  NarRaceSource,
  OddsData,
  OddsHistoryPoint,
  OddsType,
  RaceEntry,
  RaceResult,
  RealtimeRacePayload,
} from "./types";

const D1_BATCH_SIZE = 100;

interface RaceSourceRow {
  baba_code: string;
  deba_url: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  last_odds_fetch_at: string | null;
  last_odds_queued_at: string | null;
  last_result_fetch_at: string | null;
  last_result_queued_at: string | null;
  last_weight_fetch_at: string | null;
  odds_fetch_lock_until: string | null;
  odds_links_json: string;
  race_bango: string;
  race_key: string;
  race_name: string | null;
  race_start_at_jst: string;
  result_complete_at: string | null;
  result_expected_horse_count: number | null;
  result_fetch_lock_until: string | null;
  result_saved_horse_count: number | null;
}

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

interface WeightSnapshotRow {
  change_amount: number | null;
  change_sign: string | null;
  fetched_at: string;
  horse_name: string | null;
  horse_number: string;
  weight: number | null;
}

interface RaceEntrySnapshotRow {
  fetched_at: string;
  horse_name: string | null;
  horse_number: string;
  jockey_name: string | null;
  status: string | null;
}

interface RaceResultSnapshotRow {
  fetched_at: string;
  finish_position: string;
  horse_name: string | null;
  horse_number: string;
  time: string | null;
}

export interface LocalRaceRow {
  hasso_jikoku: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  kyosomei_hondai: string | null;
  race_bango: string;
}

export interface SchedulableRaceSource extends NarRaceSource {
  lastOddsQueuedAt: string | null;
  lastResultFetchAt: string | null;
  lastResultQueuedAt: string | null;
  oddsFetchLockUntil: string | null;
  resultCompleteAt: string | null;
  resultFetchLockUntil: string | null;
}

const parseOddsLinks = (value: string): Partial<Record<OddsType, string>> => {
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === "object" && parsed !== null ? parsed : {};
  } catch {
    return {};
  }
};

const toRaceSource = (row: RaceSourceRow): NarRaceSource => ({
  babaCode: row.baba_code,
  debaUrl: row.deba_url,
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  lastOddsFetchAt: row.last_odds_fetch_at,
  lastWeightFetchAt: row.last_weight_fetch_at,
  oddsLinks: parseOddsLinks(row.odds_links_json),
  raceBango: row.race_bango,
  raceKey: row.race_key,
  raceName: row.race_name,
  raceStartAtJst: row.race_start_at_jst,
});

const toSchedulableRaceSource = (row: RaceSourceRow): SchedulableRaceSource => ({
  ...toRaceSource(row),
  lastOddsQueuedAt: row.last_odds_queued_at,
  lastResultFetchAt: row.last_result_fetch_at,
  lastResultQueuedAt: row.last_result_queued_at,
  oddsFetchLockUntil: row.odds_fetch_lock_until,
  resultCompleteAt: row.result_complete_at,
  resultFetchLockUntil: row.result_fetch_lock_until,
});

const runD1Batches = async (db: D1Database, statements: D1PreparedStatement[]): Promise<void> => {
  for (let index = 0; index < statements.length; index += D1_BATCH_SIZE) {
    await db.batch(statements.slice(index, index + D1_BATCH_SIZE));
  }
};

export const upsertNarRaceSource = async (
  db: D1Database,
  link: KeibaGoRaceLink,
  race: LocalRaceRow,
  oddsLinks: Partial<Record<OddsType, string>>,
): Promise<void> => {
  const keibajoCode = BABA_CODE_TO_LOCAL_KEIBAJO[link.babaCode];
  if (!keibajoCode || !race.hasso_jikoku) {
    return;
  }
  const raceBango = race.race_bango.padStart(2, "0");
  const raceKey = buildRaceKey(race.kaisai_nen, race.kaisai_tsukihi, keibajoCode, raceBango);
  const now = toJstIsoString();
  await db
    .prepare(
      `
        insert into nar_race_sources (
          race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          baba_code, race_start_at_jst, race_name, deba_url, odds_links_json,
          discovered_at, updated_at
        )
        values (?, 'nar', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(race_key) do update set
          baba_code = excluded.baba_code,
          race_start_at_jst = excluded.race_start_at_jst,
          race_name = excluded.race_name,
          deba_url = excluded.deba_url,
          odds_links_json = excluded.odds_links_json,
          updated_at = excluded.updated_at
      `,
    )
    .bind(
      raceKey,
      race.kaisai_nen,
      race.kaisai_tsukihi,
      keibajoCode,
      raceBango,
      link.babaCode,
      formatRaceStartJst(race.kaisai_nen, race.kaisai_tsukihi, race.hasso_jikoku),
      race.kyosomei_hondai,
      link.url,
      JSON.stringify(oddsLinks),
      now,
      now,
    )
    .run();
};

export const listRaceSourceKeibajoCodesByDate = async (
  db: D1Database,
  targetDate: string,
): Promise<string[]> => {
  const result = await db
    .prepare(
      `
        select distinct keibajo_code
        from nar_race_sources
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
        order by keibajo_code
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8))
    .all<{ keibajo_code: string }>();
  return result.results.map((row) => row.keibajo_code);
};

export const getRaceSource = async (
  db: D1Database,
  raceKey: string,
): Promise<NarRaceSource | null> => {
  const row = await db
    .prepare("select * from nar_race_sources where race_key = ?")
    .bind(raceKey)
    .first<RaceSourceRow>();
  return row ? toRaceSource(row) : null;
};

export const listSchedulableRaceSourcesByDate = async (
  db: D1Database,
  targetDate: string,
): Promise<SchedulableRaceSource[]> => {
  const result = await db
    .prepare(
      `
        select *
        from nar_race_sources
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
        order by race_start_at_jst asc
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8))
    .all<RaceSourceRow>();
  return result.results.map(toSchedulableRaceSource);
};

export const countRaceSourcesByDate = async (
  db: D1Database,
  targetDate: string,
): Promise<number> => {
  const row = await db
    .prepare(
      `
        select count(*) count
        from nar_race_sources
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8))
    .first<{ count: number }>();
  return Number(row?.count ?? 0);
};

export const updateOddsLinks = async (
  db: D1Database,
  raceKey: string,
  oddsLinks: Partial<Record<OddsType, string>>,
): Promise<void> => {
  await db
    .prepare("update nar_race_sources set odds_links_json = ?, updated_at = ? where race_key = ?")
    .bind(JSON.stringify(oddsLinks), toJstIsoString(), raceKey)
    .run();
};

export const updateLastFetch = async (
  db: D1Database,
  raceKey: string,
  column: "last_odds_fetch_at" | "last_result_fetch_at" | "last_weight_fetch_at",
  fetchedAt: string,
): Promise<void> => {
  await db
    .prepare(`update nar_race_sources set ${column} = ?, updated_at = ? where race_key = ?`)
    .bind(fetchedAt, toJstIsoString(), raceKey)
    .run();
};

export const markResultFetchQueued = async (
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
          `
            update nar_race_sources
            set last_result_queued_at = ?, updated_at = ?
            where race_key = ?
              and result_complete_at is null
              and (last_result_queued_at is null or last_result_queued_at <= ?)
          `,
        )
        .bind(queuedAt, queuedAt, raceKey, queuedAt),
    ),
  );
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
          `
            update nar_race_sources
            set last_odds_queued_at = ?, updated_at = ?
            where race_key = ?
              and (last_odds_fetch_at is null or last_odds_fetch_at <= ?)
          `,
        )
        .bind(queuedAt, queuedAt, raceKey, queuedAt),
    ),
  );
};

export const claimOddsFetch = async (
  db: D1Database,
  raceKey: string,
  lockUntil: string,
  now = toJstIsoString(),
): Promise<boolean> => {
  const result = await db
    .prepare(
      `
        update nar_race_sources
        set odds_fetch_lock_until = ?, updated_at = ?
        where race_key = ?
          and (odds_fetch_lock_until is null or odds_fetch_lock_until <= ?)
      `,
    )
    .bind(lockUntil, now, raceKey, now)
    .run();
  return result.meta.changes > 0;
};

export const claimResultFetch = async (
  db: D1Database,
  raceKey: string,
  lockUntil: string,
  now = toJstIsoString(),
): Promise<boolean> => {
  const result = await db
    .prepare(
      `
        update nar_race_sources
        set result_fetch_lock_until = ?, updated_at = ?
        where race_key = ?
          and result_complete_at is null
          and (result_fetch_lock_until is null or result_fetch_lock_until <= ?)
      `,
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
  const now = toJstIsoString();
  await db
    .prepare(
      `
        update nar_race_sources
        set last_odds_fetch_at = ?,
            last_odds_queued_at = null,
            odds_fetch_lock_until = null,
            updated_at = ?
        where race_key = ?
      `,
    )
    .bind(fetchedAt, now, raceKey)
    .run();
};

export const failOddsFetch = async (db: D1Database, raceKey: string): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        update nar_race_sources
        set last_odds_queued_at = null,
            odds_fetch_lock_until = null,
            updated_at = ?
        where race_key = ?
      `,
    )
    .bind(now, raceKey)
    .run();
};

export const completeResultFetch = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  completion: {
    expectedHorseCount: number;
    isComplete: boolean;
    savedHorseCount: number;
  },
): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        update nar_race_sources
        set last_result_fetch_at = ?,
            last_result_queued_at = null,
            result_fetch_lock_until = null,
            result_complete_at = case when ? then ? else result_complete_at end,
            result_expected_horse_count = ?,
            result_saved_horse_count = ?,
            updated_at = ?
        where race_key = ?
      `,
    )
    .bind(
      fetchedAt,
      completion.isComplete ? 1 : 0,
      fetchedAt,
      completion.expectedHorseCount,
      completion.savedHorseCount,
      now,
      raceKey,
    )
    .run();
};

export const failResultFetch = async (db: D1Database, raceKey: string): Promise<void> => {
  const now = toJstIsoString();
  await db
    .prepare(
      `
        update nar_race_sources
        set last_result_queued_at = null,
            result_fetch_lock_until = null,
            updated_at = ?
        where race_key = ?
          and result_complete_at is null
      `,
    )
    .bind(now, raceKey)
    .run();
};

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
          `
            insert into odds_snapshots (
              race_key, fetched_at, odds_type, combination, odds,
              min_odds, max_odds, average_odds, rank
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
          `,
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
  if (statements.length > 0) {
    await runD1Batches(db, statements);
  }
  return statements.length;
};

export const insertHorseWeightSnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  weights: HorseWeight[],
): Promise<void> => {
  await db.prepare("delete from horse_weight_snapshots where race_key = ?").bind(raceKey).run();
  if (weights.length === 0) {
    return;
  }
  await runD1Batches(
    db,
    weights.map((weight) =>
      db
        .prepare(
          `
            insert into horse_weight_snapshots (
              race_key, fetched_at, horse_number, horse_name,
              weight, change_sign, change_amount
            )
            values (?, ?, ?, ?, ?, ?, ?)
          `,
        )
        .bind(
          raceKey,
          fetchedAt,
          weight.horseNumber,
          weight.horseName,
          weight.weight,
          weight.changeSign,
          weight.changeAmount,
        ),
    ),
  );
};

export const insertRaceEntrySnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  entries: Omit<RaceEntry, "fetchedAt">[],
): Promise<number> => {
  await db.prepare("delete from race_entry_snapshots where race_key = ?").bind(raceKey).run();
  if (entries.length === 0) {
    return 0;
  }
  await runD1Batches(
    db,
    entries.map((entry) =>
      db
        .prepare(
          `
            insert into race_entry_snapshots (
              race_key, fetched_at, horse_number, horse_name, jockey_name, status
            )
            values (?, ?, ?, ?, ?, ?)
          `,
        )
        .bind(
          raceKey,
          fetchedAt,
          entry.horseNumber,
          entry.horseName,
          entry.jockeyName,
          entry.status,
        ),
    ),
  );
  return entries.length;
};

export const insertRaceResultSnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  results: Omit<RaceResult, "fetchedAt">[],
): Promise<number> => {
  if (results.length === 0) {
    return 0;
  }
  await db.prepare("delete from race_result_snapshots where race_key = ?").bind(raceKey).run();
  await runD1Batches(
    db,
    results.map((result) =>
      db
        .prepare(
          `
            insert into race_result_snapshots (
              race_key, fetched_at, horse_number, horse_name,
              finish_position, time
            )
            values (?, ?, ?, ?, ?, ?)
          `,
        )
        .bind(
          raceKey,
          fetchedAt,
          result.horseNumber,
          result.horseName,
          result.finishPosition,
          result.time,
        ),
    ),
  );
  return results.length;
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
      "insert into fetch_logs (race_key, job_type, status, message, created_at) values (?, ?, ?, ?, ?)",
    )
    .bind(raceKey, jobType, status, message, toJstIsoString())
    .run();
};

export const listTanshoHistory = async (
  db: D1Database,
  raceKey: string,
): Promise<OddsHistoryPoint[]> => {
  const result = await db
    .prepare(
      `
        select fetched_at, combination, odds, rank
        from odds_snapshots
        where race_key = ?
          and odds_type = 'tansho'
        order by fetched_at asc, cast(combination as integer) asc
      `,
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

export const getLatestOddsFromD1 = async (
  db: D1Database,
  raceKey: string,
): Promise<{
  fetchedAt: string;
  latest: Partial<Record<OddsType, OddsData[]>>;
} | null> => {
  const latest = await db
    .prepare("select max(fetched_at) as fetched_at from odds_snapshots where race_key = ?")
    .bind(raceKey)
    .first<{ fetched_at: string | null }>();
  if (!latest?.fetched_at) {
    return null;
  }
  const result = await db
    .prepare(
      `
        select odds_type, fetched_at, combination, odds, min_odds, max_odds, average_odds, rank
        from odds_snapshots
        where race_key = ? and fetched_at = ?
        order by odds_type asc, coalesce(rank, 999999) asc
      `,
    )
    .bind(raceKey, latest.fetched_at)
    .all<OddsSnapshotRow>();
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
  return {
    fetchedAt: latest.fetched_at,
    latest: grouped,
  };
};

export const toHorseTrends = (history: OddsHistoryPoint[]): HorseOddsTrend[] => {
  const byHorse = new Map<string, OddsHistoryPoint[]>();
  for (const point of history) {
    byHorse.set(point.horseNumber, [...(byHorse.get(point.horseNumber) ?? []), point]);
  }
  return Array.from(byHorse.entries()).map(([horseNumber, points]) => ({ horseNumber, points }));
};

export const getLatestHorseWeights = async (
  db: D1Database,
  raceKey: string,
): Promise<{ fetchedAt: string; horses: HorseWeight[] } | null> => {
  const latest = await db
    .prepare("select max(fetched_at) as fetched_at from horse_weight_snapshots where race_key = ?")
    .bind(raceKey)
    .first<{ fetched_at: string | null }>();
  if (!latest?.fetched_at) {
    return null;
  }
  const result = await db
    .prepare(
      `
        select *
        from horse_weight_snapshots
        where race_key = ? and fetched_at = ?
        order by cast(horse_number as integer) asc
      `,
    )
    .bind(raceKey, latest.fetched_at)
    .all<WeightSnapshotRow>();
  return {
    fetchedAt: latest.fetched_at,
    horses: result.results.map((row) => ({
      changeAmount: row.change_amount,
      changeSign: row.change_sign,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
      weight: row.weight,
    })),
  };
};

export const getLatestRaceEntries = async (
  db: D1Database,
  raceKey: string,
): Promise<{ fetchedAt: string; horses: RaceEntry[] } | null> => {
  const latest = await db
    .prepare("select max(fetched_at) as fetched_at from race_entry_snapshots where race_key = ?")
    .bind(raceKey)
    .first<{ fetched_at: string | null }>();
  if (!latest?.fetched_at) {
    return null;
  }
  const result = await db
    .prepare(
      `
        select *
        from race_entry_snapshots
        where race_key = ? and fetched_at = ?
        order by cast(horse_number as integer) asc
      `,
    )
    .bind(raceKey, latest.fetched_at)
    .all<RaceEntrySnapshotRow>();
  return {
    fetchedAt: latest.fetched_at,
    horses: result.results.map((row) => ({
      fetchedAt: row.fetched_at,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
      jockeyName: row.jockey_name,
      status: row.status,
    })),
  };
};

export const getLatestRaceResults = async (
  db: D1Database,
  raceKey: string,
): Promise<{ fetchedAt: string; horses: RaceResult[] } | null> => {
  const latest = await db
    .prepare("select max(fetched_at) as fetched_at from race_result_snapshots where race_key = ?")
    .bind(raceKey)
    .first<{ fetched_at: string | null }>();
  if (!latest?.fetched_at) {
    return null;
  }
  const result = await db
    .prepare(
      `
        select *
        from race_result_snapshots
        where race_key = ? and fetched_at = ?
        order by
          case when finish_position glob '[0-9]*' then cast(finish_position as integer) else 999 end asc,
          cast(horse_number as integer) asc
      `,
    )
    .bind(raceKey, latest.fetched_at)
    .all<RaceResultSnapshotRow>();
  return {
    fetchedAt: latest.fetched_at,
    horses: result.results.map((row) => ({
      fetchedAt: row.fetched_at,
      finishPosition: row.finish_position,
      horseName: row.horse_name,
      horseNumber: row.horse_number,
      time: row.time,
    })),
  };
};

export const buildRealtimePayload = async (
  db: D1Database,
  raceKey: string,
  source: NarRaceSource | null,
  odds: {
    fetchedAt: string;
    latest: Partial<Record<OddsType, OddsData[]>>;
  } | null,
): Promise<RealtimeRacePayload> => {
  const history = await listTanshoHistory(db, raceKey);
  return {
    raceEntries: await getLatestRaceEntries(db, raceKey),
    horseWeights: await getLatestHorseWeights(db, raceKey),
    odds: odds
      ? {
          fetchedAt: odds.fetchedAt,
          history,
          horseTrends: toHorseTrends(history),
          latest: odds.latest,
        }
      : null,
    raceResults: await getLatestRaceResults(db, raceKey),
    raceKey,
    source,
  };
};
