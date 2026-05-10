import { BABA_CODE_TO_LOCAL_KEIBAJO, buildRaceKey, type KeibaGoRaceLink } from "./keiba-go";
import { formatRaceStartJst, toJstIsoString } from "./time";
import type {
  HorseOddsTrend,
  HorseWeight,
  NarRaceSource,
  OddsData,
  OddsHistoryPoint,
  OddsType,
  RealtimeRacePayload,
} from "./types";

interface RaceSourceRow {
  baba_code: string;
  deba_url: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  last_odds_fetch_at: string | null;
  last_weight_fetch_at: string | null;
  odds_links_json: string;
  race_bango: string;
  race_key: string;
  race_name: string | null;
  race_start_at_jst: string;
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

export interface LocalRaceRow {
  hasso_jikoku: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  kyosomei_hondai: string | null;
  race_bango: string;
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

export const listFutureRaceSources = async (
  db: D1Database,
  targetDate: string,
  nowJst: string,
): Promise<NarRaceSource[]> => {
  const result = await db
    .prepare(
      `
        select *
        from nar_race_sources
        where kaisai_nen = ?
          and kaisai_tsukihi = ?
          and race_start_at_jst > ?
        order by race_start_at_jst asc
      `,
    )
    .bind(targetDate.slice(0, 4), targetDate.slice(4, 8), nowJst)
    .all<RaceSourceRow>();
  return result.results.map(toRaceSource);
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
  column: "last_odds_fetch_at" | "last_weight_fetch_at",
  fetchedAt: string,
): Promise<void> => {
  await db
    .prepare(`update nar_race_sources set ${column} = ?, updated_at = ? where race_key = ?`)
    .bind(fetchedAt, toJstIsoString(), raceKey)
    .run();
};

export const insertOddsSnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  odds: Partial<Record<OddsType, OddsData[]>>,
): Promise<void> => {
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
    await db.batch(statements);
  }
};

export const insertHorseWeightSnapshot = async (
  db: D1Database,
  raceKey: string,
  fetchedAt: string,
  weights: HorseWeight[],
): Promise<void> => {
  if (weights.length === 0) {
    return;
  }
  await db.batch(
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
    horseWeights: await getLatestHorseWeights(db, raceKey),
    odds: odds
      ? {
          fetchedAt: odds.fetchedAt,
          history,
          horseTrends: toHorseTrends(history),
          latest: odds.latest,
        }
      : null,
    raceKey,
    source,
  };
};
