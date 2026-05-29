// Run with bun. Loads WIN5 leg inputs (race meta + runners + horse history)
// from Postgres, with optional ML model score injection.

import type { Pool } from "pg";

import { computeHistoricalWinScore, type Win5LegInput, type Win5RunnerInput } from "./prediction";
import type { Win5RaceLeg, Win5Schedule } from "./types";

const RACE_BANGO_PAD_LENGTH = 2;
const JRA_SOURCE_PREFIX = "jra";

export interface Win5ModelScoreLookupParams {
  raceId: string;
  kettoTorokuBango: string;
}

export interface Win5ModelScoreLookup {
  get(params: Win5ModelScoreLookupParams): number | null;
}

export interface BuildWin5LegInputsParams {
  pool: Pool;
  schedule: Win5Schedule;
  modelScoreLookup?: Win5ModelScoreLookup;
}

interface ResolvedLegMetaRow {
  kaisai_kai: string;
  kaisai_nichime: string;
  kyosomei_hondai: string | null;
}

interface RunnerRow {
  bamei: string | null;
  ketto_toroku_bango: string;
  kishumei_ryakusho: string | null;
  tansho_ninkijun: string | null;
  tansho_odds: string | null;
  umaban: string;
}

interface HistoryRow {
  ketto_toroku_bango: string;
  runs: string;
  wins: string;
}

interface FetchLegMetaParams {
  pool: Pool;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

interface FetchRunnersParams {
  pool: Pool;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  kaisaiKai: string;
  kaisaiNichime: string;
  raceBango: string;
}

interface FetchHistoryParams {
  pool: Pool;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  kaisaiKai: string;
  kaisaiNichime: string;
  raceBango: string;
}

interface BuildLegParams {
  pool: Pool;
  schedule: Win5Schedule;
  leg: Win5RaceLeg;
  modelScoreLookup: Win5ModelScoreLookup | undefined;
}

interface BuildRunnerInputsParams {
  raceId: string;
  runnerRows: readonly RunnerRow[];
  historyMap: ReadonlyMap<string, number>;
  modelScoreLookup: Win5ModelScoreLookup | undefined;
}

const parseStoredNumber = (value: string | null | undefined): number | null => {
  const cleaned = (value ?? "").trim();
  if (!cleaned || /^0+$/u.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseOdds = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value);
  return parsed === null || parsed <= 0 ? null : parsed / 10;
};

const padRaceBango = (raceBango: string): string => raceBango.padStart(RACE_BANGO_PAD_LENGTH, "0");

const buildJraRaceId = (params: {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}): string =>
  `${JRA_SOURCE_PREFIX}:${params.kaisaiNen}:${params.kaisaiTsukihi}:${params.keibajoCode}:${params.raceBango}`;

const fetchLegMeta = async (
  params: FetchLegMetaParams,
): Promise<ResolvedLegMetaRow | undefined> => {
  const result = await params.pool.query<ResolvedLegMetaRow>(
    `
      select kaisai_kai, kaisai_nichime, kyosomei_hondai
        from jvd_ra
        where kaisai_nen = $1
          and kaisai_tsukihi = $2
          and keibajo_code = $3
          and race_bango = $4
        order by kaisai_kai asc, kaisai_nichime asc
        limit 1
    `,
    [params.kaisaiNen, params.kaisaiTsukihi, params.keibajoCode, params.raceBango],
  );
  return result.rows[0];
};

const fetchRunners = async (params: FetchRunnersParams): Promise<RunnerRow[]> => {
  const result = await params.pool.query<RunnerRow>(
    `
      select
        se.umaban,
        se.ketto_toroku_bango,
        se.bamei,
        se.kishumei_ryakusho,
        se.tansho_ninkijun,
        se.tansho_odds
      from jvd_se se
      where se.kaisai_nen = $1
        and se.kaisai_tsukihi = $2
        and se.keibajo_code = $3
        and se.kaisai_kai = $4
        and se.kaisai_nichime = $5
        and se.race_bango = $6
        and coalesce(se.ijo_kubun_code, '0') = '0'
      order by se.umaban::int asc
    `,
    [
      params.kaisaiNen,
      params.kaisaiTsukihi,
      params.keibajoCode,
      params.kaisaiKai,
      params.kaisaiNichime,
      params.raceBango,
    ],
  );
  return result.rows;
};

const fetchHistoryAggregates = async (params: FetchHistoryParams): Promise<HistoryRow[]> => {
  const result = await params.pool.query<HistoryRow>(
    `
      select
        se.ketto_toroku_bango,
        count(*)::text as runs,
        count(*) filter (where se.kakutei_chakujun = '01')::text as wins
      from jvd_se se
      where se.ketto_toroku_bango in (
        select ketto_toroku_bango
        from jvd_se
        where kaisai_nen = $1
          and kaisai_tsukihi = $2
          and keibajo_code = $3
          and kaisai_kai = $4
          and kaisai_nichime = $5
          and race_bango = $6
      )
        and (se.kaisai_nen, se.kaisai_tsukihi) < ($1::text, $2::text)
      group by se.ketto_toroku_bango
    `,
    [
      params.kaisaiNen,
      params.kaisaiTsukihi,
      params.keibajoCode,
      params.kaisaiKai,
      params.kaisaiNichime,
      params.raceBango,
    ],
  );
  return result.rows;
};

const buildHistoryMap = (rows: readonly HistoryRow[]): Map<string, number> => {
  const map = new Map<string, number>();
  rows.forEach((row) => {
    map.set(
      row.ketto_toroku_bango,
      computeHistoricalWinScore({
        runs: Number(row.runs),
        wins: Number(row.wins),
      }),
    );
  });
  return map;
};

const buildRunnerInputs = (params: BuildRunnerInputsParams): Win5RunnerInput[] =>
  params.runnerRows.map((row) => ({
    horseName: row.bamei?.trim() ?? row.umaban,
    horseNumber: row.umaban.replace(/^0+/u, "") || row.umaban,
    jockeyName: row.kishumei_ryakusho?.trim() ?? null,
    odds: parseOdds(row.tansho_odds),
    popularity: parseStoredNumber(row.tansho_ninkijun),
    historicalScore: params.historyMap.get(row.ketto_toroku_bango) ?? 0,
    modelScore:
      params.modelScoreLookup?.get({
        raceId: params.raceId,
        kettoTorokuBango: row.ketto_toroku_bango,
      }) ?? null,
  }));

const buildLeg = async (params: BuildLegParams): Promise<Win5LegInput | null> => {
  const raceBango = padRaceBango(params.leg.raceBango);
  const legMeta = await fetchLegMeta({
    pool: params.pool,
    kaisaiNen: params.schedule.kaisaiNen,
    kaisaiTsukihi: params.schedule.kaisaiTsukihi,
    keibajoCode: params.leg.keibajoCode,
    raceBango,
  });
  const kaisaiKai = legMeta?.kaisai_kai ?? params.leg.kaisaiKai ?? "00";
  const kaisaiNichime = legMeta?.kaisai_nichime ?? params.leg.kaisaiNichime ?? "00";
  const enrichedLeg: Win5RaceLeg = {
    ...params.leg,
    kaisaiKai,
    kaisaiNichime,
    raceLabel: params.leg.raceLabel ?? (legMeta?.kyosomei_hondai?.trim() || undefined),
  };
  const fetchArgs = {
    pool: params.pool,
    kaisaiNen: params.schedule.kaisaiNen,
    kaisaiTsukihi: params.schedule.kaisaiTsukihi,
    keibajoCode: enrichedLeg.keibajoCode,
    kaisaiKai,
    kaisaiNichime,
    raceBango,
  };
  const runnerRows = await fetchRunners(fetchArgs);
  if (runnerRows.length === 0) {
    return null;
  }
  const historyRows = await fetchHistoryAggregates(fetchArgs);
  const raceId = buildJraRaceId({
    kaisaiNen: params.schedule.kaisaiNen,
    kaisaiTsukihi: params.schedule.kaisaiTsukihi,
    keibajoCode: enrichedLeg.keibajoCode,
    raceBango,
  });
  const runners = buildRunnerInputs({
    raceId,
    runnerRows,
    historyMap: buildHistoryMap(historyRows),
    modelScoreLookup: params.modelScoreLookup,
  });
  return { leg: enrichedLeg, runners };
};

const isLegInput = (value: Win5LegInput | null): value is Win5LegInput => value !== null;

export const buildWin5LegInputsWithPool = async (
  params: BuildWin5LegInputsParams,
): Promise<Win5LegInput[]> => {
  const tasks = params.schedule.legs.map((leg) =>
    buildLeg({
      pool: params.pool,
      schedule: params.schedule,
      leg,
      modelScoreLookup: params.modelScoreLookup,
    }),
  );
  const results = await Promise.all(tasks);
  return results.filter(isLegInput);
};

export { buildJraRaceId, padRaceBango };
