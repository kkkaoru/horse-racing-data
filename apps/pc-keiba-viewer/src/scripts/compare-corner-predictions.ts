import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Pool } from "pg";

import type { RaceSource } from "../lib/codes";
import { isCornerPacePredictionSupported } from "../lib/race-pace-prediction";
import { buildRacePacePredictionRowsFromResults } from "../lib/race-pace-prediction";
import type { RacePacePredictionModel } from "../lib/race-pace-prediction";
import type {
  HorseRaceResult,
  RaceDetail,
  RacePacePredictionRow,
  RacePaceSimilarityFeature,
  Runner,
} from "../lib/race-types";

export type CliOptions = {
  concurrency: number;
  fromDate: string;
  fromYear: string | null;
  model?: RacePacePredictionModel;
  output: "json" | "text";
  sourceScope: RaceSource | "all";
  target: "local" | "neon";
  toDate: string;
  toYear: string | null;
};

type RaceRow = {
  source: RaceSource;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  kyosomei_hondai: string | null;
  kyosomei_fukudai: string | null;
  kyosomei_kakkonai: string | null;
  grade_code: string | null;
  kyoso_shubetsu_code: string | null;
  kyoso_kigo_code: string | null;
  juryo_shubetsu_code: string | null;
  kyoso_joken_code: string | null;
  kyoso_joken_meisho: string | null;
  kyori: string | null;
  track_code: string | null;
  hasso_jikoku: string | null;
  toroku_tosu: string | null;
  shusso_tosu: string | null;
  tenko_code: string | null;
  babajotai_code_shiba: string | null;
  babajotai_code_dirt: string | null;
  kaisai_kai: string | null;
  kaisai_nichime: string | null;
};

type RunnerRow = {
  wakuban: string | null;
  umaban: string | null;
  ketto_toroku_bango: string | null;
  bamei: string | null;
  seibetsu_code: string | null;
  barei: string | null;
  futan_juryo: string | null;
  kishumei_ryakusho: string | null;
  chokyoshimei_ryakusho: string | null;
  banushimei: string | null;
  bataiju: string | null;
  zogen_fugo: string | null;
  zogen_sa: string | null;
  kakutei_chakujun: string | null;
  tansho_odds: string | null;
  tansho_ninkijun: string | null;
  soha_time: string | null;
  time_sa: string | null;
  corner_1: string | null;
  corner_2: string | null;
  corner_3: string | null;
  corner_4: string | null;
  kohan_3f: string | null;
};

type HistoryRow = RunnerRow & {
  current_barei: string | null;
  current_jockey: string | null;
  current_seibetsu_code: string | null;
  current_umaban: string | null;
  history_kaisai_nen: string;
  history_kaisai_tsukihi: string;
  history_keibajo_code: string;
  history_race_bango: string;
  history_kyosomei_hondai: string | null;
  history_kyosomei_fukudai: string | null;
  history_kyosomei_kakkonai: string | null;
  history_grade_code: string | null;
  history_kyoso_shubetsu_code: string | null;
  history_kyoso_kigo_code: string | null;
  history_juryo_shubetsu_code: string | null;
  history_kyoso_joken_code: string | null;
  history_kyoso_joken_meisho: string | null;
  history_kyori: string | null;
  history_track_code: string | null;
  history_hasso_jikoku: string | null;
  history_shusso_tosu: string | null;
  history_tenko_code: string | null;
  history_babajotai_code_shiba: string | null;
  history_babajotai_code_dirt: string | null;
};

type CornerKey = "corner1" | "corner2" | "corner3" | "corner4";

export type CornerComparison = {
  actualOrder: string[];
  exactMatches: number;
  meanAbsolutePositionError: number | null;
  predictedOrder: string[];
  score: number | null;
};

export type RaceComparison = {
  corners: Partial<Record<CornerKey, CornerComparison>>;
  date: string;
  keibajoCode: string;
  raceName: string;
  raceNumber: string;
  runnerCount: number;
  source: RaceSource;
};

export type MonthSummary = {
  averageScore: number | null;
  comparedCorners: number;
  comparedRaces: number;
  exactMatches: number;
  month: string;
  runnerPositions: number;
  skippedRaces: number;
  year: string;
};

export type AggregateSummary = {
  averageScore: number | null;
  comparedCorners: number;
  comparedRaces: number;
  exactMatches: number;
  monthSummaries: MonthSummary[];
  races: RaceComparison[];
  runnerPositions: number;
  skippedRaces: number;
};

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const APP_DIR = resolve(SCRIPT_DIR, "../..");
const ROOT_DIR = resolve(APP_DIR, "../..");
const CORNER_KEYS: CornerKey[] = ["corner1", "corner2", "corner3", "corner4"];

const pad2 = (value: number): string => String(value).padStart(2, "0");

const formatDate = (date: Date): string =>
  `${date.getFullYear()}${pad2(date.getMonth() + 1)}${pad2(date.getDate())}`;

const addDays = (date: Date, days: number): Date => {
  const nextDate = new Date(date);
  nextDate.setDate(nextDate.getDate() + days);
  return nextDate;
};

const addYears = (date: Date, years: number): Date => {
  const nextDate = new Date(date);
  nextDate.setFullYear(nextDate.getFullYear() + years);
  return nextDate;
};

const parseDate = (value: string): Date => {
  const normalized = value.replaceAll("-", "");
  if (!/^\d{8}$/u.test(normalized)) {
    throw new Error(`Invalid date: ${value}`);
  }
  return new Date(
    Number(normalized.slice(0, 4)),
    Number(normalized.slice(4, 6)) - 1,
    Number(normalized.slice(6, 8)),
  );
};

const getDefaultDateRange = (): { fromDate: string; toDate: string } => {
  const today = new Date();
  const endDate = addDays(today, -1);
  const startDate = addYears(endDate, -1);
  return {
    fromDate: formatDate(startDate),
    toDate: formatDate(endDate),
  };
};

const parseArgs = (args: string[]): CliOptions => {
  const defaults = getDefaultDateRange();
  const options: CliOptions = {
    concurrency: 4,
    fromDate: defaults.fromDate,
    fromYear: null,
    output: "text",
    sourceScope: "all",
    target: "local",
    toDate: defaults.toDate,
    toYear: null,
  };

  for (let index = 0; index < args.length; index += 1) {
    const name = args[index];
    const value = args[index + 1];
    if (name === "--help" || name === "-h") {
      printHelp();
      process.exit(0);
    }
    if (!value) {
      throw new Error(`${name} requires a value.`);
    }
    if (name === "--from-date") {
      options.fromDate = value.replaceAll("-", "");
      index += 1;
    } else if (name === "--to-date") {
      options.toDate = value.replaceAll("-", "");
      index += 1;
    } else if (name === "--from-year") {
      options.fromYear = value;
      index += 1;
    } else if (name === "--to-year") {
      options.toYear = value;
      index += 1;
    } else if (name === "--year") {
      options.fromYear = value;
      options.toYear = value;
      index += 1;
    } else if (name === "--concurrency") {
      options.concurrency = Math.max(1, Number(value));
      index += 1;
    } else if (name === "--target") {
      if (value !== "local" && value !== "neon") {
        throw new Error("--target must be local or neon.");
      }
      options.target = value;
      index += 1;
    } else if (name === "--source-scope") {
      if (value !== "all" && value !== "jra" && value !== "nar") {
        throw new Error("--source-scope must be all, jra, or nar.");
      }
      options.sourceScope = value;
      index += 1;
    } else if (name === "--output") {
      if (value !== "text" && value !== "json") {
        throw new Error("--output must be text or json.");
      }
      options.output = value;
      index += 1;
    } else {
      throw new Error(`Unknown argument: ${name}`);
    }
  }

  if (options.fromYear || options.toYear) {
    const fromYear = options.fromYear ?? options.toYear;
    const toYear = options.toYear ?? options.fromYear;
    if (!fromYear || !toYear) {
      throw new Error("Year range is invalid.");
    }
    options.fromDate = `${fromYear}0101`;
    options.toDate = `${toYear}1231`;
  }

  parseDate(options.fromDate);
  parseDate(options.toDate);
  return options;
};

const printHelp = () => {
  console.log(`Usage:
  bun run src/scripts/compare-corner-predictions.ts [options]

Options:
  --from-date YYYY-MM-DD   Start date. Default: one year before yesterday.
  --to-date YYYY-MM-DD     End date. Default: yesterday.
  --year YYYY              Validate one full year.
  --from-year YYYY         Start year for full-year range.
  --to-year YYYY           End year for full-year range.
  --target local|neon      Database target. Default: local.
  --source-scope all|jra|nar
  --concurrency N          Parallel month workers. Default: 4.
  --output text|json       Output format. Default: text.
`);
};

const loadEnvFile = async (path: string) => {
  if (!existsSync(path)) {
    return;
  }
  const text = await readFile(path, "utf8");
  for (const line of text.split(/\r?\n/u)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const delimiterIndex = trimmed.indexOf("=");
    if (delimiterIndex <= 0) {
      continue;
    }
    const key = trimmed.slice(0, delimiterIndex).trim();
    const rawValue = trimmed.slice(delimiterIndex + 1).trim();
    if (process.env[key] !== undefined) {
      continue;
    }
    process.env[key] = rawValue.replace(/^["']|["']$/gu, "");
  }
};

export const loadEnv = async () => {
  await loadEnvFile(resolve(ROOT_DIR, ".env"));
  await loadEnvFile(resolve(APP_DIR, ".env"));
  await loadEnvFile(resolve(APP_DIR, ".env.local"));
};

export const getConnectionString = (target: CliOptions["target"]): string => {
  const connectionString =
    target === "neon"
      ? process.env.DATABASE_URL_NEON
      : (process.env.DATABASE_URL_LOCAL ?? process.env.DATABASE_URL);
  if (!connectionString) {
    throw new Error(
      target === "neon"
        ? "DATABASE_URL_NEON is required."
        : "DATABASE_URL_LOCAL or DATABASE_URL is required.",
    );
  }
  return connectionString;
};

const toRaceDetail = (row: RaceRow): RaceDetail => ({
  babajotaiCodeDirt: row.babajotai_code_dirt,
  babajotaiCodeShiba: row.babajotai_code_shiba,
  gradeCode: row.grade_code,
  hassoJikoku: row.hasso_jikoku,
  juryoShubetsuCode: row.juryo_shubetsu_code,
  kaisaiKai: row.kaisai_kai,
  kaisaiNen: row.kaisai_nen,
  kaisaiNichime: row.kaisai_nichime,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  kyori: row.kyori,
  kyosoJokenCode: row.kyoso_joken_code,
  kyosoJokenMeisho: row.kyoso_joken_meisho,
  kyosoKigoCode: row.kyoso_kigo_code,
  kyosoShubetsuCode: row.kyoso_shubetsu_code,
  kyosomeiFukudai: row.kyosomei_fukudai,
  kyosomeiHondai: row.kyosomei_hondai,
  kyosomeiKakkonai: row.kyosomei_kakkonai,
  raceBango: row.race_bango,
  source: row.source,
  shussoTosu: row.shusso_tosu,
  tenkoCode: row.tenko_code,
  torokuTosu: row.toroku_tosu,
  trackCode: row.track_code,
});

const toRunner = (row: RunnerRow): Runner => ({
  banushimei: row.banushimei,
  barei: row.barei,
  bamei: row.bamei,
  bataiju: row.bataiju,
  chokyoshimeiRyakusho: row.chokyoshimei_ryakusho,
  corner1: row.corner_1,
  corner2: row.corner_2,
  corner3: row.corner_3,
  corner4: row.corner_4,
  damSireName: null,
  futanJuryo: row.futan_juryo,
  kakuteiChakujun: row.kakutei_chakujun,
  kettoTorokuBango: row.ketto_toroku_bango,
  kishumeiRyakusho: row.kishumei_ryakusho,
  kohan3f: row.kohan_3f,
  seibetsuCode: row.seibetsu_code,
  sireName: null,
  sireSireName: null,
  sohaTime: row.soha_time,
  tanshoNinkijun: row.tansho_ninkijun,
  tanshoOdds: row.tansho_odds,
  timeSa: row.time_sa,
  umaban: row.umaban,
  wakuban: row.wakuban,
  zogenFugo: row.zogen_fugo,
  zogenSa: row.zogen_sa,
});

const toHorseRaceResult = (row: HistoryRow): HorseRaceResult => ({
  babajotaiCodeDirt: row.history_babajotai_code_dirt,
  babajotaiCodeShiba: row.history_babajotai_code_shiba,
  banushimei: row.banushimei,
  barei: row.barei,
  bamei: row.bamei,
  bataiju: row.bataiju,
  chokyoshimeiRyakusho: row.chokyoshimei_ryakusho,
  corner1: row.corner_1,
  corner2: row.corner_2,
  corner3: row.corner_3,
  corner4: row.corner_4,
  currentBarei: row.current_barei,
  currentJockey: row.current_jockey,
  currentSeibetsuCode: row.current_seibetsu_code,
  currentUmaban: row.current_umaban,
  futanJuryo: row.futan_juryo,
  gradeCode: row.history_grade_code,
  hassoJikoku: row.history_hasso_jikoku,
  juryoShubetsuCode: row.history_juryo_shubetsu_code,
  kakuteiChakujun: row.kakutei_chakujun,
  kaisaiNen: row.history_kaisai_nen,
  kaisaiTsukihi: row.history_kaisai_tsukihi,
  keibajoCode: row.history_keibajo_code,
  kettoTorokuBango: row.ketto_toroku_bango,
  kishumeiRyakusho: row.kishumei_ryakusho,
  kohan3f: row.kohan_3f,
  kyori: row.history_kyori,
  kyosoJokenCode: row.history_kyoso_joken_code,
  kyosoJokenMeisho: row.history_kyoso_joken_meisho,
  kyosoKigoCode: row.history_kyoso_kigo_code,
  kyosoShubetsuCode: row.history_kyoso_shubetsu_code,
  kyosomeiFukudai: row.history_kyosomei_fukudai,
  kyosomeiHondai: row.history_kyosomei_hondai,
  kyosomeiKakkonai: row.history_kyosomei_kakkonai,
  raceBango: row.history_race_bango,
  seibetsuCode: row.seibetsu_code,
  shussoTosu: row.history_shusso_tosu,
  sohaTime: row.soha_time,
  tanshoNinkijun: row.tansho_ninkijun,
  tanshoOdds: row.tansho_odds,
  tenkoCode: row.history_tenko_code,
  timeSa: row.time_sa,
  trackCode: row.history_track_code,
  umaban: row.umaban,
  wakuban: row.wakuban,
  zogenFugo: row.zogen_fugo,
  zogenSa: row.zogen_sa,
});

export const getMonths = (
  fromDate: string,
  toDate: string,
): Array<{ month: string; year: string }> => {
  const startDate = parseDate(fromDate);
  const endDate = parseDate(toDate);
  const months: Array<{ month: string; year: string }> = [];
  const monthCount =
    (endDate.getFullYear() - startDate.getFullYear()) * 12 +
    (endDate.getMonth() - startDate.getMonth()) +
    1;
  for (let offset = 0; offset < monthCount; offset += 1) {
    const current = new Date(startDate.getFullYear(), startDate.getMonth() + offset, 1);
    months.push({ month: pad2(current.getMonth() + 1), year: String(current.getFullYear()) });
  }
  return months;
};

const raceDateValue = (race: Pick<RaceDetail, "kaisaiNen" | "kaisaiTsukihi">): string =>
  `${race.kaisaiNen}${race.kaisaiTsukihi}`;

const getRaceName = (race: RaceDetail): string =>
  [race.kyosomeiHondai, race.kyosomeiFukudai, race.kyosomeiKakkonai]
    .map((value) => value?.trim())
    .filter(Boolean)
    .join(" ")
    .trim() || "一般競走";

const parseCorner = (value: string | null | undefined): number | null => {
  const cleaned = value?.trim() ?? "";
  if (!cleaned || cleaned === "00") {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const parseNumber = (value: string | null | undefined, emptyValue: string): number | null => {
  const cleaned = value?.trim() ?? "";
  if (!cleaned || cleaned === emptyValue) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildSimilarityVector = (race: RaceDetail, runner: Runner, runnerCount: number): string => {
  const distance = parseNumber(race.kyori, "") ?? 0;
  const horseNumber = parseNumber(runner.umaban, "") ?? 0;
  const popularity = parseNumber(runner.tanshoNinkijun, "00") ?? runnerCount;
  const odds = (parseNumber(runner.tanshoOdds, "0000") ?? 10) / 10;
  const trackCode = race.trackCode?.trim() ?? "";
  const venue = parseNumber(race.keibajoCode, "") ?? 0;
  const raceNumber = parseNumber(race.raceBango, "") ?? 0;
  const values = [
    Math.min(1, Math.max(0, distance / 3600)),
    Math.min(1, Math.max(0, runnerCount / 18)),
    Math.min(1, Math.max(0, horseNumber / Math.max(runnerCount, 1))),
    Math.min(1, Math.max(0, popularity / Math.max(runnerCount, 1))),
    Math.min(1, Math.max(0, Math.log(Math.max(odds, 1)) / Math.log(300))),
    trackCode.startsWith("1") ? 0 : 1,
    Math.min(1, Math.max(0, venue / 99)),
    Math.min(1, Math.max(0, raceNumber / 12)),
  ];
  return `[${values.map((value) => value.toFixed(6)).join(",")}]`;
};

const fetchSimilarityFeatures = async (
  pool: Pool,
  race: RaceDetail,
  runners: Runner[],
): Promise<RacePaceSimilarityFeature[]> => {
  const runnerCount = runners.length;
  if (runnerCount <= 1) {
    return [];
  }
  try {
    const features = await Promise.all(
      runners.map(async (runner): Promise<RacePaceSimilarityFeature | null> => {
        const horseNumber = runner.umaban?.replace(/^0+/u, "") || runner.umaban || "";
        if (!horseNumber) {
          return null;
        }
        const distance = parseNumber(race.kyori, "");
        const vector = buildSimilarityVector(race, runner, runnerCount);
        const result = await pool.query<{
          corner1: string | null;
          corner2: string | null;
          corner3: string | null;
          corner4: string | null;
          neighbor_count: string;
          similarity_score: string | null;
        }>(
          `
            with nearest as (
              select *
              from (
                select
                  corner1_norm,
                  corner2_norm,
                  corner3_norm,
                  corner4_norm,
                  feature_vector
                from race_entry_corner_features
                where
                  source = $2
                  and race_date < $3
                  and ($4::integer is null or kyori between $4::integer - 400 and $4::integer + 400)
                  and left(coalesce(track_code, ''), 1) = left(coalesce($5, ''), 1)
                  and keibajo_code = $6
                  and race_date >= $7
                order by race_date desc
                limit 2500
              ) candidates
              order by feature_vector <-> $1::vector
              limit 40
            ),
            weighted_nearest as (
              select
                corner1_norm,
                corner2_norm,
                corner3_norm,
                corner4_norm,
                1 / (1 + (feature_vector <-> $1::vector)) weight
              from nearest
            )
            select
              sum(corner1_norm * weight) / nullif(sum(weight), 0) corner1,
              sum(corner2_norm * weight) / nullif(sum(weight), 0) corner2,
              sum(corner3_norm * weight) / nullif(sum(weight), 0) corner3,
              sum(corner4_norm * weight) / nullif(sum(weight), 0) corner4,
              count(*)::text neighbor_count,
              avg(weight)::text similarity_score
            from weighted_nearest
          `,
          [
            vector,
            race.source,
            raceDateValue(race),
            distance,
            race.trackCode,
            race.keibajoCode,
            `${Number(race.kaisaiNen) - 3}${race.kaisaiTsukihi}`,
          ],
        );
        const row = result.rows[0];
        const scaleCorner = (value: string | null): number | null => {
          if (value === null) {
            return null;
          }
          const parsed = Number(value);
          return Number.isFinite(parsed) ? parsed * (runnerCount - 1) + 1 : null;
        };
        const neighborCount = Number(row?.neighbor_count ?? 0);
        if (!row || neighborCount === 0) {
          return null;
        }
        return {
          corner1: scaleCorner(row.corner1),
          corner2: scaleCorner(row.corner2),
          corner3: scaleCorner(row.corner3),
          corner4: scaleCorner(row.corner4),
          horseNumber,
          neighborCount,
          similarityScore: Number(row.similarity_score ?? 0),
        };
      }),
    );
    return features.filter((feature): feature is RacePaceSimilarityFeature => feature !== null);
  } catch {
    return [];
  }
};

const getActualCornerOrder = (runners: Runner[], cornerKey: CornerKey): string[] =>
  runners
    .map((runner) => ({
      corner: parseCorner(runner[cornerKey]),
      horseNumber: runner.umaban?.replace(/^0+/u, "") || runner.umaban || "",
    }))
    .filter((item) => item.corner !== null && item.horseNumber)
    .toSorted((left, right) => {
      const leftCorner = left.corner ?? Number.POSITIVE_INFINITY;
      const rightCorner = right.corner ?? Number.POSITIVE_INFINITY;
      return leftCorner - rightCorner || Number(left.horseNumber) - Number(right.horseNumber);
    })
    .map((item) => item.horseNumber);

const getPredictedCornerOrder = (
  rows: RacePacePredictionRow[],
  cornerKey: CornerKey,
  actualHorseNumbers: Set<string>,
): string[] =>
  rows
    .filter((row) => row[cornerKey] !== null && actualHorseNumbers.has(row.horseNumber))
    .toSorted(
      (left, right) =>
        (left[cornerKey] ?? Number.POSITIVE_INFINITY) -
          (right[cornerKey] ?? Number.POSITIVE_INFINITY) ||
        Number(left.horseNumber) - Number(right.horseNumber),
    )
    .map((row) => row.horseNumber);

const compareCorner = (
  actualOrder: string[],
  predictedOrder: string[],
): CornerComparison | null => {
  const predictedPositions = new Map(
    predictedOrder.map((horseNumber, index) => [horseNumber, index + 1]),
  );
  const comparedActualOrder = actualOrder.filter((horseNumber) =>
    predictedPositions.has(horseNumber),
  );
  if (comparedActualOrder.length === 0) {
    return null;
  }

  let exactMatches = 0;
  let errorTotal = 0;
  comparedActualOrder.forEach((horseNumber, index) => {
    const actualPosition = index + 1;
    const predictedPosition = predictedPositions.get(horseNumber) ?? actualPosition;
    if (actualPosition === predictedPosition) {
      exactMatches += 1;
    }
    errorTotal += Math.abs(actualPosition - predictedPosition);
  });

  const maxError = Math.max(1, comparedActualOrder.length - 1);
  const meanAbsolutePositionError = errorTotal / comparedActualOrder.length;
  return {
    actualOrder: comparedActualOrder,
    exactMatches,
    meanAbsolutePositionError,
    predictedOrder: predictedOrder.filter((horseNumber) => actualOrder.includes(horseNumber)),
    score: Math.max(0, 1 - meanAbsolutePositionError / maxError),
  };
};

const getRaceTableNames = (source: RaceSource): { raceTable: string; runnerTable: string } =>
  source === "jra"
    ? { raceTable: "jvd_ra", runnerTable: "jvd_se" }
    : { raceTable: "nvd_ra", runnerTable: "nvd_se" };

const fetchMonthRaces = async (
  pool: Pool,
  year: string,
  month: string,
  fromDate: string,
  toDate: string,
  sourceScope: RaceSource | "all",
): Promise<RaceDetail[]> => {
  const sources: RaceSource[] =
    sourceScope === "all" ? ["jra", "nar"] : sourceScope === "jra" ? ["jra"] : ["nar"];
  const races = await Promise.all(
    sources.map(async (source) => {
      const { raceTable } = getRaceTableNames(source);
      const result = await pool.query<RaceRow>(
        `
          select
            $1::text source,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            kyosomei_hondai,
            kyosomei_fukudai,
            kyosomei_kakkonai,
            grade_code,
            kyoso_shubetsu_code,
            kyoso_kigo_code,
            juryo_shubetsu_code,
            kyoso_joken_code,
            kyoso_joken_meisho,
            kyori,
            track_code,
            hasso_jikoku,
            toroku_tosu,
            shusso_tosu,
            tenko_code,
            babajotai_code_shiba,
            babajotai_code_dirt,
            kaisai_kai,
            kaisai_nichime
          from ${raceTable}
          where
            kaisai_nen = $2
            and substring(kaisai_tsukihi from 1 for 2) = $3
            and kaisai_nen || kaisai_tsukihi between $4 and $5
          order by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
        `,
        [source, year, month, fromDate, toDate],
      );
      return result.rows.map(toRaceDetail);
    }),
  );
  return races.flat().toSorted((left, right) => {
    const leftKey = `${left.kaisaiNen}${left.kaisaiTsukihi}${left.keibajoCode}${left.raceBango}${left.source}`;
    const rightKey = `${right.kaisaiNen}${right.kaisaiTsukihi}${right.keibajoCode}${right.raceBango}${right.source}`;
    return leftKey.localeCompare(rightKey);
  });
};

const fetchRunners = async (pool: Pool, race: RaceDetail): Promise<Runner[]> => {
  const { runnerTable } = getRaceTableNames(race.source);
  const result = await pool.query<RunnerRow>(
    `
      select
        wakuban,
        umaban,
        ketto_toroku_bango,
        bamei,
        seibetsu_code,
        barei,
        futan_juryo,
        kishumei_ryakusho,
        chokyoshimei_ryakusho,
        banushimei,
        bataiju,
        zogen_fugo,
        zogen_sa,
        kakutei_chakujun,
        tansho_odds,
        tansho_ninkijun,
        soha_time,
        time_sa,
        corner_1,
        corner_2,
        corner_3,
        corner_4,
        kohan_3f
      from ${runnerTable}
      where
        kaisai_nen = $1
        and kaisai_tsukihi = $2
        and keibajo_code = $3
        and race_bango = $4
      order by umaban::int
    `,
    [race.kaisaiNen, race.kaisaiTsukihi, race.keibajoCode, race.raceBango],
  );
  return result.rows.map(toRunner);
};

const fetchHistory = async (
  pool: Pool,
  race: RaceDetail,
  sourceScope: RaceSource | "all",
): Promise<HorseRaceResult[]> => {
  const { runnerTable } = getRaceTableNames(race.source);
  const raceDate = raceDateValue(race);
  const includeJraHistory = sourceScope === "all" || sourceScope === "jra";
  const includeNarHistory = sourceScope === "all" || sourceScope === "nar";
  const result = await pool.query<HistoryRow>(
    `
      with current_horses as (
        select
          umaban current_umaban,
          ketto_toroku_bango,
          seibetsu_code current_seibetsu_code,
          barei current_barei,
          coalesce(nullif(btrim(kishumei_ryakusho), ''), '不明') current_jockey
        from ${runnerTable}
        where
          kaisai_nen = $1
          and kaisai_tsukihi = $2
          and keibajo_code = $3
          and race_bango = $4
          and ketto_toroku_bango is not null
          and btrim(ketto_toroku_bango) <> ''
      ),
      history as (
        select
          ch.current_jockey,
          ch.current_barei,
          ch.current_seibetsu_code,
          ch.current_umaban,
          past.history_kaisai_nen,
          past.history_kaisai_tsukihi,
          past.history_keibajo_code,
          past.history_race_bango,
          past.history_kyosomei_hondai,
          past.history_kyosomei_fukudai,
          past.history_kyosomei_kakkonai,
          past.history_grade_code,
          past.history_kyoso_shubetsu_code,
          past.history_kyoso_kigo_code,
          past.history_juryo_shubetsu_code,
          past.history_kyoso_joken_code,
          past.history_kyoso_joken_meisho,
          past.history_kyori,
          past.history_track_code,
          past.history_hasso_jikoku,
          past.history_shusso_tosu,
          past.history_tenko_code,
          past.history_babajotai_code_shiba,
          past.history_babajotai_code_dirt,
          past.wakuban,
          past.umaban,
          past.ketto_toroku_bango,
          past.bamei,
          past.seibetsu_code,
          past.barei,
          past.futan_juryo,
          past.kishumei_ryakusho,
          past.chokyoshimei_ryakusho,
          past.banushimei,
          past.bataiju,
          past.zogen_fugo,
          past.zogen_sa,
          past.kakutei_chakujun,
          past.tansho_odds,
          past.tansho_ninkijun,
          past.soha_time,
          past.time_sa,
          past.corner_1,
          past.corner_2,
          past.corner_3,
          past.corner_4,
          past.kohan_3f,
          row_number() over (
            partition by ch.current_umaban, past.history_kaisai_nen, past.history_kaisai_tsukihi, past.history_keibajo_code, past.history_race_bango
            order by past.history_kaisai_nen desc, past.history_kaisai_tsukihi desc, past.history_race_bango desc
          ) rn
        from current_horses ch
        join (
          select
            ra.kaisai_nen || ra.kaisai_tsukihi race_date,
            ra.kaisai_nen history_kaisai_nen,
            ra.kaisai_tsukihi history_kaisai_tsukihi,
            ra.keibajo_code history_keibajo_code,
            ra.race_bango history_race_bango,
            ra.kyosomei_hondai history_kyosomei_hondai,
            ra.kyosomei_fukudai history_kyosomei_fukudai,
            ra.kyosomei_kakkonai history_kyosomei_kakkonai,
            ra.grade_code history_grade_code,
            ra.kyoso_shubetsu_code history_kyoso_shubetsu_code,
            ra.kyoso_kigo_code history_kyoso_kigo_code,
            ra.juryo_shubetsu_code history_juryo_shubetsu_code,
            ra.kyoso_joken_code history_kyoso_joken_code,
            ra.kyoso_joken_meisho history_kyoso_joken_meisho,
            ra.kyori history_kyori,
            ra.track_code history_track_code,
            ra.hasso_jikoku history_hasso_jikoku,
            ra.shusso_tosu history_shusso_tosu,
            ra.tenko_code history_tenko_code,
            ra.babajotai_code_shiba history_babajotai_code_shiba,
            ra.babajotai_code_dirt history_babajotai_code_dirt,
            se.wakuban,
            se.umaban,
            se.ketto_toroku_bango,
            se.bamei,
            se.seibetsu_code,
            se.barei,
            se.futan_juryo,
            se.kishumei_ryakusho,
            se.chokyoshimei_ryakusho,
            se.banushimei,
            se.bataiju,
            se.zogen_fugo,
            se.zogen_sa,
            se.kakutei_chakujun,
            se.tansho_odds,
            se.tansho_ninkijun,
            se.soha_time,
            se.time_sa,
            se.corner_1,
            se.corner_2,
            se.corner_3,
            se.corner_4,
            se.kohan_3f
          from jvd_se se
          join jvd_ra ra
            on ra.kaisai_nen = se.kaisai_nen
            and ra.kaisai_tsukihi = se.kaisai_tsukihi
            and ra.keibajo_code = se.keibajo_code
            and ra.race_bango = se.race_bango
          where
            $6 = true
            and se.ketto_toroku_bango in (select ketto_toroku_bango from current_horses)
          union all
          select
            ra.kaisai_nen || ra.kaisai_tsukihi race_date,
            ra.kaisai_nen history_kaisai_nen,
            ra.kaisai_tsukihi history_kaisai_tsukihi,
            ra.keibajo_code history_keibajo_code,
            ra.race_bango history_race_bango,
            ra.kyosomei_hondai history_kyosomei_hondai,
            ra.kyosomei_fukudai history_kyosomei_fukudai,
            ra.kyosomei_kakkonai history_kyosomei_kakkonai,
            ra.grade_code history_grade_code,
            ra.kyoso_shubetsu_code history_kyoso_shubetsu_code,
            ra.kyoso_kigo_code history_kyoso_kigo_code,
            ra.juryo_shubetsu_code history_juryo_shubetsu_code,
            ra.kyoso_joken_code history_kyoso_joken_code,
            ra.kyoso_joken_meisho history_kyoso_joken_meisho,
            ra.kyori history_kyori,
            ra.track_code history_track_code,
            ra.hasso_jikoku history_hasso_jikoku,
            ra.shusso_tosu history_shusso_tosu,
            ra.tenko_code history_tenko_code,
            ra.babajotai_code_shiba history_babajotai_code_shiba,
            ra.babajotai_code_dirt history_babajotai_code_dirt,
            se.wakuban,
            se.umaban,
            se.ketto_toroku_bango,
            se.bamei,
            se.seibetsu_code,
            se.barei,
            se.futan_juryo,
            se.kishumei_ryakusho,
            se.chokyoshimei_ryakusho,
            se.banushimei,
            se.bataiju,
            se.zogen_fugo,
            se.zogen_sa,
            se.kakutei_chakujun,
            se.tansho_odds,
            se.tansho_ninkijun,
            se.soha_time,
            se.time_sa,
            se.corner_1,
            se.corner_2,
            se.corner_3,
            se.corner_4,
            se.kohan_3f
          from nvd_se se
          join nvd_ra ra
            on ra.kaisai_nen = se.kaisai_nen
            and ra.kaisai_tsukihi = se.kaisai_tsukihi
            and ra.keibajo_code = se.keibajo_code
            and ra.race_bango = se.race_bango
          where
            $7 = true
            and se.ketto_toroku_bango in (select ketto_toroku_bango from current_horses)
        ) past
          on past.ketto_toroku_bango = ch.ketto_toroku_bango
        where past.race_date < $5
      )
      select *
      from history
      where rn = 1
      order by current_umaban::int, history_kaisai_nen desc, history_kaisai_tsukihi desc, history_race_bango desc
    `,
    [
      race.kaisaiNen,
      race.kaisaiTsukihi,
      race.keibajoCode,
      race.raceBango,
      raceDate,
      includeJraHistory,
      includeNarHistory,
    ],
  );
  return result.rows.map(toHorseRaceResult);
};

export const compareRace = async (
  pool: Pool,
  race: RaceDetail,
  sourceScope: RaceSource | "all",
  model?: RacePacePredictionModel,
): Promise<RaceComparison | null> => {
  if (
    !isCornerPacePredictionSupported({
      distance: race.kyori,
      keibajoCode: race.keibajoCode,
      source: race.source,
    })
  ) {
    return null;
  }
  const runners = await fetchRunners(pool, race);
  if (
    !runners.some((runner) =>
      CORNER_KEYS.some((cornerKey) => parseCorner(runner[cornerKey]) !== null),
    )
  ) {
    return null;
  }
  const history = await fetchHistory(pool, race, sourceScope);
  const similarityFeatures = await fetchSimilarityFeatures(pool, race, runners);
  const predictions = buildRacePacePredictionRowsFromResults({
    currentConditionCode: race.kyosoJokenCode,
    currentConditionName: race.kyosoJokenMeisho,
    currentDistance: race.kyori,
    currentGradeCode: race.gradeCode,
    currentRaceAgeCode: race.kyosoShubetsuCode,
    currentRaceDate: raceDateValue(race),
    currentSource: race.source,
    currentTrackCode: race.trackCode,
    model,
    results: history,
    runners,
    similarityFeatures,
  });

  const corners: Partial<Record<CornerKey, CornerComparison>> = {};
  for (const cornerKey of CORNER_KEYS) {
    const actualOrder = getActualCornerOrder(runners, cornerKey);
    if (actualOrder.length === 0) {
      continue;
    }
    const predictedOrder = getPredictedCornerOrder(predictions, cornerKey, new Set(actualOrder));
    const comparison = compareCorner(actualOrder, predictedOrder);
    if (comparison) {
      corners[cornerKey] = comparison;
    }
  }

  if (Object.keys(corners).length === 0) {
    return null;
  }

  return {
    corners,
    date: raceDateValue(race),
    keibajoCode: race.keibajoCode,
    raceName: getRaceName(race),
    raceNumber: race.raceBango,
    runnerCount: runners.length,
    source: race.source,
  };
};

const summarizeRaceComparisons = (
  year: string,
  month: string,
  comparisons: RaceComparison[],
  skippedRaces: number,
): MonthSummary => {
  let scoreTotal = 0;
  let scoreCount = 0;
  let exactMatches = 0;
  let runnerPositions = 0;
  for (const comparison of comparisons) {
    for (const corner of Object.values(comparison.corners)) {
      if (!corner) {
        continue;
      }
      exactMatches += corner.exactMatches;
      runnerPositions += corner.actualOrder.length;
      if (corner.score !== null) {
        scoreTotal += corner.score;
        scoreCount += 1;
      }
    }
  }
  return {
    averageScore: scoreCount > 0 ? scoreTotal / scoreCount : null,
    comparedCorners: scoreCount,
    comparedRaces: comparisons.length,
    exactMatches,
    month,
    runnerPositions,
    skippedRaces,
    year,
  };
};

export const compareMonth = async (
  pool: Pool,
  year: string,
  month: string,
  options: CliOptions,
): Promise<{ comparisons: RaceComparison[]; summary: MonthSummary }> => {
  const races = await fetchMonthRaces(
    pool,
    year,
    month,
    options.fromDate,
    options.toDate,
    options.sourceScope,
  );
  const comparisons: RaceComparison[] = [];
  let skippedRaces = 0;
  const comparisonResults = await runInBatches(races, 2, (race) =>
    compareRace(pool, race, options.sourceScope, options.model),
  );
  for (const comparison of comparisonResults) {
    if (comparison) {
      comparisons.push(comparison);
    } else {
      skippedRaces += 1;
    }
  }
  return {
    comparisons,
    summary: summarizeRaceComparisons(year, month, comparisons, skippedRaces),
  };
};

export const runInBatches = async <T, R>(
  items: T[],
  concurrency: number,
  task: (item: T) => Promise<R>,
): Promise<R[]> => {
  const results: R[] = [];
  let nextIndex = 0;
  const runNext = async (): Promise<void> => {
    if (nextIndex >= items.length) {
      return;
    }
    const currentIndex = nextIndex;
    nextIndex += 1;
    const item = items[currentIndex];
    if (item === undefined) {
      return;
    }
    results[currentIndex] = await task(item);
    await runNext();
  };
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    await runNext();
  });
  await Promise.all(workers);
  return results;
};

export const getAggregateSummary = (
  monthSummaries: MonthSummary[],
  races: RaceComparison[],
): AggregateSummary => {
  const scoreRows = monthSummaries.filter((summary) => summary.averageScore !== null);
  const scoreTotal = scoreRows.reduce((total, summary) => total + (summary.averageScore ?? 0), 0);
  return {
    averageScore: scoreRows.length > 0 ? scoreTotal / scoreRows.length : null,
    comparedCorners: monthSummaries.reduce((total, summary) => total + summary.comparedCorners, 0),
    comparedRaces: monthSummaries.reduce((total, summary) => total + summary.comparedRaces, 0),
    exactMatches: monthSummaries.reduce((total, summary) => total + summary.exactMatches, 0),
    monthSummaries,
    races,
    runnerPositions: monthSummaries.reduce((total, summary) => total + summary.runnerPositions, 0),
    skippedRaces: monthSummaries.reduce((total, summary) => total + summary.skippedRaces, 0),
  };
};

const formatPercent = (value: number | null): string =>
  value === null ? "-" : `${(value * 100).toFixed(1)}%`;

const getRaceAverageScore = (race: RaceComparison): number | null => {
  const scores = Object.values(race.corners)
    .map((corner) => corner?.score ?? null)
    .filter((score): score is number => score !== null);
  return scores.length > 0
    ? scores.reduce((total, score) => total + score, 0) / scores.length
    : null;
};

const formatCornerComparison = (race: RaceComparison, cornerKey: CornerKey): string => {
  const corner = race.corners[cornerKey];
  if (!corner) {
    return "-";
  }
  return `${cornerKey}:p=${corner.predictedOrder.join("-") || "-"} a=${corner.actualOrder.join("-") || "-"} s=${formatPercent(corner.score)}`;
};

const printTextResult = (summary: AggregateSummary) => {
  console.log("corner prediction comparison");
  console.log(
    `races=${summary.comparedRaces} skipped=${summary.skippedRaces} corners=${summary.comparedCorners} score=${formatPercent(summary.averageScore)}`,
  );
  console.log("year-month,races,skipped,corners,exact/positions,score");
  for (const month of summary.monthSummaries) {
    console.log(
      `${month.year}-${month.month},${month.comparedRaces},${month.skippedRaces},${month.comparedCorners},${month.exactMatches}/${month.runnerPositions},${formatPercent(month.averageScore)}`,
    );
  }
  console.log("date,source,keibajo,race,score,corners");
  for (const race of summary.races) {
    console.log(
      [
        race.date,
        race.source,
        race.keibajoCode,
        race.raceNumber,
        formatPercent(getRaceAverageScore(race)),
        CORNER_KEYS.map((cornerKey) => formatCornerComparison(race, cornerKey)).join(" | "),
        race.raceName,
      ].join(","),
    );
  }
};

const main = async () => {
  await loadEnv();
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool({
    connectionString: getConnectionString(options.target),
    max: options.concurrency + 2,
  });
  try {
    const months = getMonths(options.fromDate, options.toDate);
    const monthResults = await runInBatches(months, options.concurrency, (month) =>
      compareMonth(pool, month.year, month.month, options),
    );
    const races = monthResults.flatMap((result) => result.comparisons);
    const summary = getAggregateSummary(
      monthResults.map((result) => result.summary),
      races,
    );
    if (options.output === "json") {
      console.log(
        JSON.stringify(
          {
            options,
            summary,
          },
          null,
          2,
        ),
      );
    } else {
      printTextResult(summary);
    }
  } finally {
    await pool.end();
  }
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
