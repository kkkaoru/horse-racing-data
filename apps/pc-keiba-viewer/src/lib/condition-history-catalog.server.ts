// Run with bun.
import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import type {
  ConditionCorrelationDetail,
  ConditionCorrelationRow,
  FinishPositionStatsRow,
  FrameStatsRow,
  RaceTimeStats,
  RaceTimeTargetRace,
  StatsDetail,
  WeightClassStatsRow,
} from "./race-types";

export interface ConditionHistoryCatalogQuery {
  day: string;
  includeAge?: boolean;
  includeClass?: boolean;
  includeConditionKey?: boolean;
  includeDistance: boolean;
  includeGrade?: boolean;
  includeRaceTitle?: boolean;
  includeSurface: boolean;
  includeTrackCode?: boolean;
  includeTurn: boolean;
  includeVenue: boolean;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: "jra" | "nar";
  year: string;
  years: number;
}

export interface ConditionHistoryCatalogStats {
  carriedWeightClassStats: WeightClassStatsRow[];
  finishPositionStats: FinishPositionStatsRow[];
  frameStats: FrameStatsRow[];
  raceTimeStats: RaceTimeStats;
  weightClassStats: WeightClassStatsRow[];
}

const CATALOG_ORIGIN: string = "https://pc-keiba-r2-catalog.internal";
const CORRELATION_DETAIL_KEYS: ReadonlyArray<ConditionCorrelationDetail["key"]> = [
  "horseShow",
  "horseWin",
  "jockeyShow",
  "odds",
  "ownerShow",
  "popularity",
  "trainerShow",
];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const flagParam = (value: boolean): string => (value ? "1" : "0");

const requiredString = (value: unknown): string | null => {
  if (typeof value === "string" && value.length > 0) return value;
  if (typeof value === "number" || typeof value === "bigint") {
    const text = String(value);
    return text.length > 0 ? text : null;
  }
  return null;
};

const stringOrEmpty = (value: unknown): string => requiredString(value) ?? "";

const requiredNumber = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const nullableNumber = (value: unknown): number | null | undefined => {
  if (value === null) return null;
  const parsed = requiredNumber(value);
  return parsed === null ? undefined : parsed;
};

const isCorrelationDetailKey = (value: string): value is ConditionCorrelationDetail["key"] =>
  CORRELATION_DETAIL_KEYS.some((key) => key === value);

const parseWeightClassStatsRow = (value: unknown): WeightClassStatsRow | null => {
  if (!isRecord(value)) return null;
  const key = requiredString(value.key);
  const quinellaCount = requiredNumber(value.quinellaCount);
  const quinellaRate = nullableNumber(value.quinellaRate);
  const showCount = requiredNumber(value.showCount);
  const showRate = nullableNumber(value.showRate);
  const starts = requiredNumber(value.starts);
  const winCount = requiredNumber(value.winCount);
  const winRate = nullableNumber(value.winRate);
  if (
    key === null ||
    quinellaCount === null ||
    quinellaRate === undefined ||
    showCount === null ||
    showRate === undefined ||
    starts === null ||
    winCount === null ||
    winRate === undefined
  ) {
    return null;
  }
  return {
    key,
    quinellaCount,
    quinellaRate,
    showCount,
    showRate,
    starts,
    winCount,
    winRate,
  };
};

const parseFrameStatsRow = (value: unknown): FrameStatsRow | null => {
  if (!isRecord(value)) return null;
  if (value.details !== undefined && !Array.isArray(value.details)) return null;
  const frameNumber = requiredString(value.frameNumber);
  const runnerCount = nullableNumber(value.runnerCount);
  const count = requiredNumber(value.count);
  const score = requiredNumber(value.score);
  const averageFinish = nullableNumber(value.averageFinish);
  const medianFinish = nullableNumber(value.medianFinish);
  const averagePopularity = nullableNumber(value.averagePopularity);
  const medianPopularity = nullableNumber(value.medianPopularity);
  const quinellaCount = requiredNumber(value.quinellaCount);
  const quinellaRate = nullableNumber(value.quinellaRate);
  const showCount = requiredNumber(value.showCount);
  const showRate = nullableNumber(value.showRate);
  const winCount = requiredNumber(value.winCount);
  const winRate = nullableNumber(value.winRate);
  if (
    frameNumber === null ||
    runnerCount === undefined ||
    count === null ||
    score === null ||
    averageFinish === undefined ||
    medianFinish === undefined ||
    averagePopularity === undefined ||
    medianPopularity === undefined ||
    quinellaCount === null ||
    quinellaRate === undefined ||
    showCount === null ||
    showRate === undefined ||
    winCount === null ||
    winRate === undefined
  ) {
    return null;
  }
  return {
    averageFinish,
    averagePopularity,
    count,
    details: [],
    frameNumber,
    medianFinish,
    medianPopularity,
    quinellaCount,
    quinellaRate,
    runnerCount,
    score,
    showCount,
    showRate,
    winCount,
    winRate,
  };
};

const parseFinishPositionDetail = (value: unknown): StatsDetail | null => {
  if (!isRecord(value)) return null;
  const date = requiredString(value.date);
  const horseName = requiredString(value.horseName);
  const horseNumber = requiredString(value.horseNumber);
  const jockeyName = requiredString(value.jockeyName);
  const keibajoCode = requiredString(value.keibajoCode);
  const raceNumber = requiredString(value.raceNumber);
  if (
    date === null ||
    horseName === null ||
    horseNumber === null ||
    jockeyName === null ||
    keibajoCode === null ||
    raceNumber === null
  ) {
    return null;
  }
  return {
    date,
    frameNumber: stringOrEmpty(value.frameNumber),
    horseName,
    horseNumber,
    jockeyName,
    keibajoCode,
    popularity: stringOrEmpty(value.popularity),
    raceName: stringOrEmpty(value.raceName),
    raceNumber,
    raceTime: stringOrEmpty(value.raceTime),
    rank: stringOrEmpty(value.rank),
    winOdds: stringOrEmpty(value.winOdds),
  };
};

const parseFinishPositionStatsRow = (value: unknown): FinishPositionStatsRow | null => {
  if (!isRecord(value)) return null;
  if (value.details !== undefined && !Array.isArray(value.details)) return null;
  const finishPosition = requiredNumber(value.finishPosition);
  const count = requiredNumber(value.count);
  const averagePopularity = nullableNumber(value.averagePopularity);
  const medianPopularity = nullableNumber(value.medianPopularity);
  const averageOdds = nullableNumber(value.averageOdds);
  const medianOdds = nullableNumber(value.medianOdds);
  if (
    finishPosition === null ||
    count === null ||
    averagePopularity === undefined ||
    medianPopularity === undefined ||
    averageOdds === undefined ||
    medianOdds === undefined
  ) {
    return null;
  }
  const details =
    value.details === undefined
      ? []
      : value.details.flatMap((detail) => {
          const parsed = parseFinishPositionDetail(detail);
          return parsed === null ? [] : [parsed];
        });
  return {
    averageOdds,
    averagePopularity,
    count,
    details,
    finishPosition,
    medianOdds,
    medianPopularity,
  };
};

const parseCorrelationDetail = (value: unknown): ConditionCorrelationDetail | null => {
  if (!isRecord(value)) return null;
  const key = requiredString(value.key);
  if (key === null || !isCorrelationDetailKey(key)) return null;
  const label = requiredString(value.label);
  const reason = requiredString(value.reason);
  const score = requiredNumber(value.score);
  const target = nullableNumber(value.target);
  const detailValue = nullableNumber(value.value);
  const weight = requiredNumber(value.weight);
  if (
    label === null ||
    reason === null ||
    score === null ||
    target === undefined ||
    detailValue === undefined ||
    weight === null
  ) {
    return null;
  }
  return { key, label, reason, score, target, value: detailValue, weight };
};

const parseCorrelationRow = (value: unknown): ConditionCorrelationRow | null => {
  if (!isRecord(value)) return null;
  if (value.details !== undefined && !Array.isArray(value.details)) return null;
  const horseName = requiredString(value.horseName);
  const horseNumber = requiredString(value.horseNumber);
  const score = requiredNumber(value.score);
  if (horseName === null || horseNumber === null || score === null) return null;
  const details = Array.isArray(value.details) ? value.details.map(parseCorrelationDetail) : [];
  if (details.some((detail) => detail === null)) return null;
  return {
    details: details.filter((detail) => detail !== null),
    horseName,
    horseNumber,
    score,
  };
};

const parseRowList = <T>(value: unknown, parseRow: (row: unknown) => T | null): T[] | null => {
  if (!Array.isArray(value)) return null;
  const rows = value.map(parseRow);
  return rows.some((row) => row === null) ? null : rows.filter((row) => row !== null);
};

const stringField = (value: unknown): string | null => {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  return null;
};

const parseTargetRace = (value: unknown): RaceTimeTargetRace | null => {
  if (!isRecord(value)) return null;
  const date = requiredString(value.date);
  const horseName = requiredString(value.horseName);
  const horseNumber = requiredString(value.horseNumber);
  const jockeyName = requiredString(value.jockeyName);
  const keibajoCode = requiredString(value.keibajoCode);
  const kohan3f = stringField(value.kohan3f);
  const ownerName = requiredString(value.ownerName);
  const popularity = stringField(value.popularity);
  const raceName = stringField(value.raceName);
  const raceNumber = requiredString(value.raceNumber);
  const raceTime = stringField(value.raceTime);
  const trainerName = requiredString(value.trainerName);
  if (
    date === null ||
    horseName === null ||
    horseNumber === null ||
    jockeyName === null ||
    keibajoCode === null ||
    kohan3f === null ||
    ownerName === null ||
    popularity === null ||
    raceName === null ||
    raceNumber === null ||
    raceTime === null ||
    trainerName === null
  ) {
    return null;
  }
  return {
    date,
    horseName,
    horseNumber,
    jockeyName,
    keibajoCode,
    kohan3f,
    ownerName,
    popularity,
    raceName,
    raceNumber,
    raceTime,
    trainerName,
  };
};

const parseRaceTimeStats = (value: unknown): RaceTimeStats | null => {
  if (!isRecord(value)) return null;
  if (value.correlationRows !== undefined && !Array.isArray(value.correlationRows)) return null;
  if (value.targetRaces !== undefined && !Array.isArray(value.targetRaces)) return null;
  const raceCount = requiredNumber(value.raceCount);
  const fastestRaceTime = nullableNumber(value.fastestRaceTime);
  const fastestKohan3f = nullableNumber(value.fastestKohan3f);
  const averageRaceTime = nullableNumber(value.averageRaceTime);
  const averageKohan3f = nullableNumber(value.averageKohan3f);
  const medianRaceTime = nullableNumber(value.medianRaceTime);
  const medianKohan3f = nullableNumber(value.medianKohan3f);
  if (
    raceCount === null ||
    fastestRaceTime === undefined ||
    fastestKohan3f === undefined ||
    averageRaceTime === undefined ||
    averageKohan3f === undefined ||
    medianRaceTime === undefined ||
    medianKohan3f === undefined
  ) {
    return null;
  }
  const correlationRows = Array.isArray(value.correlationRows)
    ? value.correlationRows.map(parseCorrelationRow)
    : [];
  if (correlationRows.some((row) => row === null)) return null;
  const targetRaces =
    value.targetRaces === undefined ? [] : parseRowList(value.targetRaces, parseTargetRace);
  if (targetRaces === null) return null;
  return {
    averageKohan3f,
    averageRaceTime,
    correlationRows: correlationRows.filter((row) => row !== null),
    fastestDetail: null,
    fastestKohan3f,
    fastestRaceTime,
    medianKohan3f,
    medianRaceTime,
    raceCount,
    targetRaces,
  };
};

export const buildConditionHistoryCatalogUrl = (query: ConditionHistoryCatalogQuery): URL => {
  const url = new URL("/v1/condition-history-stats", CATALOG_ORIGIN);
  url.searchParams.set("year", query.year);
  url.searchParams.set("month", query.month.padStart(2, "0"));
  url.searchParams.set("day", query.day.padStart(2, "0"));
  url.searchParams.set("keibajoCode", query.keibajoCode.padStart(2, "0"));
  url.searchParams.set("raceNumber", query.raceNumber.padStart(2, "0"));
  url.searchParams.set("source", query.source);
  url.searchParams.set("years", String(query.years));
  url.searchParams.set("includeVenue", flagParam(query.includeVenue));
  url.searchParams.set("includeDistance", flagParam(query.includeDistance));
  url.searchParams.set("includeSurface", flagParam(query.includeSurface));
  url.searchParams.set("includeTurn", flagParam(query.includeTurn));
  if (query.includeGrade === true) {
    url.searchParams.set("includeGrade", "1");
  }
  if (query.includeTrackCode === true) {
    url.searchParams.set("includeTrackCode", "1");
  }
  if (query.includeAge === true) {
    url.searchParams.set("includeAge", "1");
  }
  if (query.includeClass === true) {
    url.searchParams.set("includeClass", "1");
  }
  if (query.includeConditionKey === true) {
    url.searchParams.set("includeConditionKey", "1");
  }
  if (query.includeRaceTitle === true) {
    url.searchParams.set("includeRaceTitle", "1");
  }
  return url;
};

export const fetchConditionHistoryStatsFromCatalog = async (
  query: ConditionHistoryCatalogQuery,
): Promise<ConditionHistoryCatalogStats | null> => {
  const env = await safeGetCloudflareEnv();
  const catalog = env?.R2_CATALOG;
  if (!catalog) return null;

  const response = await catalog.fetch(buildConditionHistoryCatalogUrl(query).href);
  if (!response.ok) {
    throw new Error(`R2 Catalog condition history stats failed: ${String(response.status)}`);
  }
  const payload: unknown = await response.json();
  if (!isRecord(payload)) {
    throw new Error("R2 Catalog condition history stats payload is malformed");
  }
  const frameStats = parseRowList(payload.frameStats, parseFrameStatsRow);
  const weightClassStats = parseRowList(payload.weightClassStats, parseWeightClassStatsRow);
  const carriedWeightClassStats = parseRowList(
    payload.carriedWeightClassStats,
    parseWeightClassStatsRow,
  );
  const finishPositionStats = parseRowList(
    payload.finishPositionStats,
    parseFinishPositionStatsRow,
  );
  const raceTimeStats = parseRaceTimeStats(payload.raceTimeStats);
  if (
    frameStats === null ||
    weightClassStats === null ||
    carriedWeightClassStats === null ||
    finishPositionStats === null ||
    raceTimeStats === null
  ) {
    throw new Error("R2 Catalog condition history stats rows are malformed");
  }
  return {
    carriedWeightClassStats,
    finishPositionStats,
    frameStats,
    raceTimeStats,
    weightClassStats,
  };
};
