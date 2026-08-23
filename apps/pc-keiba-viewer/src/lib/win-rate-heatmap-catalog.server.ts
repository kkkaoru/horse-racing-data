import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import type { BloodlineStatsRow, SimilarRaceStatsRow } from "./race-types";

export interface WinRateHeatmapCatalogQuery {
  day: string;
  includeAge?: boolean;
  includeClass?: boolean;
  includeConditionKey?: boolean;
  includeDistance: boolean;
  includeGrade?: boolean;
  includeJockeyFrame?: boolean;
  includeOwner?: boolean;
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

interface CatalogGroupableRateRow {
  category: string;
  currentHorseNumbers: string;
  details: BloodlineStatsRow["details"];
  horseCount: number;
  name: string;
}

export interface WinRateHeatmapCatalogStats {
  bloodlineRows: BloodlineStatsRow[];
  similarRows: SimilarRaceStatsRow[];
}

interface CatalogBloodlineRow {
  category: BloodlineStatsRow["category"];
  name: string;
  places: number;
  shows: number;
  starts: number;
  umaban: number;
  wins: number;
}

interface CatalogSimilarRow {
  kind: "jockey" | "jockeyFrame" | "owner" | "trainer";
  name: string;
  places: number;
  shows: number;
  starts: number;
  umaban: number;
  wins: number;
}

interface CatalogFetcher {
  fetch: (input: string) => Promise<Response>;
}

const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog.internal";
const CATALOG_NOT_FOUND_STATUS = 404;
const HORSE_NUMBER_JOINER: string = ", ";
const MAX_WIN_RATE = 100;
const RATE_DECIMAL_FACTOR = 10;
const BLOODLINE_CATEGORIES: ReadonlyArray<BloodlineStatsRow["category"]> = [
  "damDamSire",
  "damSire",
  "damSireSire",
  "sire",
  "sireDamSire",
  "sireSire",
  "sireSireSire",
] satisfies ReadonlyArray<BloodlineStatsRow["category"]>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isBloodlineCategory = (value: string): value is BloodlineStatsRow["category"] =>
  BLOODLINE_CATEGORIES.some((category) => category === value);

const isSimilarKind = (value: string): value is CatalogSimilarRow["kind"] =>
  value === "jockey" || value === "jockeyFrame" || value === "owner" || value === "trainer";

const flagParam = (value: boolean): string => (value ? "1" : "0");

const requiredString = (value: unknown): string | null => {
  if (typeof value === "string" && value.length > 0) return value;
  if (typeof value === "number" || typeof value === "bigint") {
    const text = String(value);
    return text.length > 0 ? text : null;
  }
  return null;
};

const requiredInteger = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isInteger(value)) return value;
  if (typeof value === "bigint") {
    const parsed = Number(value);
    return Number.isInteger(parsed) ? parsed : null;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isInteger(parsed) ? parsed : null;
  }
  return null;
};

const toRate = (count: number, starts: number): number =>
  starts === 0
    ? 0
    : Math.round((count * MAX_WIN_RATE * RATE_DECIMAL_FACTOR) / starts) / RATE_DECIMAL_FACTOR;

const parseCatalogBloodlineRow = (value: unknown): CatalogBloodlineRow | null => {
  if (!isRecord(value)) return null;
  const category = requiredString(value.category);
  if (category === null || !isBloodlineCategory(category)) return null;
  const name = requiredString(value.name);
  const places = requiredInteger(value.places);
  const shows = requiredInteger(value.shows);
  const starts = requiredInteger(value.starts);
  const umaban = requiredInteger(value.umaban);
  const wins = requiredInteger(value.wins);
  if (
    name === null ||
    places === null ||
    shows === null ||
    starts === null ||
    umaban === null ||
    wins === null
  ) {
    return null;
  }
  return { category, name, places, shows, starts, umaban, wins };
};

const parseCatalogSimilarRow = (value: unknown): CatalogSimilarRow | null => {
  if (!isRecord(value)) return null;
  const kind = requiredString(value.kind);
  if (kind === null || !isSimilarKind(kind)) return null;
  const name = requiredString(value.name);
  const places = requiredInteger(value.places);
  const shows = requiredInteger(value.shows);
  const starts = requiredInteger(value.starts);
  const umaban = requiredInteger(value.umaban);
  const wins = requiredInteger(value.wins);
  if (
    name === null ||
    places === null ||
    shows === null ||
    starts === null ||
    umaban === null ||
    wins === null
  ) {
    return null;
  }
  return { kind, name, places, shows, starts, umaban, wins };
};

const toBloodlineStatsRow = (row: CatalogBloodlineRow): BloodlineStatsRow => ({
  category: row.category,
  currentHorseNumbers: String(row.umaban),
  details: [],
  horseCount: 0,
  name: row.name,
  quinellaCount: row.places,
  quinellaRate: toRate(row.places, row.starts),
  showCount: row.shows,
  showRate: toRate(row.shows, row.starts),
  starts: row.starts,
  winCount: row.wins,
  winRate: toRate(row.wins, row.starts),
});

const toSimilarStatsRow = (row: CatalogSimilarRow): SimilarRaceStatsRow => ({
  category: row.kind,
  currentHorseNumbers: String(row.umaban),
  details: [],
  horseCount: 0,
  name: row.name,
  quinellaCount: row.places,
  quinellaRate: toRate(row.places, row.starts),
  showCount: row.shows,
  showRate: toRate(row.shows, row.starts),
  starts: row.starts,
  winCount: row.wins,
  winRate: toRate(row.wins, row.starts),
});

const splitCatalogHorseNumbers = (value: string): string[] =>
  value
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);

const compareCatalogHorseNumbers = (left: string, right: string): number => {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber) && leftNumber !== rightNumber) {
    return leftNumber - rightNumber;
  }
  if (left < right) return -1;
  return left > right ? 1 : 0;
};

const groupCatalogRateRows = <T extends CatalogGroupableRateRow>(rows: readonly T[]): T[] => {
  const grouped = rows.reduce<Map<string, T[]>>((groups, row) => {
    const key = `${row.category}\0${row.name}`;
    const existing = groups.get(key);
    if (existing === undefined) {
      return groups.set(key, [row]);
    }
    existing.push(row);
    return groups;
  }, new Map<string, T[]>());
  return Array.from(grouped.values()).flatMap((group) => {
    const first = group[0];
    if (first === undefined) return [];
    const horseNumbers = Array.from(
      new Set(group.flatMap((row) => splitCatalogHorseNumbers(row.currentHorseNumbers))),
    ).toSorted(compareCatalogHorseNumbers);
    return [
      {
        ...first,
        currentHorseNumbers: horseNumbers.join(HORSE_NUMBER_JOINER),
        details: [],
        horseCount: 0,
      },
    ];
  });
};

export const groupCatalogBloodlineRows = (
  rows: readonly BloodlineStatsRow[],
): BloodlineStatsRow[] => groupCatalogRateRows(rows);

export const groupCatalogSimilarRows = (
  rows: readonly SimilarRaceStatsRow[],
): SimilarRaceStatsRow[] => groupCatalogRateRows(rows);

export const buildWinRateHeatmapCatalogUrl = (query: WinRateHeatmapCatalogQuery): URL => {
  const url = new URL("/v1/win-rate-heatmap-stats", CATALOG_ORIGIN);
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
  if (query.includeOwner === true) {
    url.searchParams.set("includeOwner", "1");
  }
  if (query.includeJockeyFrame === true) {
    url.searchParams.set("includeJockeyFrame", "1");
  }
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

const fetchCatalogHeatmapResponse = async (
  catalog: CatalogFetcher,
  url: string,
): Promise<Response | null> => {
  try {
    return await catalog.fetch(url);
  } catch (error) {
    if (error instanceof TypeError) {
      return null;
    }
    throw error;
  }
};

export const fetchWinRateHeatmapStatsFromCatalog = async (
  query: WinRateHeatmapCatalogQuery,
): Promise<WinRateHeatmapCatalogStats | null> => {
  const env = await safeGetCloudflareEnv();
  const catalog = env?.R2_CATALOG;
  if (!catalog) return null;

  const response = await fetchCatalogHeatmapResponse(
    catalog,
    buildWinRateHeatmapCatalogUrl(query).href,
  );
  if (response === null || response.status === CATALOG_NOT_FOUND_STATUS) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`R2 Catalog heatmap stats failed: ${String(response.status)}`);
  }
  const payload: unknown = await response.json();
  if (
    !isRecord(payload) ||
    !Array.isArray(payload.bloodlineRows) ||
    !Array.isArray(payload.similarRows)
  ) {
    throw new Error("R2 Catalog heatmap stats payload is malformed");
  }
  const bloodlineRows = payload.bloodlineRows.map(parseCatalogBloodlineRow);
  const similarRows = payload.similarRows.map(parseCatalogSimilarRow);
  if (bloodlineRows.some((row) => row === null) || similarRows.some((row) => row === null)) {
    throw new Error("R2 Catalog heatmap stats rows are malformed");
  }
  return {
    bloodlineRows: bloodlineRows.filter((row) => row !== null).map(toBloodlineStatsRow),
    similarRows: similarRows.filter((row) => row !== null).map(toSimilarStatsRow),
  };
};
