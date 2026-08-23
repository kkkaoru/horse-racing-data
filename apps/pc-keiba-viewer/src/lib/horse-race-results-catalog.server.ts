// Run with bun.
import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import type { RaceSource } from "./codes";
import type { HorseRaceResult } from "./race-types";

export interface HorseRaceResultsCatalogQuery {
  day: string;
  keibajoCode: string;
  month: string;
  raceBango: string;
  source: RaceSource;
  sourceScope: RaceSource | "all";
  year: string;
}

interface CatalogFetcher {
  fetch: (input: string) => Promise<Response>;
}

const CATALOG_ORIGIN: string = "https://pc-keiba-r2-catalog.internal";
const PARSE_FAILED: unique symbol = Symbol("parse-failed");

const HORSE_RACE_RESULT_IDENTIFIER_KEYS: ReadonlyArray<keyof HorseRaceResult> = [
  "kaisaiNen",
  "kaisaiTsukihi",
  "keibajoCode",
  "raceBango",
];

const HORSE_RACE_RESULT_NULLABLE_KEYS: ReadonlyArray<keyof HorseRaceResult> = [
  "babajotaiCodeDirt",
  "babajotaiCodeShiba",
  "bamei",
  "banushimei",
  "barei",
  "bataiju",
  "chokyoshimeiRyakusho",
  "corner1",
  "corner2",
  "corner3",
  "corner4",
  "currentBarei",
  "currentJockey",
  "currentSeibetsuCode",
  "currentUmaban",
  "futanJuryo",
  "gradeCode",
  "hassoJikoku",
  "juryoShubetsuCode",
  "kakuteiChakujun",
  "kettoTorokuBango",
  "kishumeiRyakusho",
  "kohan3f",
  "kyori",
  "kyosoJokenCode",
  "kyosoJokenMeisho",
  "kyosoKigoCode",
  "kyosoShubetsuCode",
  "kyosomeiFukudai",
  "kyosomeiHondai",
  "kyosomeiKakkonai",
  "seibetsuCode",
  "sohaTime",
  "tanshoNinkijun",
  "tanshoOdds",
  "tenkoCode",
  "timeSa",
  "trackCode",
  "umaban",
  "wakuban",
  "zogenFugo",
  "zogenSa",
];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const requiredString = (value: unknown): string | null => {
  if (typeof value === "string" && value.length > 0) return value;
  if (typeof value === "number" || typeof value === "bigint") {
    const text = String(value);
    return text.length > 0 ? text : null;
  }
  return null;
};

const parseNullableStringField = (value: unknown): string | null | typeof PARSE_FAILED => {
  if (value === null) return null;
  const parsed = requiredString(value);
  return parsed === null ? PARSE_FAILED : parsed;
};

const isOptionalNullableStringField = (value: unknown): value is string | null | undefined =>
  value === undefined || parseNullableStringField(value) !== PARSE_FAILED;

const parseKeyedNullableStrings = (
  value: Record<string, unknown>,
  keys: readonly (keyof HorseRaceResult)[],
): Map<keyof HorseRaceResult, string | null> | null => {
  const parsed = keys.map((key) => {
    const field = parseNullableStringField(value[key]);
    return field === PARSE_FAILED
      ? null
      : ([key, field] satisfies [keyof HorseRaceResult, string | null]);
  });
  if (parsed.some((entry) => entry === null)) return null;
  return new Map(
    parsed.filter((entry): entry is [keyof HorseRaceResult, string | null] => entry !== null),
  );
};

const readNullable = (
  fields: Map<keyof HorseRaceResult, string | null>,
  key: keyof HorseRaceResult,
): string | null => {
  const field = fields.get(key);
  return field === undefined ? null : field;
};

const parseHorseRaceResult = (value: unknown): HorseRaceResult | null => {
  if (!isRecord(value)) return null;
  const identifiers = HORSE_RACE_RESULT_IDENTIFIER_KEYS.map((key) => requiredString(value[key]));
  if (identifiers.some((field) => field === null)) return null;
  const filledIdentifiers = identifiers.filter((field): field is string => field !== null);
  const kaisaiNen = filledIdentifiers[0];
  const kaisaiTsukihi = filledIdentifiers[1];
  const keibajoCode = filledIdentifiers[2];
  const raceBango = filledIdentifiers[3];
  if (
    kaisaiNen === undefined ||
    kaisaiTsukihi === undefined ||
    keibajoCode === undefined ||
    raceBango === undefined
  ) {
    return null;
  }
  const nullableFields = parseKeyedNullableStrings(value, HORSE_RACE_RESULT_NULLABLE_KEYS);
  if (nullableFields === null) return null;
  if (!isOptionalNullableStringField(value.shussoTosu)) return null;
  if (!isOptionalNullableStringField(value.blinkerShiyoKubun)) return null;
  const shussoTosu =
    value.shussoTosu === undefined ? undefined : parseNullableStringField(value.shussoTosu);
  const blinkerShiyoKubun =
    value.blinkerShiyoKubun === undefined
      ? undefined
      : parseNullableStringField(value.blinkerShiyoKubun);
  if (shussoTosu === PARSE_FAILED || blinkerShiyoKubun === PARSE_FAILED) return null;
  return {
    babajotaiCodeDirt: readNullable(nullableFields, "babajotaiCodeDirt"),
    babajotaiCodeShiba: readNullable(nullableFields, "babajotaiCodeShiba"),
    bamei: readNullable(nullableFields, "bamei"),
    banushimei: readNullable(nullableFields, "banushimei"),
    barei: readNullable(nullableFields, "barei"),
    bataiju: readNullable(nullableFields, "bataiju"),
    chokyoshimeiRyakusho: readNullable(nullableFields, "chokyoshimeiRyakusho"),
    corner1: readNullable(nullableFields, "corner1"),
    corner2: readNullable(nullableFields, "corner2"),
    corner3: readNullable(nullableFields, "corner3"),
    corner4: readNullable(nullableFields, "corner4"),
    currentBarei: readNullable(nullableFields, "currentBarei"),
    currentJockey: readNullable(nullableFields, "currentJockey"),
    currentSeibetsuCode: readNullable(nullableFields, "currentSeibetsuCode"),
    currentUmaban: readNullable(nullableFields, "currentUmaban"),
    futanJuryo: readNullable(nullableFields, "futanJuryo"),
    gradeCode: readNullable(nullableFields, "gradeCode"),
    hassoJikoku: readNullable(nullableFields, "hassoJikoku"),
    juryoShubetsuCode: readNullable(nullableFields, "juryoShubetsuCode"),
    kaisaiNen,
    kaisaiTsukihi,
    kakuteiChakujun: readNullable(nullableFields, "kakuteiChakujun"),
    keibajoCode,
    kettoTorokuBango: readNullable(nullableFields, "kettoTorokuBango"),
    kishumeiRyakusho: readNullable(nullableFields, "kishumeiRyakusho"),
    kohan3f: readNullable(nullableFields, "kohan3f"),
    kyori: readNullable(nullableFields, "kyori"),
    kyosoJokenCode: readNullable(nullableFields, "kyosoJokenCode"),
    kyosoJokenMeisho: readNullable(nullableFields, "kyosoJokenMeisho"),
    kyosoKigoCode: readNullable(nullableFields, "kyosoKigoCode"),
    kyosoShubetsuCode: readNullable(nullableFields, "kyosoShubetsuCode"),
    kyosomeiFukudai: readNullable(nullableFields, "kyosomeiFukudai"),
    kyosomeiHondai: readNullable(nullableFields, "kyosomeiHondai"),
    kyosomeiKakkonai: readNullable(nullableFields, "kyosomeiKakkonai"),
    raceBango,
    seibetsuCode: readNullable(nullableFields, "seibetsuCode"),
    sohaTime: readNullable(nullableFields, "sohaTime"),
    tanshoNinkijun: readNullable(nullableFields, "tanshoNinkijun"),
    tanshoOdds: readNullable(nullableFields, "tanshoOdds"),
    tenkoCode: readNullable(nullableFields, "tenkoCode"),
    timeSa: readNullable(nullableFields, "timeSa"),
    trackCode: readNullable(nullableFields, "trackCode"),
    umaban: readNullable(nullableFields, "umaban"),
    wakuban: readNullable(nullableFields, "wakuban"),
    zogenFugo: readNullable(nullableFields, "zogenFugo"),
    zogenSa: readNullable(nullableFields, "zogenSa"),
    ...(shussoTosu === undefined ? {} : { shussoTosu }),
    ...(blinkerShiyoKubun === undefined ? {} : { blinkerShiyoKubun }),
  };
};

export const buildHorseRaceResultsCatalogUrl = (query: HorseRaceResultsCatalogQuery): URL => {
  const url = new URL("/v1/horse-race-results", CATALOG_ORIGIN);
  url.searchParams.set(
    "date",
    `${query.year}${query.month.padStart(2, "0")}${query.day.padStart(2, "0")}`,
  );
  url.searchParams.set("keibajoCode", query.keibajoCode.padStart(2, "0"));
  url.searchParams.set("raceBango", query.raceBango.padStart(2, "0"));
  url.searchParams.set("source", query.source);
  url.searchParams.set("sourceScope", query.sourceScope);
  return url;
};

const fetchCatalogResultsResponse = async (
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

export const fetchHorseRaceResultsFromCatalog = async (
  query: HorseRaceResultsCatalogQuery,
): Promise<HorseRaceResult[] | null> => {
  const env = await safeGetCloudflareEnv();
  const catalog = env?.R2_CATALOG;
  if (!catalog) return null;

  const response = await fetchCatalogResultsResponse(
    catalog,
    buildHorseRaceResultsCatalogUrl(query).href,
  );
  if (response === null) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`R2 Catalog horse race results failed: ${String(response.status)}`);
  }
  const payload: unknown = await response.json();
  if (!isRecord(payload) || !Array.isArray(payload.rows)) {
    throw new Error("R2 Catalog horse race results payload is malformed");
  }
  const rows = payload.rows.map(parseHorseRaceResult);
  if (rows.some((row) => row === null)) {
    throw new Error("R2 Catalog horse race results rows are malformed");
  }
  return rows.filter((row) => row !== null);
};
