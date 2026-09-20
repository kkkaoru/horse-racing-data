// Run with bun. Private day lists fail closed; no PostgreSQL fallback.
import "server-only";
import { Buffer } from "node:buffer";

import { readBoundedCatalogBody } from "./catalog-read-body";
import type { CatalogRaceDetailBinding } from "./race-detail-catalog";
import type { RaceListItem } from "./race-types";

interface DayListValidation {
  date: string;
  withJockeyNames: boolean;
}
interface DayListRead extends DayListValidation {
  binding: CatalogRaceDetailBinding | undefined;
}

const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-day-list";
const JOCKEY_ENDPOINT: string =
  "https://pc-keiba-r2-catalog.internal/v1/race-day-list-with-jockeys";
const EDGE_SPACES: RegExp = /^ +| +$/gu;
const FAILURE: string = "Catalog race day list unavailable";
const TIMEOUT_MS: number = 35_000;
const MAX_RACES: number = 4096;
const FIELD_COUNT: number = 18;
const DATE_PATTERN: RegExp = /^[1-9]\d{7}$/u;
const VENUE_PATTERN: RegExp = /^[0-9A-Z]{2}$/u;
const RACE_PATTERN: RegExp = /^\d{2}$/u;
const NULLABLE_FIELDS: readonly string[] = [
  "kyosomeiHondai",
  "kyosomeiFukudai",
  "gradeCode",
  "kyosoShubetsuCode",
  "kyosoKigoCode",
  "juryoShubetsuCode",
  "kyosoJokenCode",
  "kyosoJokenMeisho",
  "kyori",
  "trackCode",
  "hassoJikoku",
  "shussoTosu",
];

const validDate = (date: string): boolean => {
  if (!DATE_PATTERN.test(date)) return false;
  const iso: string = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6)}`;
  const parsed: Date = new Date(`${iso}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === iso;
};
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const compareUtf8 = (left: string, right: string): number =>
  Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
const isName = (value: unknown): value is string =>
  typeof value === "string" &&
  value.length > 0 &&
  value.isWellFormed() &&
  value.replace(EDGE_SPACES, "") === value;
const validNames = (value: unknown, withJockeyNames: boolean): value is string[] => {
  if (!Array.isArray(value)) return false;
  if (!withJockeyNames) return value.length === 0;
  if (!value.every(isName)) return false;
  return value.every((name, index) => {
    const previous: string | undefined = value[index - 1];
    return previous === undefined || compareUtf8(previous, name) < 0;
  });
};
const isRace = (value: unknown, options: DayListValidation): value is RaceListItem =>
  isRecord(value) &&
  Object.keys(value).length === FIELD_COUNT &&
  (value.source === "jra" || value.source === "nar") &&
  value.kaisaiNen === options.date.slice(0, 4) &&
  value.kaisaiTsukihi === options.date.slice(4) &&
  typeof value.keibajoCode === "string" &&
  VENUE_PATTERN.test(value.keibajoCode) &&
  typeof value.raceBango === "string" &&
  RACE_PATTERN.test(value.raceBango) &&
  validNames(value.jockeyNames, options.withJockeyNames) &&
  NULLABLE_FIELDS.every((key) => value[key] === null || typeof value[key] === "string");
const compareText = (left: string, right: string): number => {
  if (left === right) return 0;
  return left < right ? -1 : 1;
};
const compareStart = (left: string | null, right: string | null): number => {
  if (left === right) return 0;
  if (left === null) return 1;
  if (right === null) return -1;
  return compareText(left, right);
};
const compareRows = (left: RaceListItem, right: RaceListItem): number =>
  compareStart(left.hassoJikoku, right.hassoJikoku) ||
  compareText(left.keibajoCode, right.keibajoCode) ||
  compareText(left.raceBango, right.raceBango) ||
  compareText(left.source, right.source);
const identity = (row: RaceListItem): string => `${row.source}/${row.keibajoCode}/${row.raceBango}`;
const parseRaces = (value: unknown, options: DayListValidation): RaceListItem[] => {
  if (!isRecord(value) || Object.keys(value).length !== 1 || !Array.isArray(value.races)) {
    throw new Error(FAILURE);
  }
  const races: unknown[] = value.races;
  if (
    races.length > MAX_RACES ||
    !races.every((row): row is RaceListItem => isRace(row, options))
  ) {
    throw new Error(FAILURE);
  }
  if (
    new Set(races.map(identity)).size !== races.length ||
    races.some((row, index) => {
      const previous: RaceListItem | undefined = races[index - 1];
      return previous !== undefined && compareRows(previous, row) > 0;
    })
  )
    throw new Error(FAILURE);
  return races;
};

const readCatalogDayList = async ({
  binding,
  date,
  withJockeyNames,
}: DayListRead): Promise<RaceListItem[]> => {
  if (binding === undefined || !validDate(date)) throw new Error(FAILURE);
  const url: URL = new URL(withJockeyNames ? JOCKEY_ENDPOINT : ENDPOINT);
  url.searchParams.set("date", date);
  try {
    const response: Response = await binding.fetch(
      new Request(url, {
        method: "GET",
        redirect: "manual",
        signal: AbortSignal.timeout(TIMEOUT_MS),
      }),
    );
    if (response.status !== 200 || response.headers.get("cache-control") !== "no-store") {
      await response.body?.cancel();
      throw new Error(FAILURE);
    }
    return parseRaces(
      await readBoundedCatalogBody(
        response,
        withJockeyNames ? "race-day-list-with-jockeys" : undefined,
      ),
      { date, withJockeyNames },
    );
  } catch {
    throw new Error(FAILURE);
  }
};

export const readCatalogRaceDayList = (
  binding: CatalogRaceDetailBinding | undefined,
  date: string,
): Promise<RaceListItem[]> => readCatalogDayList({ binding, date, withJockeyNames: false });

export const readCatalogRaceDayListWithJockeys = (
  binding: CatalogRaceDetailBinding | undefined,
  date: string,
): Promise<RaceListItem[]> => readCatalogDayList({ binding, date, withJockeyNames: true });
