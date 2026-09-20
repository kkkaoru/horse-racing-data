// Run with bun. Private calendar reads fail closed; PostgreSQL is never a fallback.
import "server-only";
import { readBoundedCatalogBody } from "./catalog-read-body";
import type { CatalogRaceDetailBinding } from "./race-detail-catalog";
import type { RaceDaySummary } from "./race-types";

const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-calendar";
const FAILURE: string = "Catalog race calendar unavailable";
const YEAR_PATTERN: RegExp = /^[1-9]\d{3}$/u;
const CODE_PATTERN: RegExp = /^\d{2}$/u;
const TIMEOUT_MS: number = 35_000;
const MAX_DAYS: number = 366;
const FIELD_COUNT: number = 5;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isCount = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const isDay = (value: unknown, year: string): value is RaceDaySummary => {
  if (
    !isRecord(value) ||
    Object.keys(value).length !== FIELD_COUNT ||
    value.year !== year ||
    typeof value.month !== "string" ||
    typeof value.day !== "string" ||
    !CODE_PATTERN.test(value.month) ||
    !CODE_PATTERN.test(value.day) ||
    !isCount(value.jraCount) ||
    !isCount(value.narCount) ||
    (value.jraCount === 0 && value.narCount === 0)
  )
    return false;
  const date: Date = new Date(Date.UTC(Number(year), Number(value.month) - 1, Number(value.day)));
  return date.toISOString().slice(0, 10) === `${year}-${value.month}-${value.day}`;
};

const parseDays = (value: unknown, year: string): RaceDaySummary[] => {
  if (!isRecord(value) || Object.keys(value).length !== 1 || !Array.isArray(value.days))
    throw new Error(FAILURE);
  const days: unknown[] = value.days;
  if (days.length > MAX_DAYS || !days.every((day): day is RaceDaySummary => isDay(day, year)))
    throw new Error(FAILURE);
  const keys: string[] = days.map((day) => day.month + day.day);
  if (new Set(keys).size !== keys.length || keys.join() !== keys.toSorted().toReversed().join())
    throw new Error(FAILURE);
  return days;
};

export const readCatalogRaceCalendar = async (
  binding: CatalogRaceDetailBinding | undefined,
  year: string,
): Promise<RaceDaySummary[]> => {
  if (binding === undefined || !YEAR_PATTERN.test(year)) throw new Error(FAILURE);
  const url: URL = new URL(ENDPOINT);
  url.searchParams.set("year", year);
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
    return parseDays(await readBoundedCatalogBody(response), year);
  } catch {
    throw new Error(FAILURE);
  }
};
