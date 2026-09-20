// Run with bun. Private year summaries fail closed without PostgreSQL fallback.
import "server-only";
import { readBoundedCatalogBody } from "./catalog-read-body";
import type { CatalogRaceDetailBinding } from "./race-detail-catalog";
import type { RaceYearSummary } from "./race-types";

const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-years";
const FAILURE: string = "Catalog race years unavailable";
const YEAR_PATTERN: RegExp = /^[1-9]\d{3}$/u;
const TIMEOUT_MS: number = 35_000;
const MAX_YEARS: number = 256;
const FIELD_COUNT: number = 3;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isCount = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0;

const isYear = (value: unknown): value is RaceYearSummary => {
  if (
    !isRecord(value) ||
    Object.keys(value).length !== FIELD_COUNT ||
    typeof value.year !== "string" ||
    !YEAR_PATTERN.test(value.year) ||
    !isCount(value.raceCount) ||
    !isCount(value.dayCount)
  )
    return false;
  const year: number = Number(value.year);
  const leap: boolean = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  return value.dayCount <= (leap ? 366 : 365) && value.raceCount >= value.dayCount;
};

const parseYears = (value: unknown): RaceYearSummary[] => {
  if (!isRecord(value) || Object.keys(value).length !== 1 || !Array.isArray(value.years)) {
    throw new Error(FAILURE);
  }
  const years: unknown[] = value.years;
  if (years.length > MAX_YEARS || !years.every(isYear)) throw new Error(FAILURE);
  const keys: string[] = years.map((row) => row.year);
  if (new Set(keys).size !== keys.length || keys.join() !== keys.toSorted().toReversed().join()) {
    throw new Error(FAILURE);
  }
  return years;
};

export const readCatalogRaceYears = async (
  binding: CatalogRaceDetailBinding | undefined,
): Promise<RaceYearSummary[]> => {
  if (binding === undefined) throw new Error(FAILURE);
  try {
    const response: Response = await binding.fetch(
      new Request(ENDPOINT, {
        method: "GET",
        redirect: "manual",
        signal: AbortSignal.timeout(TIMEOUT_MS),
      }),
    );
    if (response.status !== 200 || response.headers.get("cache-control") !== "no-store") {
      await response.body?.cancel();
      throw new Error(FAILURE);
    }
    return parseYears(await readBoundedCatalogBody(response));
  } catch {
    throw new Error(FAILURE);
  }
};
