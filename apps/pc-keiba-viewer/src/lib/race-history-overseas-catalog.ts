// Run with bun. Private Catalog reader for the overseas history mode; the
// 13-field jra/nar shape stays in race-history-catalog.ts.
import "server-only";
import { readBoundedCatalogBody } from "./catalog-read-body";

export interface OverseasRaceHistoryRow {
  sourceHorseId: string;
  raceDate: string;
  distanceMetres: number | null;
}

export interface CatalogOverseasRaceHistoryQuery {
  horseIds: readonly string[];
  beforeDate: string;
  minDate: string | null;
  limit: number;
}

export interface CatalogOverseasRaceHistoryBinding {
  fetch(request: Request): Promise<Response>;
}

const FIELD_COUNT: number = 3;
const TIMEOUT_MS: number = 35_000;
const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-history";
const FAILURE: string = "Catalog race history unavailable";
const HORSE_ID: RegExp = /^\d{10}$/u;
const DASHED_DATE: RegExp = /^(\d{4})-(\d{2})-(\d{2})$/u;
const MAX_HORSE_IDS: number = 40;
const MAX_ROWS: number = 4000;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseDistance = (value: unknown): number | null => {
  if (value === null) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value !== "" && Number.isFinite(Number(value)))
    return Number(value);
  throw new Error(FAILURE);
};

const parseRow = (value: unknown, requested: ReadonlySet<string>): OverseasRaceHistoryRow => {
  if (!isRecord(value) || Object.keys(value).length !== FIELD_COUNT) throw new Error(FAILURE);
  const sourceHorseId: unknown = value.sourceHorseId;
  const raceDate: unknown = value.raceDate;
  if (typeof sourceHorseId !== "string" || !HORSE_ID.test(sourceHorseId)) throw new Error(FAILURE);
  if (!requested.has(sourceHorseId)) throw new Error(FAILURE);
  if (typeof raceDate !== "string") throw new Error(FAILURE);
  const match: RegExpMatchArray | null = DASHED_DATE.exec(raceDate);
  if (match === null) throw new Error(FAILURE);
  const parsed: Date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  if (
    !Number.isFinite(parsed.getTime()) ||
    parsed.getUTCFullYear() !== Number(match[1]) ||
    parsed.getUTCMonth() !== Number(match[2]) - 1 ||
    parsed.getUTCDate() !== Number(match[3])
  )
    throw new Error(FAILURE);
  return { sourceHorseId, raceDate, distanceMetres: parseDistance(value.distanceMetres) };
};

export const readCatalogOverseasRaceHistory = async (
  binding: CatalogOverseasRaceHistoryBinding | undefined,
  query: CatalogOverseasRaceHistoryQuery,
): Promise<OverseasRaceHistoryRow[]> => {
  if (
    binding === undefined ||
    query.horseIds.length === 0 ||
    query.horseIds.length > MAX_HORSE_IDS ||
    !query.horseIds.every((id) => HORSE_ID.test(id)) ||
    !/^\d{8}$/u.test(query.beforeDate) ||
    (query.minDate !== null && !/^\d{8}$/u.test(query.minDate))
  )
    throw new Error(FAILURE);
  const url: URL = new URL(ENDPOINT);
  const params: Record<string, string> = {
    source: "overseas",
    horseIds: query.horseIds.join(","),
    beforeDate: query.beforeDate,
    limit: String(query.limit),
  };
  if (query.minDate !== null) params.minDate = query.minDate;
  url.search = new URLSearchParams(params).toString();
  try {
    const response: Response = await binding.fetch(
      new Request(url, {
        method: "GET",
        // Workers supports manual/follow, not error; non-200 responses are rejected below.
        redirect: "manual",
        signal: AbortSignal.timeout(TIMEOUT_MS),
      }),
    );
    if (response.status !== 200 || response.headers.get("cache-control") !== "no-store") {
      await response.body?.cancel();
      throw new Error(FAILURE);
    }
    const value: unknown = await readBoundedCatalogBody(response);
    if (!isRecord(value) || !Array.isArray(value.rows)) throw new Error(FAILURE);
    if (value.rows.length > MAX_ROWS) throw new Error(FAILURE);
    const requested: ReadonlySet<string> = new Set(query.horseIds);
    return value.rows.map((row) => parseRow(row, requested));
  } catch {
    throw new Error(FAILURE);
  }
};
