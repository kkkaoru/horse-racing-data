// Run with bun. Private Catalog reader; failures never become absence or PostgreSQL fallback.
import "server-only";
import { readBoundedCatalogBody } from "./catalog-read-body";
import type { RaceSource } from "./codes";
import type { RaceDetail } from "./race-types";

export interface CatalogRaceDetailQuery {
  source: RaceSource;
  date: string;
  keibajoCode: string;
  raceBango: string;
}
export interface CatalogRaceDetailBinding {
  fetch(request: Request): Promise<Response>;
}
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
  "kaisaiKai",
  "kaisaiNichime",
  "kyosomeiKakkonai",
  "torokuTosu",
  "tenkoCode",
  "babajotaiCodeShiba",
  "babajotaiCodeDirt",
];
const TIMEOUT_MS: number = 35_000;
const DETAIL_FIELD_COUNT: number = 24;
const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-detail";
const FAILURE: string = "Catalog race detail unavailable";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isDetail = (value: unknown, query: CatalogRaceDetailQuery): value is RaceDetail =>
  isRecord(value) &&
  Object.keys(value).length === DETAIL_FIELD_COUNT &&
  value.source === query.source &&
  value.kaisaiNen === query.date.slice(0, 4) &&
  value.kaisaiTsukihi === query.date.slice(4) &&
  value.keibajoCode === query.keibajoCode &&
  value.raceBango === query.raceBango &&
  NULLABLE_FIELDS.every((key) => value[key] === null || typeof value[key] === "string");

export const readCatalogRaceDetail = async (
  binding: CatalogRaceDetailBinding | undefined,
  query: CatalogRaceDetailQuery,
): Promise<RaceDetail | null> => {
  if (binding === undefined) throw new Error(FAILURE);
  const url: URL = new URL(ENDPOINT);
  url.search = new URLSearchParams({
    source: query.source,
    date: query.date,
    keibajoCode: query.keibajoCode,
    raceBango: query.raceBango,
  }).toString();
  try {
    const response = await binding.fetch(
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
    if (!isRecord(value)) throw new Error(FAILURE);
    if (value.row === null) return null;
    if (!isDetail(value.row, query)) throw new Error(FAILURE);
    return value.row;
  } catch {
    throw new Error(FAILURE);
  }
};
