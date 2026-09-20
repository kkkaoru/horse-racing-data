// Run with bun. Private Catalog reader; failures never become absence or PostgreSQL fallback.
import "server-only";
import { readBoundedCatalogBody } from "./catalog-read-body";

export interface RaceHistoryRow {
  kettoTorokuBango: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  umaban: string | null;
  kyori: string | null;
  sohaTime: string | null;
  kohan3f: string | null;
  bataiju: string | null;
  futanJuryo: string | null;
  timeSa: string | null;
  kakuteiChakujun: string | null;
}

export interface CatalogRaceHistoryQuery {
  horseIds: readonly string[];
  beforeDate: string;
  minDate: string | null;
  limit: number;
}

export interface CatalogRaceHistoryBinding {
  fetch(request: Request): Promise<Response>;
}

const FIELD_COUNT: number = 13;
const TIMEOUT_MS: number = 35_000;
const ENDPOINT: string = "https://pc-keiba-r2-catalog.internal/v1/race-history";
const FAILURE: string = "Catalog race history unavailable";
const HORSE_ID: RegExp = /^\d{10}$/u;
const MAX_HORSE_IDS: number = 40;
const MAX_ROWS: number = 4000;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseRow = (value: unknown, requested: ReadonlySet<string>): RaceHistoryRow => {
  if (!isRecord(value) || Object.keys(value).length !== FIELD_COUNT) throw new Error(FAILURE);
  const text = (column: string): string | null => {
    const item: unknown = value[column];
    if (item !== null && typeof item !== "string") throw new Error(FAILURE);
    return item;
  };
  const horseId: string | null = text("ketto_toroku_bango");
  const kaisaiNen: string | null = text("kaisai_nen");
  const kaisaiTsukihi: string | null = text("kaisai_tsukihi");
  const keibajoCode: string | null = text("keibajo_code");
  const raceBango: string | null = text("race_bango");
  if (
    horseId === null ||
    !HORSE_ID.test(horseId) ||
    !requested.has(horseId) ||
    kaisaiNen === null ||
    kaisaiTsukihi === null ||
    keibajoCode === null ||
    raceBango === null
  )
    throw new Error(FAILURE);
  return {
    kettoTorokuBango: horseId,
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    raceBango,
    umaban: text("umaban"),
    kyori: text("kyori"),
    sohaTime: text("soha_time"),
    kohan3f: text("kohan_3f"),
    bataiju: text("bataiju"),
    futanJuryo: text("futan_juryo"),
    timeSa: text("time_sa"),
    kakuteiChakujun: text("kakutei_chakujun"),
  };
};

export const readCatalogRaceHistory = async (
  binding: CatalogRaceHistoryBinding | undefined,
  query: CatalogRaceHistoryQuery,
): Promise<RaceHistoryRow[]> => {
  if (
    binding === undefined ||
    query.horseIds.length === 0 ||
    query.horseIds.length > MAX_HORSE_IDS ||
    !query.horseIds.every((id) => HORSE_ID.test(id))
  )
    throw new Error(FAILURE);
  const url: URL = new URL(ENDPOINT);
  const params: Record<string, string> = {
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
