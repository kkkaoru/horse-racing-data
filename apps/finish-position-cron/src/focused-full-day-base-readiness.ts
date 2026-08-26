// Run with bun. Canonical fail-closed day-base readiness for focused-full work.

import { buildDayBaseObjectKey } from "./day-base-object-key";
import type { DaybaseWatermark } from "./ndjson-stream";
import type { Env, PredictCategory } from "./types";

interface FocusedFullDayBaseReadinessParams {
  category: PredictCategory;
  env: Env;
  runYmd: string;
}

interface DayBaseCandidateReadinessParams extends FocusedFullDayBaseReadinessParams {
  watermark: DaybaseWatermark;
}

interface DayBaseMetadata {
  maxSourceUpdated: string;
  rowCount: number;
  rsPredictedAtMax: string;
  rsRowCount: number;
}

interface CatalogRowsPayload {
  rows: unknown[];
}

interface LiveDayBaseWatermark {
  rowCount: number;
  sourceUpdatedMax: string | null;
}

export interface FocusedFullDayBaseReadiness {
  ready: boolean;
  reason: string;
}

const CATALOG_ORIGIN: string = "https://pc-keiba-r2-catalog.internal";
const READY_REASON: string = "ready";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const numericMetadata = (metadata: Record<string, string>, key: string): number | null => {
  const raw = metadata[key];
  if (raw === undefined || raw.trim().length === 0) return null;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
};

const parseMetadata = (object: R2Object | null): DayBaseMetadata | null => {
  if (object === null || object.customMetadata === undefined) return null;
  const metadata = object.customMetadata;
  const maxSourceUpdated = metadata["max-data-sakusei-nengappi"]?.trim() ?? "";
  const rsPredictedAtMax = metadata["rs-predicted-at-max"]?.trim() ?? "";
  const rowCount = numericMetadata(metadata, "row-count");
  const rsRowCount = numericMetadata(metadata, "rs-row-count");
  if (maxSourceUpdated.length === 0 || rsPredictedAtMax.length === 0) return null;
  if (rowCount === null || rowCount === 0 || rsRowCount === null) return null;
  return { maxSourceUpdated, rowCount, rsPredictedAtMax, rsRowCount };
};

const parseCandidateMetadata = (watermark: DaybaseWatermark): DayBaseMetadata | null => {
  const maxSourceUpdated = watermark.maxDataSakuseiNengappi.trim();
  const rsPredictedAtMax = watermark.rsPredictedAtMax.trim();
  if (maxSourceUpdated.length === 0 || rsPredictedAtMax.length === 0) return null;
  if (!Number.isSafeInteger(watermark.rowCount) || watermark.rowCount <= 0) return null;
  if (!Number.isSafeInteger(watermark.rsRowCount) || watermark.rsRowCount < 0) return null;
  return {
    maxSourceUpdated,
    rowCount: watermark.rowCount,
    rsPredictedAtMax,
    rsRowCount: watermark.rsRowCount,
  };
};

const catalogSource = (category: PredictCategory): string => category;

const optionalSourceWatermark = (rows: readonly unknown[]): string | null => {
  const values = rows.flatMap((row): string[] => {
    if (!isRecord(row)) return [];
    const value = row.data_sakusei_nengappi;
    return typeof value === "string" && value.trim().length > 0 ? [value.trim()] : [];
  });
  return values.length === 0 ? null : (values.toSorted().at(-1) ?? null);
};

const fetchCatalogWatermark = async (
  params: FocusedFullDayBaseReadinessParams,
): Promise<{ rowCount: number; sourceUpdatedMax: string | null }> => {
  if (params.env.PC_KEIBA_R2_CATALOG === undefined)
    throw new Error("PC_KEIBA_R2_CATALOG binding is unavailable");
  const url = new URL("/v1/race-features", CATALOG_ORIGIN);
  url.searchParams.set("date", params.runYmd);
  url.searchParams.set("source", catalogSource(params.category));
  const response = await params.env.PC_KEIBA_R2_CATALOG.fetch(new Request(url));
  if (!response.ok)
    throw new Error(`Catalog day-base readiness failed with HTTP ${response.status}`);
  const payload: unknown = await response.json();
  if (!isRecord(payload) || !Array.isArray(payload.rows))
    throw new Error("Catalog day-base readiness returned invalid rows");
  const catalogPayload: CatalogRowsPayload = { rows: payload.rows };
  return {
    rowCount: catalogPayload.rows.length,
    sourceUpdatedMax: optionalSourceWatermark(catalogPayload.rows),
  };
};

const liveWatermark = async (
  params: FocusedFullDayBaseReadinessParams,
): Promise<LiveDayBaseWatermark | null> => {
  const catalog = await fetchCatalogWatermark(params);
  return catalog.rowCount === 0 ? null : catalog;
};

const compareWithLiveWatermark = async (
  params: FocusedFullDayBaseReadinessParams,
  metadata: DayBaseMetadata,
): Promise<FocusedFullDayBaseReadiness> => {
  const live = await liveWatermark(params);
  if (live === null) return { ready: false, reason: "live-readiness-incomplete" };
  if (metadata.rowCount !== live.rowCount)
    return {
      ready: false,
      reason: `source-row-count-${String(metadata.rowCount)}-of-${String(live.rowCount)}`,
    };
  if (live.sourceUpdatedMax !== null && metadata.maxSourceUpdated !== live.sourceUpdatedMax)
    return { ready: false, reason: "source-watermark-mismatch" };
  return { ready: true, reason: READY_REASON };
};

export const getDayBaseCandidateReadiness = async (
  params: DayBaseCandidateReadinessParams,
): Promise<FocusedFullDayBaseReadiness> => {
  const metadata = parseCandidateMetadata(params.watermark);
  if (metadata === null) return { ready: false, reason: "day-base-missing-or-invalid" };
  return compareWithLiveWatermark(params, metadata);
};

export const getFocusedFullDayBaseReadiness = async (
  params: FocusedFullDayBaseReadinessParams,
): Promise<FocusedFullDayBaseReadiness> => {
  const object = await params.env.FEATURES_CACHE.head(buildDayBaseObjectKey(params));
  const metadata = parseMetadata(object);
  if (metadata === null) return { ready: false, reason: "day-base-missing-or-invalid" };
  return compareWithLiveWatermark(params, metadata);
};

// The hourly category prewarm uses the same live Catalog + per-race
// running-style contract as focused-full dispatch. Keeping this alias in the
// readiness module gives the prewarm path an explicit fail-closed API while
// ensuring the two callers cannot drift to different freshness rules.
export const getDayBasePrewarmHitReadiness = getFocusedFullDayBaseReadiness;
