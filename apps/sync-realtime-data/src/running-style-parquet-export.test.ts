// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  buildRunningStyleDayParquetKey,
  exportRunningStyleParquetForDay,
  serializeRunningStylePredictionParquet,
  type RunningStylePredictionExportRow,
} from "./running-style-parquet-export";
import type { Env } from "./types";

const ROW_JRA_A: RunningStylePredictionExportRow = {
  kaisai_nen: "2026",
  kaisai_tsukihi: "0607",
  keibajo_code: "08",
  ketto_toroku_bango: "2024100001",
  cell_model_key: "running-style/models/jra/cells/tokyo-turf.flatbin",
  cell_variant_id: "tokyo-turf",
  model_version: "jra-running-style-lgbm-prod-v3",
  p_nige: 0.1,
  p_oikomi: 0.2,
  p_sashi: 0.3,
  p_senkou: 0.4,
  predicted_at: "2026-06-07T11:30:00.000Z",
  predicted_class: 1,
  predicted_label: "senkou",
  race_bango: "01",
  source: "jra",
  umaban: 1,
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("buildRunningStyleDayParquetKey composes the per-day per-source per-model key", () => {
  expect(
    buildRunningStyleDayParquetKey({
      dateYmd: "20260607",
      modelVersion: "jra-running-style-lgbm-prod-v3",
      source: "jra",
    }),
  ).toBe("running-style/predictions/by-day/2026/06/07/jra/jra-running-style-lgbm-prod-v3.parquet");
});

it("buildRunningStyleDayParquetKey supports the nar source variant", () => {
  expect(
    buildRunningStyleDayParquetKey({
      dateYmd: "20260101",
      modelVersion: "nar-running-style-lgbm-prod-v3",
      source: "nar",
    }),
  ).toBe("running-style/predictions/by-day/2026/01/01/nar/nar-running-style-lgbm-prod-v3.parquet");
});

it("serializeRunningStylePredictionParquet emits non-empty Parquet bytes", async () => {
  const bytes = await serializeRunningStylePredictionParquet([ROW_JRA_A]);
  expect(bytes.byteLength).toBeGreaterThan(0);
  expect(bytes[0]).toBe(0x50);
  expect(bytes[1]).toBe(0x41);
  expect(bytes[2]).toBe(0x52);
  expect(bytes[3]).toBe(0x31);
});

it("exportRunningStyleParquetForDay returns skipped when FEATURES_ARCHIVE is missing", async () => {
  const env = {
    FEATURES_ARCHIVE: undefined,
    REALTIME_DB: {} as unknown as D1Database,
  } as unknown as Env;
  const result = await exportRunningStyleParquetForDay({
    dateYmd: "20260607",
    env,
    source: "jra",
  });
  expect(result.skipped).toBe(true);
  expect(result.skippedReason).toBe("FEATURES_ARCHIVE binding not configured");
  expect(result.fileCount).toBe(0);
});

it("exportRunningStyleParquetForDay returns skipped when D1 returns no rows", async () => {
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const put = vi.fn(async (_key: string, _body: Uint8Array, _options?: unknown) => undefined);
  const env = {
    FEATURES_ARCHIVE: { put } as unknown as R2Bucket,
    REALTIME_DB: { prepare } as unknown as D1Database,
  } as unknown as Env;
  const result = await exportRunningStyleParquetForDay({
    dateYmd: "20260607",
    env,
    source: "jra",
  });
  expect(result.skipped).toBe(true);
  expect(result.skippedReason).toBe("no rows for day");
  expect(put).not.toHaveBeenCalled();
});

it("exportRunningStyleParquetForDay groups rows by model_version and puts one R2 object per group", async () => {
  const rowsBatch1 = [
    {
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "A",
      cell_model_key: "running-style/models/jra/cells/tokyo-turf.flatbin",
      cell_variant_id: "tokyo-turf",
      model_version: "v3",
      p_nige: 0.1,
      p_oikomi: 0.2,
      p_sashi: 0.3,
      p_senkou: 0.4,
      predicted_at: "2026-06-07T11:00:00Z",
      predicted_label: "senkou",
      race_key: "jra:20260607:08:01",
    },
    {
      horse_number: 2,
      kaisai_nen: "2026",
      ketto_toroku_bango: "B",
      cell_model_key: null,
      cell_variant_id: null,
      model_version: "v3",
      p_nige: 0.4,
      p_oikomi: 0.1,
      p_sashi: 0.2,
      p_senkou: 0.3,
      predicted_at: "2026-06-07T11:00:00Z",
      predicted_label: "nige",
      race_key: "jra:20260607:08:01",
    },
    {
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "C",
      cell_model_key: "running-style/models/jra/latest.flatbin",
      cell_variant_id: "latest",
      model_version: "v4-experiment",
      p_nige: 0.05,
      p_oikomi: 0.05,
      p_sashi: 0.1,
      p_senkou: 0.8,
      predicted_at: "2026-06-07T11:00:00Z",
      predicted_label: "senkou",
      race_key: "jra:20260607:09:02",
    },
  ];
  const all = vi
    .fn()
    .mockResolvedValueOnce({ results: rowsBatch1 })
    .mockResolvedValueOnce({ results: [] });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const put = vi.fn(async (_key: string, _body: Uint8Array, _options?: unknown) => undefined);
  const env = {
    FEATURES_ARCHIVE: { put } as unknown as R2Bucket,
    REALTIME_DB: { prepare } as unknown as D1Database,
  } as unknown as Env;
  const result = await exportRunningStyleParquetForDay({
    dateYmd: "20260607",
    env,
    source: "jra",
  });
  expect(result.skipped).toBe(false);
  expect(result.fileCount).toBe(2);
  expect(result.rowCount).toBe(3);
  expect(result.bytesWritten).toBeGreaterThan(0);
  expect(put).toHaveBeenCalledTimes(2);
  const putKeys = put.mock.calls.map((call) => call[0] as string).sort();
  expect(putKeys).toStrictEqual([
    "running-style/predictions/by-day/2026/06/07/jra/v3.parquet",
    "running-style/predictions/by-day/2026/06/07/jra/v4-experiment.parquet",
  ]);
  expect(
    (put.mock.calls[0]![2] as { httpMetadata: { contentType: string } }).httpMetadata.contentType,
  ).toBe("application/vnd.apache.parquet");
});

it("exportRunningStyleParquetForDay filters rows whose race_key cannot be parsed", async () => {
  const rows = [
    {
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "A",
      cell_model_key: null,
      cell_variant_id: null,
      model_version: "v3",
      p_nige: 0.1,
      p_oikomi: 0.2,
      p_sashi: 0.3,
      p_senkou: 0.4,
      predicted_at: "2026-06-07T11:00:00Z",
      predicted_label: "senkou",
      race_key: "jra:short",
    },
  ];
  const all = vi
    .fn()
    .mockResolvedValueOnce({ results: rows })
    .mockResolvedValueOnce({ results: [] });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const put = vi.fn(async (_key: string, _body: Uint8Array, _options?: unknown) => undefined);
  const env = {
    FEATURES_ARCHIVE: { put } as unknown as R2Bucket,
    REALTIME_DB: { prepare } as unknown as D1Database,
  } as unknown as Env;
  const result = await exportRunningStyleParquetForDay({
    dateYmd: "20260607",
    env,
    source: "jra",
  });
  expect(result.skipped).toBe(false);
  expect(result.rowCount).toBe(0);
  expect(result.fileCount).toBe(0);
  expect(put).not.toHaveBeenCalled();
});

it("exportRunningStyleParquetForDay sorts rows by keibajo + race_bango + umaban before writing", async () => {
  const rows = [
    {
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "Z",
      cell_model_key: null,
      cell_variant_id: null,
      model_version: "v3",
      p_nige: 0.1,
      p_oikomi: 0.2,
      p_sashi: 0.3,
      p_senkou: 0.4,
      predicted_at: "2026-06-07T11:00:00Z",
      predicted_label: "senkou",
      race_key: "jra:20260607:09:02",
    },
    {
      horse_number: 2,
      kaisai_nen: "2026",
      ketto_toroku_bango: "Y",
      cell_model_key: null,
      cell_variant_id: null,
      model_version: "v3",
      p_nige: 0.1,
      p_oikomi: 0.2,
      p_sashi: 0.3,
      p_senkou: 0.4,
      predicted_at: "2026-06-07T11:00:00Z",
      predicted_label: "senkou",
      race_key: "jra:20260607:08:02",
    },
    {
      horse_number: 1,
      kaisai_nen: "2026",
      ketto_toroku_bango: "X",
      cell_model_key: null,
      cell_variant_id: null,
      model_version: "v3",
      p_nige: 0.1,
      p_oikomi: 0.2,
      p_sashi: 0.3,
      p_senkou: 0.4,
      predicted_at: "2026-06-07T11:00:00Z",
      predicted_label: "senkou",
      race_key: "jra:20260607:08:01",
    },
  ];
  const all = vi
    .fn()
    .mockResolvedValueOnce({ results: rows })
    .mockResolvedValueOnce({ results: [] });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const put = vi.fn(async (_key: string, _body: Uint8Array, _options?: unknown) => undefined);
  const env = {
    FEATURES_ARCHIVE: { put } as unknown as R2Bucket,
    REALTIME_DB: { prepare } as unknown as D1Database,
  } as unknown as Env;
  const result = await exportRunningStyleParquetForDay({
    dateYmd: "20260607",
    env,
    source: "jra",
  });
  expect(result.rowCount).toBe(3);
  expect(result.fileCount).toBe(1);
});

it("exportRunningStyleParquetForDay paginates D1 across multiple batches", async () => {
  const buildRow = (index: number): Record<string, unknown> => ({
    horse_number: index,
    kaisai_nen: "2026",
    ketto_toroku_bango: `H${index}`,
    cell_model_key: null,
    cell_variant_id: null,
    model_version: "v3",
    p_nige: 0.25,
    p_oikomi: 0.25,
    p_sashi: 0.25,
    p_senkou: 0.25,
    predicted_at: "2026-06-07T11:00:00Z",
    predicted_label: "nige",
    race_key: "jra:20260607:08:01",
  });
  const fullBatch = Array.from({ length: 500 }, (_, i) => buildRow(i + 1));
  const all = vi
    .fn()
    .mockResolvedValueOnce({ results: fullBatch })
    .mockResolvedValueOnce({ results: [buildRow(501)] });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const put = vi.fn(async (_key: string, _body: Uint8Array, _options?: unknown) => undefined);
  const env = {
    FEATURES_ARCHIVE: { put } as unknown as R2Bucket,
    REALTIME_DB: { prepare } as unknown as D1Database,
  } as unknown as Env;
  const result = await exportRunningStyleParquetForDay({
    dateYmd: "20260607",
    env,
    source: "jra",
  });
  expect(result.rowCount).toBe(501);
  expect(all).toHaveBeenCalledTimes(2);
});
