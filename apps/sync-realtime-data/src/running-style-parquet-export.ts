// Run with bun. Per-day per-source running-style prediction Parquet export to
// the shared `pc-keiba-features-archive` R2 bucket. The container-side
// finish-position pipeline reads these files via DuckDB httpfs instead of
// going back to Neon Postgres, removing the Hyperdrive round-trip cost.
//
// Object layout (one Parquet per model_version per day per source):
//   running-style/predictions/by-day/{YYYY}/{MM}/{DD}/{source}/{model_version}.parquet
//
// Schema (DuckDB-compatible). predicted_at is encoded as a UTF8 ISO timestamp
// string because @dsnp/parquetjs lacks a stable TIMESTAMP encoder under the
// Workers runtime; DuckDB / pyarrow can cast it back via `CAST(predicted_at AS
// TIMESTAMP)` on read.

import { Writable } from "node:stream";

import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";

import type { RunningStyleClassLabel } from "./running-style-lightgbm-tree";
import type { Env } from "./types";

export interface ExportRunningStyleParquetParams {
  env: Env;
  source: "jra" | "nar";
  dateYmd: string;
}

export interface ExportRunningStyleParquetResult {
  bytesWritten: number;
  fileCount: number;
  keys: string[];
  rowCount: number;
  skipped: boolean;
  skippedReason?: string;
}

export interface RunningStylePredictionExportRow {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  umaban: number;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  predicted_label: RunningStyleClassLabel;
  predicted_class: number;
  model_version: string;
  predicted_at: string;
}

interface D1PredictionRow {
  race_key: string;
  horse_number: number;
  ketto_toroku_bango: string;
  kaisai_nen: string;
  model_version: string;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  predicted_label: RunningStyleClassLabel;
  predicted_at: string;
}

const RUNNING_STYLE_LABEL_INDEX: Record<RunningStyleClassLabel, number> = {
  nige: 0,
  oikomi: 3,
  sashi: 2,
  senkou: 1,
};

const RACE_KEY_DATE_SLICE_YEAR_END = 4;
const RACE_KEY_DATE_SLICE_MONTH_END = 6;
const RACE_KEY_DATE_SLICE_DAY_END = 8;
const RACE_KEY_PART_DATE_INDEX = 1;
const RACE_KEY_PART_KEIBAJO_INDEX = 2;
const RACE_KEY_PART_RACE_BANGO_INDEX = 3;
const D1_SELECT_BATCH_SIZE = 500;

const PARQUET_SCHEMA_DEFINITION: Record<string, Record<string, unknown>> = {
  source: { type: "UTF8" },
  kaisai_nen: { type: "UTF8" },
  kaisai_tsukihi: { type: "UTF8" },
  keibajo_code: { type: "UTF8" },
  race_bango: { type: "UTF8" },
  ketto_toroku_bango: { type: "UTF8" },
  umaban: { type: "INT32" },
  p_nige: { type: "DOUBLE" },
  p_senkou: { type: "DOUBLE" },
  p_sashi: { type: "DOUBLE" },
  p_oikomi: { type: "DOUBLE" },
  predicted_label: { type: "UTF8" },
  predicted_class: { type: "INT32" },
  model_version: { type: "UTF8" },
  predicted_at: { type: "UTF8" },
};

class MemorySink extends Writable {
  readonly chunks: Uint8Array[] = [];

  override _write(
    chunk: Uint8Array,
    _encoding: string,
    callback: (error?: Error | null) => void,
  ): void {
    this.chunks.push(new Uint8Array(chunk));
    callback();
  }

  toBytes(): Uint8Array {
    const total = this.chunks.reduce((sum, c) => sum + c.length, 0);
    const out = new Uint8Array(total);
    this.chunks.reduce((offset, chunk) => {
      out.set(chunk, offset);
      return offset + chunk.length;
    }, 0);
    return out;
  }
}

export const buildRunningStyleDayParquetKey = (params: {
  source: "jra" | "nar";
  dateYmd: string;
  modelVersion: string;
}): string => {
  const year = params.dateYmd.slice(0, RACE_KEY_DATE_SLICE_YEAR_END);
  const month = params.dateYmd.slice(RACE_KEY_DATE_SLICE_YEAR_END, RACE_KEY_DATE_SLICE_MONTH_END);
  const day = params.dateYmd.slice(RACE_KEY_DATE_SLICE_MONTH_END, RACE_KEY_DATE_SLICE_DAY_END);
  return `running-style/predictions/by-day/${year}/${month}/${day}/${params.source}/${params.modelVersion}.parquet`;
};

const labelToClassIndex = (label: RunningStyleClassLabel): number =>
  RUNNING_STYLE_LABEL_INDEX[label];

const parseRaceKeyParts = (
  raceKey: string,
): { keibajoCode: string; raceBango: string; kaisaiTsukihi: string } | null => {
  const parts = raceKey.split(":");
  const datePart = parts[RACE_KEY_PART_DATE_INDEX];
  const keibajo = parts[RACE_KEY_PART_KEIBAJO_INDEX];
  const raceBango = parts[RACE_KEY_PART_RACE_BANGO_INDEX];
  if (
    datePart === undefined ||
    datePart.length < RACE_KEY_DATE_SLICE_DAY_END ||
    keibajo === undefined ||
    raceBango === undefined
  ) {
    return null;
  }
  return {
    kaisaiTsukihi: datePart.slice(RACE_KEY_DATE_SLICE_YEAR_END, RACE_KEY_DATE_SLICE_DAY_END),
    keibajoCode: keibajo,
    raceBango,
  };
};

const toExportRow = (
  row: D1PredictionRow,
  source: "jra" | "nar",
): RunningStylePredictionExportRow | null => {
  const parsed = parseRaceKeyParts(row.race_key);
  if (parsed === null) return null;
  return {
    kaisai_nen: row.kaisai_nen,
    kaisai_tsukihi: parsed.kaisaiTsukihi,
    keibajo_code: parsed.keibajoCode,
    ketto_toroku_bango: row.ketto_toroku_bango,
    model_version: row.model_version,
    p_nige: Number(row.p_nige),
    p_oikomi: Number(row.p_oikomi),
    p_sashi: Number(row.p_sashi),
    p_senkou: Number(row.p_senkou),
    predicted_at: row.predicted_at,
    predicted_class: labelToClassIndex(row.predicted_label),
    predicted_label: row.predicted_label,
    race_bango: parsed.raceBango,
    source,
    umaban: Number(row.horse_number),
  };
};

const groupByModelVersion = (
  rows: ReadonlyArray<RunningStylePredictionExportRow>,
): Map<string, RunningStylePredictionExportRow[]> => {
  const grouped = new Map<string, RunningStylePredictionExportRow[]>();
  rows.forEach((row) => {
    const bucket = grouped.get(row.model_version);
    if (bucket === undefined) {
      grouped.set(row.model_version, [row]);
      return;
    }
    bucket.push(row);
  });
  return grouped;
};

const compareByRaceThenUmaban = (
  left: RunningStylePredictionExportRow,
  right: RunningStylePredictionExportRow,
): number => {
  if (left.keibajo_code !== right.keibajo_code) {
    return left.keibajo_code < right.keibajo_code ? -1 : 1;
  }
  if (left.race_bango !== right.race_bango) {
    return left.race_bango < right.race_bango ? -1 : 1;
  }
  return left.umaban - right.umaban;
};

export const serializeRunningStylePredictionParquet = async (
  rows: ReadonlyArray<RunningStylePredictionExportRow>,
): Promise<Uint8Array> => {
  const sink = new MemorySink();
  const writer = await ParquetWriter.openStream(
    new ParquetSchema(PARQUET_SCHEMA_DEFINITION),
    sink,
    { useDataPageV2: false },
  );
  for (const row of rows) {
    await writer.appendRow({ ...row });
  }
  await writer.close();
  return sink.toBytes();
};

const fetchPredictionRowsBatch = async (
  db: D1Database,
  prefix: string,
  offset: number,
): Promise<D1PredictionRow[]> => {
  const result = await db
    .prepare(
      `select race_key, horse_number, ketto_toroku_bango, kaisai_nen,
              model_version, p_nige, p_senkou, p_sashi, p_oikomi,
              predicted_label, predicted_at
         from race_running_styles
        where race_key like ?
        order by race_key, horse_number
        limit ? offset ?`,
    )
    .bind(`${prefix}%`, D1_SELECT_BATCH_SIZE, offset)
    .all<D1PredictionRow>();
  return result.results;
};

const queryPredictionRowsForDay = async (
  db: D1Database,
  source: "jra" | "nar",
  dateYmd: string,
): Promise<D1PredictionRow[]> => {
  const prefix = `${source}:${dateYmd}:`;
  const collectPage = async (
    pageOffset: number,
    acc: D1PredictionRow[],
  ): Promise<D1PredictionRow[]> => {
    const batch = await fetchPredictionRowsBatch(db, prefix, pageOffset);
    if (batch.length === 0) return acc;
    const next = acc.concat(batch);
    if (batch.length < D1_SELECT_BATCH_SIZE) return next;
    return collectPage(pageOffset + D1_SELECT_BATCH_SIZE, next);
  };
  return collectPage(0, []);
};

export const exportRunningStyleParquetForDay = async (
  params: ExportRunningStyleParquetParams,
): Promise<ExportRunningStyleParquetResult> => {
  const bucket = params.env.FEATURES_ARCHIVE;
  if (bucket === undefined) {
    return {
      bytesWritten: 0,
      fileCount: 0,
      keys: [],
      rowCount: 0,
      skipped: true,
      skippedReason: "FEATURES_ARCHIVE binding not configured",
    };
  }
  const rawRows = await queryPredictionRowsForDay(
    params.env.REALTIME_DB,
    params.source,
    params.dateYmd,
  );
  if (rawRows.length === 0) {
    return {
      bytesWritten: 0,
      fileCount: 0,
      keys: [],
      rowCount: 0,
      skipped: true,
      skippedReason: "no rows for day",
    };
  }
  const exportRows = rawRows
    .map((row) => toExportRow(row, params.source))
    .filter((row): row is RunningStylePredictionExportRow => row !== null);
  const grouped = groupByModelVersion(exportRows);
  const writeOne = async (
    entry: [string, RunningStylePredictionExportRow[]],
  ): Promise<{ key: string; size: number }> => {
    const [modelVersion, group] = entry;
    const sorted = [...group].sort(compareByRaceThenUmaban);
    const bytes = await serializeRunningStylePredictionParquet(sorted);
    const key = buildRunningStyleDayParquetKey({
      dateYmd: params.dateYmd,
      modelVersion,
      source: params.source,
    });
    await bucket.put(key, bytes, {
      httpMetadata: { contentType: "application/vnd.apache.parquet" },
    });
    return { key, size: bytes.byteLength };
  };
  const writes = await Promise.all([...grouped.entries()].map(writeOne));
  return {
    bytesWritten: writes.reduce((sum, w) => sum + w.size, 0),
    fileCount: writes.length,
    keys: writes.map((w) => w.key),
    rowCount: exportRows.length,
    skipped: false,
  };
};
