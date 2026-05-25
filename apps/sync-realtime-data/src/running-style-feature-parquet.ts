// Run with bun. Per-race running-style feature Parquet round-trip helpers for
// Workers. The persisted schema is flat: race identity columns plus one DOUBLE
// column per model feature.

import { Buffer } from "node:buffer";
import { Writable } from "node:stream";

import { ParquetReader, ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";

import { buildRunningStyleRaceKey, type RunningStyleRaceParams } from "./running-style-features";
import type { RaceHorseFeatureRow } from "./running-style-r2";

const METADATA_COLUMNS = [
  "raceKey",
  "source",
  "kaisaiNen",
  "kaisaiTsukihi",
  "keibajoCode",
  "raceBango",
  "category",
  "kettoTorokuBango",
  "bamei",
] as const;

const PEER_INPUT_FEATURES = {
  career_win_rate: "careerWinRate",
  kohan3f_avg_5: "kohan3fAvg5",
  past_corner_1_norm_avg_5: "pastCorner1NormAvg5",
  past_first_3f_avg_5: "pastFirst3fAvg5",
  past_nige_rate_self: "pastNigeRate",
  past_oikomi_rate_self: "pastOikomiRate",
  past_sashi_rate_self: "pastSashiRate",
  past_senkou_rate_self: "pastSenkouRate",
  speed_index_avg_5: "speedIndexAvg5",
  speed_index_best_5: "speedIndexBest5",
} as const;

class MemorySink extends Writable {
  readonly chunks: Buffer[] = [];

  override _write(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    this.chunks.push(Buffer.from(chunk));
    callback();
  }

  toBuffer(): Buffer {
    return Buffer.concat(this.chunks);
  }
}

const schemaForFeatureNames = (featureNames: ReadonlyArray<string>): ParquetSchema => {
  const fields: Record<string, Record<string, unknown>> = {
    raceKey: { type: "UTF8" },
    source: { type: "UTF8" },
    kaisaiNen: { type: "UTF8" },
    kaisaiTsukihi: { type: "UTF8" },
    keibajoCode: { type: "UTF8" },
    raceBango: { type: "UTF8" },
    category: { type: "UTF8" },
    kettoTorokuBango: { type: "UTF8" },
    umaban: { type: "INT32" },
    bamei: { optional: true, type: "UTF8" },
  };
  featureNames.forEach((name) => {
    fields[name] = { optional: true, type: "DOUBLE" };
  });
  return new ParquetSchema(fields);
};

// Parquet rows always come back as string/number/null. The type narrowing below
// is intentionally minimal; broader unknown handling lives in
// `running-style-feature-sql.ts` where DB drivers may emit bigint/Date/etc.
const toNumberOrNull = (value: unknown): number | null => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string" && value.length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const toStringOrNull = (value: unknown): string | null => {
  if (typeof value === "string") {
    return value.length > 0 ? value : null;
  }
  return null;
};

const toParquetRow = (
  row: RaceHorseFeatureRow,
  featureNames: ReadonlyArray<string>,
): Record<string, unknown> => {
  const output: Record<string, unknown> = {
    bamei: row.bamei,
    category: row.category,
    kaisaiNen: row.kaisaiNen,
    kaisaiTsukihi: row.kaisaiTsukihi,
    keibajoCode: row.keibajoCode,
    kettoTorokuBango: row.kettoTorokuBango,
    raceBango: row.raceBango,
    raceKey: row.raceKey,
    source: row.source,
    umaban: row.umaban,
  };
  featureNames.forEach((name) => {
    output[name] = row.perHorseFeatures[name] ?? null;
  });
  return output;
};

const fromParquetRow = (
  row: Record<string, unknown>,
  featureNames: ReadonlyArray<string>,
): RaceHorseFeatureRow => {
  const perHorseFeatures: Record<string, number | null> = {};
  featureNames.forEach((name) => {
    perHorseFeatures[name] = toNumberOrNull(row[name]);
  });
  const peerInputs = {} as RaceHorseFeatureRow["peerInputs"];
  Object.entries(PEER_INPUT_FEATURES).forEach(([featureName, peerName]) => {
    peerInputs[peerName] = perHorseFeatures[featureName] ?? null;
  });
  return {
    bamei: toStringOrNull(row.bamei),
    category: String(row.category),
    kaisaiNen: String(row.kaisaiNen),
    kaisaiTsukihi: String(row.kaisaiTsukihi),
    keibajoCode: String(row.keibajoCode),
    kettoTorokuBango: String(row.kettoTorokuBango),
    peerInputs,
    perHorseFeatures,
    raceBango: String(row.raceBango),
    raceKey: String(row.raceKey),
    source: String(row.source),
    umaban: toNumberOrNull(row.umaban) ?? 0,
  };
};

export const serializeRunningStyleFeatureParquet = async (
  rows: ReadonlyArray<RaceHorseFeatureRow>,
  featureNames: ReadonlyArray<string>,
): Promise<Uint8Array> => {
  const sink = new MemorySink();
  const writer = await ParquetWriter.openStream(schemaForFeatureNames(featureNames), sink, {
    useDataPageV2: false,
  });
  for (const row of rows) {
    await writer.appendRow(toParquetRow(row, featureNames));
  }
  await writer.close();
  return sink.toBuffer();
};

export const deserializeRunningStyleFeatureParquet = async (
  bytes: ArrayBuffer,
  featureNames: ReadonlyArray<string>,
): Promise<RaceHorseFeatureRow[]> => {
  const reader = await ParquetReader.openBuffer(Buffer.from(bytes));
  const cursor = reader.getCursor();
  const rows: RaceHorseFeatureRow[] = [];
  for (;;) {
    const row = await cursor.next();
    if (row === null) break;
    rows.push(fromParquetRow(row, featureNames));
  }
  await reader.close();
  return rows;
};

export const putRunningStyleFeatureParquet = async (
  bucket: R2Bucket,
  key: string,
  rows: ReadonlyArray<RaceHorseFeatureRow>,
  featureNames: ReadonlyArray<string>,
): Promise<number> => {
  const bytes = await serializeRunningStyleFeatureParquet(rows, featureNames);
  await bucket.put(key, bytes, {
    httpMetadata: { contentType: "application/vnd.apache.parquet" },
  });
  return bytes.byteLength;
};

export const loadRunningStyleFeatureParquet = async (
  bucket: R2Bucket,
  key: string,
  featureNames: ReadonlyArray<string>,
): Promise<RaceHorseFeatureRow[]> => {
  const object = await bucket.get(key);
  if (object === null) throw new Error(`R2 object not found: ${key}`);
  return deserializeRunningStyleFeatureParquet(await object.arrayBuffer(), featureNames);
};

export const validateFeatureCoverage = (
  rows: ReadonlyArray<RaceHorseFeatureRow>,
  featureNames: ReadonlyArray<string>,
): { missingCells: number; missingFeatureNames: string[] } => {
  const missingFeatureNames = featureNames.filter((name) =>
    rows.every((row) => !(name in row.perHorseFeatures)),
  );
  const missingCells = rows.reduce(
    (count, row) =>
      count + featureNames.filter((name) => row.perHorseFeatures[name] === undefined).length,
    0,
  );
  return { missingCells, missingFeatureNames };
};

export const runningStyleParquetVerificationKey = (
  source: string,
  raceDate: string,
  raceKey: string,
): string => `running-style/verification/features-parquet/${source}/${raceDate}/${raceKey}.parquet`;

export const buildRunningStyleFeatureParquetKey = (params: RunningStyleRaceParams): string =>
  `running-style/features-parquet/${params.source}/${params.kaisaiNen}${params.kaisaiTsukihi}/${buildRunningStyleRaceKey(params)}.parquet`;

export const metadataColumns = METADATA_COLUMNS;
