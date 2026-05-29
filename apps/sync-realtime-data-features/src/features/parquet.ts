// Run with bun. Parquet encode (via @dsnp/parquetjs) + decode (via hyparquet).
// Schema mirrors all 45 DailyRaceEntryRow fields so the per-race file is the
// canonical source of truth (no D1 daily_race_entries dependency).

import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import { parquetReadObjects } from "hyparquet";
import { Writable } from "node:stream";

import { numericOrNull } from "./normalise";
import type { DailyRaceEntryRow } from "../types";

const utf8Optional = { optional: true, type: "UTF8" } as const;
const utf8Required = { type: "UTF8" } as const;
const int32Optional = { optional: true, type: "INT32" } as const;
const doubleOptional = { optional: true, type: "DOUBLE" } as const;

const RACE_FEATURES_SCHEMA_DEFINITION: Record<string, Record<string, unknown>> = {
  source: utf8Required,
  race_date: utf8Required,
  kaisai_nen: utf8Required,
  kaisai_tsukihi: utf8Required,
  keibajo_code: utf8Required,
  race_bango: utf8Required,
  ketto_toroku_bango: utf8Required,
  wakuban: utf8Optional,
  umaban: int32Optional,
  bamei: utf8Optional,
  race_name: utf8Optional,
  hasso_jikoku: utf8Optional,
  track_code: utf8Optional,
  grade_code: utf8Optional,
  kyoso_shubetsu_code: utf8Optional,
  juryo_shubetsu_code: utf8Optional,
  kyoso_joken_code: utf8Optional,
  babajotai_code_shiba: utf8Optional,
  babajotai_code_dirt: utf8Optional,
  kyori: int32Optional,
  shusso_tosu: int32Optional,
  seibetsu_code: utf8Optional,
  barei: int32Optional,
  futan_juryo: doubleOptional,
  kishumei_ryakusho: utf8Optional,
  chokyoshimei_ryakusho: utf8Optional,
  banushimei: utf8Optional,
  finish_position: int32Optional,
  finish_norm: doubleOptional,
  tansho_ninkijun: int32Optional,
  tansho_odds: doubleOptional,
  soha_time: int32Optional,
  time_sa: doubleOptional,
  kohan_3f: doubleOptional,
  corner1_norm: doubleOptional,
  corner2_norm: doubleOptional,
  corner3_norm: doubleOptional,
  corner4_norm: doubleOptional,
  corner_1: int32Optional,
  corner_2: int32Optional,
  corner_3: int32Optional,
  corner_4: int32Optional,
  bataiju: int32Optional,
  zogen_fugo: utf8Optional,
  zogen_sa: int32Optional,
};

const buildSchema = (): ParquetSchema => new ParquetSchema(RACE_FEATURES_SCHEMA_DEFINITION);

const toParquetRow = (row: DailyRaceEntryRow): Record<string, unknown> => ({ ...row });

class BufferCollector extends Writable {
  chunks: Uint8Array[] = [];
  override _write(chunk: Uint8Array, _enc: string, cb: () => void): void {
    this.chunks.push(chunk);
    cb();
  }
  toBytes(): Uint8Array {
    const total = this.chunks.reduce((sum, c) => sum + c.length, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const c of this.chunks) {
      out.set(c, offset);
      offset += c.length;
    }
    return out;
  }
}

export const encodeRaceFeaturesParquet = async (rows: DailyRaceEntryRow[]): Promise<Uint8Array> => {
  const collector = new BufferCollector();
  const writer = await ParquetWriter.openStream(buildSchema(), collector);
  for (const row of rows) {
    await writer.appendRow(toParquetRow(row));
  }
  await writer.close();
  return collector.toBytes();
};

const fromParquetRow = (raw: Record<string, unknown>): DailyRaceEntryRow => {
  const source = raw.source === "jra" || raw.source === "nar" ? raw.source : "nar";
  return {
    babajotai_code_dirt: (raw.babajotai_code_dirt as string | null) ?? null,
    babajotai_code_shiba: (raw.babajotai_code_shiba as string | null) ?? null,
    bamei: (raw.bamei as string | null) ?? null,
    banushimei: (raw.banushimei as string | null) ?? null,
    barei: numericOrNull(raw.barei),
    bataiju: numericOrNull(raw.bataiju),
    chokyoshimei_ryakusho: (raw.chokyoshimei_ryakusho as string | null) ?? null,
    corner1_norm: numericOrNull(raw.corner1_norm),
    corner2_norm: numericOrNull(raw.corner2_norm),
    corner3_norm: numericOrNull(raw.corner3_norm),
    corner4_norm: numericOrNull(raw.corner4_norm),
    corner_1: numericOrNull(raw.corner_1),
    corner_2: numericOrNull(raw.corner_2),
    corner_3: numericOrNull(raw.corner_3),
    corner_4: numericOrNull(raw.corner_4),
    finish_norm: numericOrNull(raw.finish_norm),
    finish_position: numericOrNull(raw.finish_position),
    futan_juryo: numericOrNull(raw.futan_juryo),
    grade_code: (raw.grade_code as string | null) ?? null,
    hasso_jikoku: (raw.hasso_jikoku as string | null) ?? null,
    juryo_shubetsu_code: (raw.juryo_shubetsu_code as string | null) ?? null,
    kaisai_nen: String(raw.kaisai_nen),
    kaisai_tsukihi: String(raw.kaisai_tsukihi),
    keibajo_code: String(raw.keibajo_code),
    ketto_toroku_bango: String(raw.ketto_toroku_bango),
    kishumei_ryakusho: (raw.kishumei_ryakusho as string | null) ?? null,
    kohan_3f: numericOrNull(raw.kohan_3f),
    kyori: numericOrNull(raw.kyori),
    kyoso_joken_code: (raw.kyoso_joken_code as string | null) ?? null,
    kyoso_shubetsu_code: (raw.kyoso_shubetsu_code as string | null) ?? null,
    race_bango: String(raw.race_bango),
    race_date: String(raw.race_date),
    race_name: (raw.race_name as string | null) ?? null,
    seibetsu_code: (raw.seibetsu_code as string | null) ?? null,
    shusso_tosu: numericOrNull(raw.shusso_tosu),
    soha_time: numericOrNull(raw.soha_time),
    source,
    tansho_ninkijun: numericOrNull(raw.tansho_ninkijun),
    tansho_odds: numericOrNull(raw.tansho_odds),
    time_sa: numericOrNull(raw.time_sa),
    track_code: (raw.track_code as string | null) ?? null,
    umaban: numericOrNull(raw.umaban),
    wakuban: (raw.wakuban as string | null) ?? null,
    zogen_fugo: (raw.zogen_fugo as string | null) ?? null,
    zogen_sa: numericOrNull(raw.zogen_sa),
  };
};

export const decodeRaceFeaturesParquet = async (
  bytes: Uint8Array,
): Promise<DailyRaceEntryRow[]> => {
  const buffer = bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  ) as ArrayBuffer;
  const rows = await parquetReadObjects({ file: buffer });
  return rows.map(fromParquetRow);
};
