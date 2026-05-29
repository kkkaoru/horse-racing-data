// Run with bun. Normalise raw Postgres rows into DailyRaceEntryRow shape.

import type { DailyRaceEntryRow } from "../types";

export const numericOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const stringOrNull = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return JSON.stringify(value);
};

const requireSource = (value: unknown): "jra" | "nar" => {
  if (value === "jra" || value === "nar") return value;
  throw new Error(`unexpected source value: ${String(value)}`);
};

export const normaliseDailyRaceEntryRow = (raw: Record<string, unknown>): DailyRaceEntryRow => ({
  babajotai_code_dirt: stringOrNull(raw.babajotai_code_dirt),
  babajotai_code_shiba: stringOrNull(raw.babajotai_code_shiba),
  bamei: stringOrNull(raw.bamei),
  banushimei: stringOrNull(raw.banushimei),
  barei: numericOrNull(raw.barei),
  bataiju: numericOrNull(raw.bataiju),
  chokyoshimei_ryakusho: stringOrNull(raw.chokyoshimei_ryakusho),
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
  grade_code: stringOrNull(raw.grade_code),
  hasso_jikoku: stringOrNull(raw.hasso_jikoku),
  juryo_shubetsu_code: stringOrNull(raw.juryo_shubetsu_code),
  kaisai_nen: String(raw.kaisai_nen),
  kaisai_tsukihi: String(raw.kaisai_tsukihi),
  keibajo_code: String(raw.keibajo_code),
  ketto_toroku_bango: String(raw.ketto_toroku_bango),
  kishumei_ryakusho: stringOrNull(raw.kishumei_ryakusho),
  kohan_3f: numericOrNull(raw.kohan_3f),
  kyori: numericOrNull(raw.kyori),
  kyoso_joken_code: stringOrNull(raw.kyoso_joken_code),
  kyoso_shubetsu_code: stringOrNull(raw.kyoso_shubetsu_code),
  race_bango: String(raw.race_bango),
  race_date: String(raw.race_date),
  race_name: stringOrNull(raw.race_name),
  seibetsu_code: stringOrNull(raw.seibetsu_code),
  shusso_tosu: numericOrNull(raw.shusso_tosu),
  soha_time: numericOrNull(raw.soha_time),
  source: requireSource(raw.source),
  tansho_ninkijun: numericOrNull(raw.tansho_ninkijun),
  tansho_odds: numericOrNull(raw.tansho_odds),
  time_sa: numericOrNull(raw.time_sa),
  track_code: stringOrNull(raw.track_code),
  umaban: numericOrNull(raw.umaban),
  wakuban: stringOrNull(raw.wakuban),
  zogen_fugo: stringOrNull(raw.zogen_fugo),
  zogen_sa: numericOrNull(raw.zogen_sa),
});

export const buildRaceKey = (row: DailyRaceEntryRow): string =>
  `${row.source}:${row.race_date}:${row.keibajo_code}:${row.race_bango}`;
