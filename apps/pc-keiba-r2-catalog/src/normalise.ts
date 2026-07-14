import type { CatalogRaceKeyRow, CatalogSource, DailyRaceEntryRow } from "./types";

const numericOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value !== "string" || value.trim().length === 0) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const stringOrNull = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return JSON.stringify(value);
};

const requiredString = (value: unknown, field: string): string => {
  const normalised = stringOrNull(value);
  if (normalised === null || normalised.length === 0) {
    throw new Error(`R2 SQL row is missing ${field}`);
  }
  return normalised;
};

const normaliseSource = (value: unknown): CatalogSource => {
  if (value === "jra" || value === "nar") return value;
  throw new Error(`R2 SQL row has invalid source: ${String(value)}`);
};

const normaliseRaceDate = (value: unknown): string => {
  const date = requiredString(value, "race_date");
  if (/^\d{8}$/u.test(date)) return date;
  const compact = date.slice(0, 10).replaceAll("-", "");
  if (/^\d{8}$/u.test(compact)) return compact;
  throw new Error(`R2 SQL row has invalid race_date: ${date}`);
};

const paddedCode = (value: unknown, field: string): string =>
  requiredString(value, field).padStart(2, "0");

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
  kaisai_nen: requiredString(raw.kaisai_nen, "kaisai_nen"),
  kaisai_tsukihi: requiredString(raw.kaisai_tsukihi, "kaisai_tsukihi"),
  keibajo_code: paddedCode(raw.keibajo_code, "keibajo_code"),
  ketto_toroku_bango: requiredString(raw.ketto_toroku_bango, "ketto_toroku_bango"),
  kishumei_ryakusho: stringOrNull(raw.kishumei_ryakusho),
  kohan_3f: numericOrNull(raw.kohan_3f),
  kyori: numericOrNull(raw.kyori),
  kyoso_joken_code: stringOrNull(raw.kyoso_joken_code),
  kyoso_shubetsu_code: stringOrNull(raw.kyoso_shubetsu_code),
  race_bango: paddedCode(raw.race_bango, "race_bango"),
  race_date: normaliseRaceDate(raw.race_date),
  race_name: stringOrNull(raw.race_name),
  seibetsu_code: stringOrNull(raw.seibetsu_code),
  shusso_tosu: numericOrNull(raw.shusso_tosu),
  soha_time: numericOrNull(raw.soha_time),
  source: normaliseSource(raw.source),
  tansho_ninkijun: numericOrNull(raw.tansho_ninkijun),
  tansho_odds: numericOrNull(raw.tansho_odds),
  time_sa: numericOrNull(raw.time_sa),
  track_code: stringOrNull(raw.track_code),
  umaban: numericOrNull(raw.umaban),
  wakuban: stringOrNull(raw.wakuban),
  zogen_fugo: stringOrNull(raw.zogen_fugo),
  zogen_sa: numericOrNull(raw.zogen_sa),
});

export const normaliseCatalogRaceKeyRow = (raw: Record<string, unknown>): CatalogRaceKeyRow => {
  const source = normaliseSource(raw.source);
  const date = normaliseRaceDate(raw.race_date);
  return {
    kaisai_nen: date.slice(0, 4),
    kaisai_tsukihi: date.slice(4, 8),
    keibajo_code: paddedCode(raw.keibajo_code, "keibajo_code"),
    race_bango: paddedCode(raw.race_bango, "race_bango"),
    source,
  };
};
