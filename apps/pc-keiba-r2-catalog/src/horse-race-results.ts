// Run with bun (bunx vitest).
// Domestic JV/NAR history only. Catalog TABLE_SPECS include oversea_* tables,
// but joining them here can exceed R2 SQL expression depth (error 40018).

import type {
  CatalogSource,
  HorseRaceResultRow,
  HorseRaceResultsFilters,
  HorseRaceResultsSourceScope,
  R2SqlCatalogConfig,
} from "./types";

interface HistoryTableSet {
  raceTable: "jvd_ra" | "nvd_ra";
  runnerTable: "jvd_se" | "nvd_se";
}

const IDENTIFIER_PATTERN: RegExp = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE_PATTERN: RegExp = /^\d{8}$/u;
const CODE_PATTERN: RegExp = /^\d{2}$/u;
const UNKNOWN_NAME: string = "不明";
const JRA_HISTORY: HistoryTableSet = { raceTable: "jvd_ra", runnerTable: "jvd_se" };
const NAR_HISTORY: HistoryTableSet = { raceTable: "nvd_ra", runnerTable: "nvd_se" };

const requireIdentifier = (value: string, label: string): string => {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`${label} must be an unquoted SQL identifier`);
  }
  return value;
};

const compactUtcDate = (timestamp: number): string =>
  new Date(timestamp).toISOString().slice(0, 10).replaceAll("-", "");

const requireDate = (date: string): string => {
  if (!DATE_PATTERN.test(date)) throw new Error("date must match YYYYMMDD");
  const timestamp = Date.UTC(
    Number(date.slice(0, 4)),
    Number(date.slice(4, 6)) - 1,
    Number(date.slice(6, 8)),
  );
  if (compactUtcDate(timestamp) !== date) throw new Error("date must be a valid calendar date");
  return date;
};

const requireCode = (value: string, label: string): string => {
  if (!CODE_PATTERN.test(value)) throw new Error(`${label} must contain two digits`);
  return value;
};

const tableName = (env: R2SqlCatalogConfig, table: string): string =>
  `${requireIdentifier(env.R2_SQL_NAMESPACE, "R2_SQL_NAMESPACE")}.${table}`;

const currentRunnerTable = (source: CatalogSource): "jvd_se" | "nvd_se" =>
  source === "jra" ? "jvd_se" : "nvd_se";

const historyTables = (sourceScope: HorseRaceResultsSourceScope): HistoryTableSet[] => {
  if (sourceScope === "jra") return [JRA_HISTORY];
  if (sourceScope === "nar") return [NAR_HISTORY];
  return [JRA_HISTORY, NAR_HISTORY];
};

const trimmedSql = (expr: string): string =>
  `nullif(btrim(replace(coalesce(${expr}, ''), chr(12288), '')), '')`;

const namedOrUnknownSql = (expr: string): string =>
  `coalesce(${trimmedSql(expr)}, '${UNKNOWN_NAME}')`;

const registeredHorseSql = (column: string): string =>
  `${trimmedSql(column)} IS NOT NULL
    AND replace(btrim(coalesce(${column}, '')), '0', '') <> ''`;

const currentRaceIdentitySql = (filters: HorseRaceResultsFilters): string =>
  `kaisai_nen = '${filters.date.slice(0, 4)}'
    AND kaisai_tsukihi = '${filters.date.slice(4)}'
    AND keibajo_code = '${filters.keibajoCode}'
    AND race_bango = '${filters.raceBango}'`;

const historySelectSql = (env: R2SqlCatalogConfig, tables: HistoryTableSet): string => `SELECT
    ch.current_jockey,
    ch.current_barei,
    ch.current_seibetsu_code,
    ch.current_umaban,
    ra.kaisai_nen,
    ra.kaisai_tsukihi,
    ra.keibajo_code,
    ra.race_bango,
    ra.kyosomei_hondai,
    ra.kyosomei_fukudai,
    ra.kyosomei_kakkonai,
    ra.grade_code,
    ra.kyoso_shubetsu_code,
    ra.kyoso_kigo_code,
    ra.juryo_shubetsu_code,
    ra.kyoso_joken_code,
    ra.kyoso_joken_meisho,
    ra.kyori,
    ra.track_code,
    ra.hasso_jikoku,
    ra.shusso_tosu,
    ra.tenko_code,
    ra.babajotai_code_shiba,
    ra.babajotai_code_dirt,
    se.wakuban,
    se.umaban,
    se.ketto_toroku_bango,
    se.bamei,
    se.seibetsu_code,
    se.barei,
    se.futan_juryo,
    se.kishumei_ryakusho,
    se.chokyoshimei_ryakusho,
    se.banushimei,
    se.bataiju,
    se.zogen_fugo,
    se.zogen_sa,
    se.kakutei_chakujun,
    se.tansho_odds,
    se.tansho_ninkijun,
    se.soha_time,
    se.time_sa,
    se.corner_1,
    se.corner_2,
    se.corner_3,
    se.corner_4,
    se.kohan_3f,
    se.blinker_shiyo_kubun
  FROM ${tableName(env, tables.runnerTable)} se
  INNER JOIN ${tableName(env, tables.raceTable)} ra
    ON ra.kaisai_nen = se.kaisai_nen
    AND ra.kaisai_tsukihi = se.kaisai_tsukihi
    AND ra.keibajo_code = se.keibajo_code
    AND ra.race_bango = se.race_bango
  INNER JOIN current_horses ch
    ON ch.ketto_toroku_bango = se.ketto_toroku_bango`;

export const buildHorseRaceResultsQuery = (
  env: R2SqlCatalogConfig,
  filters: HorseRaceResultsFilters,
): string => {
  const date = requireDate(filters.date);
  const keibajoCode = requireCode(filters.keibajoCode, "keibajoCode");
  const raceBango = requireCode(filters.raceBango, "raceBango");
  const checked: HorseRaceResultsFilters = {
    date,
    keibajoCode,
    raceBango,
    source: filters.source,
    sourceScope: filters.sourceScope,
  };
  const historySql = historyTables(checked.sourceScope)
    .map((tables) => historySelectSql(env, tables))
    .join("\n  UNION ALL\n  ");
  return `
WITH current_horses AS (
  SELECT
    lpad(btrim(coalesce(umaban, '')), 2, '0') AS current_umaban,
    ketto_toroku_bango,
    seibetsu_code AS current_seibetsu_code,
    barei AS current_barei,
    ${namedOrUnknownSql("kishumei_ryakusho")} AS current_jockey
  FROM ${tableName(env, currentRunnerTable(checked.source))}
  WHERE ${currentRaceIdentitySql(checked)}
    AND ${registeredHorseSql("ketto_toroku_bango")}
),
history AS (
  ${historySql}
)
SELECT
  current_jockey,
  current_barei,
  current_seibetsu_code,
  current_umaban,
  kaisai_nen,
  kaisai_tsukihi,
  keibajo_code,
  race_bango,
  kyosomei_hondai,
  kyosomei_fukudai,
  kyosomei_kakkonai,
  grade_code,
  kyoso_shubetsu_code,
  kyoso_kigo_code,
  juryo_shubetsu_code,
  kyoso_joken_code,
  kyoso_joken_meisho,
  kyori,
  track_code,
  hasso_jikoku,
  shusso_tosu,
  tenko_code,
  babajotai_code_shiba,
  babajotai_code_dirt,
  wakuban,
  umaban,
  ketto_toroku_bango,
  bamei,
  seibetsu_code,
  barei,
  futan_juryo,
  kishumei_ryakusho,
  chokyoshimei_ryakusho,
  banushimei,
  bataiju,
  zogen_fugo,
  zogen_sa,
  kakutei_chakujun,
  tansho_odds,
  tansho_ninkijun,
  soha_time,
  time_sa,
  corner_1,
  corner_2,
  corner_3,
  corner_4,
  kohan_3f,
  blinker_shiyo_kubun
FROM history
WHERE concat(kaisai_nen, kaisai_tsukihi) < '${date}'
ORDER BY try_cast(current_umaban AS INT), kaisai_nen DESC, kaisai_tsukihi DESC, race_bango DESC`;
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

export const normaliseHorseRaceResultRow = (raw: Record<string, unknown>): HorseRaceResultRow => ({
  babajotaiCodeDirt: stringOrNull(raw.babajotai_code_dirt),
  babajotaiCodeShiba: stringOrNull(raw.babajotai_code_shiba),
  bamei: stringOrNull(raw.bamei),
  banushimei: stringOrNull(raw.banushimei),
  barei: stringOrNull(raw.barei),
  bataiju: stringOrNull(raw.bataiju),
  blinkerShiyoKubun: stringOrNull(raw.blinker_shiyo_kubun),
  chokyoshimeiRyakusho: stringOrNull(raw.chokyoshimei_ryakusho),
  corner1: stringOrNull(raw.corner_1),
  corner2: stringOrNull(raw.corner_2),
  corner3: stringOrNull(raw.corner_3),
  corner4: stringOrNull(raw.corner_4),
  currentBarei: stringOrNull(raw.current_barei),
  currentJockey: stringOrNull(raw.current_jockey),
  currentSeibetsuCode: stringOrNull(raw.current_seibetsu_code),
  currentUmaban: stringOrNull(raw.current_umaban),
  futanJuryo: stringOrNull(raw.futan_juryo),
  gradeCode: stringOrNull(raw.grade_code),
  hassoJikoku: stringOrNull(raw.hasso_jikoku),
  juryoShubetsuCode: stringOrNull(raw.juryo_shubetsu_code),
  kaisaiNen: requiredString(raw.kaisai_nen, "kaisai_nen"),
  kaisaiTsukihi: requiredString(raw.kaisai_tsukihi, "kaisai_tsukihi"),
  kakuteiChakujun: stringOrNull(raw.kakutei_chakujun),
  keibajoCode: requiredString(raw.keibajo_code, "keibajo_code"),
  kettoTorokuBango: stringOrNull(raw.ketto_toroku_bango),
  kishumeiRyakusho: stringOrNull(raw.kishumei_ryakusho),
  kohan3f: stringOrNull(raw.kohan_3f),
  kyori: stringOrNull(raw.kyori),
  kyosoJokenCode: stringOrNull(raw.kyoso_joken_code),
  kyosoJokenMeisho: stringOrNull(raw.kyoso_joken_meisho),
  kyosoKigoCode: stringOrNull(raw.kyoso_kigo_code),
  kyosomeiFukudai: stringOrNull(raw.kyosomei_fukudai),
  kyosomeiHondai: stringOrNull(raw.kyosomei_hondai),
  kyosomeiKakkonai: stringOrNull(raw.kyosomei_kakkonai),
  kyosoShubetsuCode: stringOrNull(raw.kyoso_shubetsu_code),
  raceBango: requiredString(raw.race_bango, "race_bango"),
  seibetsuCode: stringOrNull(raw.seibetsu_code),
  shussoTosu: stringOrNull(raw.shusso_tosu),
  sohaTime: stringOrNull(raw.soha_time),
  tanshoNinkijun: stringOrNull(raw.tansho_ninkijun),
  tanshoOdds: stringOrNull(raw.tansho_odds),
  tenkoCode: stringOrNull(raw.tenko_code),
  timeSa: stringOrNull(raw.time_sa),
  trackCode: stringOrNull(raw.track_code),
  umaban: stringOrNull(raw.umaban),
  wakuban: stringOrNull(raw.wakuban),
  zogenFugo: stringOrNull(raw.zogen_fugo),
  zogenSa: stringOrNull(raw.zogen_sa),
});

export const uniqueHorseRaceResults = (
  rows: ReadonlyArray<HorseRaceResultRow>,
): HorseRaceResultRow[] => {
  const seen = new Set<string>();
  return rows.filter((row) => {
    const key = [
      row.currentUmaban,
      row.kaisaiNen,
      row.kaisaiTsukihi,
      row.keibajoCode,
      row.raceBango,
      row.kettoTorokuBango,
    ].join("-");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};
