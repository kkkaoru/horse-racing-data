import { runningStyleFeatureCtesSql } from "./running-style-feature-ctes";
import type {
  R2SqlCatalogConfig,
  RunningStyleFeatureFilters,
  RunningStyleSourceScope,
} from "./types";

const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE_PATTERN = /^\d{8}$/u;
const CODE_PATTERN = /^\d{2}$/u;

interface RunningStyleRawSource {
  masterTable: "jvd_um" | "nvd_um";
  raceTable: "jvd_ra" | "nvd_ra";
  runnerTable: "jvd_se" | "nvd_se";
  rowSource: "jra" | "nar";
  scope: RunningStyleSourceScope;
}

const JRA_SOURCE: RunningStyleRawSource = {
  masterTable: "jvd_um",
  raceTable: "jvd_ra",
  rowSource: "jra",
  runnerTable: "jvd_se",
  scope: "jra",
};

const NAR_SOURCE: RunningStyleRawSource = {
  masterTable: "nvd_um",
  raceTable: "nvd_ra",
  rowSource: "nar",
  runnerTable: "nvd_se",
  scope: "nar",
};

const BAN_EI_SOURCE: RunningStyleRawSource = {
  masterTable: "nvd_um",
  raceTable: "nvd_ra",
  rowSource: "nar",
  runnerTable: "nvd_se",
  scope: "ban-ei",
};

const sourceConfig = (source: RunningStyleSourceScope): RunningStyleRawSource => {
  if (source === "jra") return JRA_SOURCE;
  if (source === "nar") return NAR_SOURCE;
  return BAN_EI_SOURCE;
};

const requireIdentifier = (value: string): string => {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  }
  return value;
};

const validateFilters = (filters: RunningStyleFeatureFilters): void => {
  if (!DATE_PATTERN.test(filters.date)) throw new Error("date must match YYYYMMDD");
  if (!CODE_PATTERN.test(filters.keibajoCode)) {
    throw new Error("keibajoCode must contain two digits");
  }
  if (!CODE_PATTERN.test(filters.raceBango)) {
    throw new Error("raceBango must contain two digits");
  }
  if (filters.source === "ban-ei" && filters.keibajoCode !== "83") {
    throw new Error("keibajoCode must be 83 for ban-ei");
  }
  if (filters.source === "nar" && filters.keibajoCode === "83") {
    throw new Error("source must be ban-ei for keibajoCode 83");
  }
};

const tableName = (env: R2SqlCatalogConfig, table: string): string =>
  `${requireIdentifier(env.R2_SQL_NAMESPACE)}.${table}`;

const historyStart = (date: string): string =>
  `${String(Number(date.slice(0, 4)) - 10).padStart(4, "0")}${date.slice(4)}`;

const venuePredicate = (source: RunningStyleRawSource): string => {
  if (source.scope === "nar") return "keibajo_code <> '83'";
  if (source.scope === "ban-ei") return "keibajo_code = '83'";
  return "1 = 1";
};

const historyPredicates = (
  filters: RunningStyleFeatureFilters,
  source: RunningStyleRawSource,
): string => {
  const start = historyStart(filters.date);
  return `kaisai_nen >= '${start.slice(0, 4)}'
    AND kaisai_nen <= '${filters.date.slice(0, 4)}'
    AND concat(kaisai_nen, kaisai_tsukihi) >= '${start}'
    AND concat(kaisai_nen, kaisai_tsukihi) <= '${filters.date}'
    AND ${venuePredicate(source)}`;
};

const targetPredicates = (
  filters: RunningStyleFeatureFilters,
  source: RunningStyleRawSource,
): string => `kaisai_nen = '${filters.date.slice(0, 4)}'
    AND kaisai_tsukihi = '${filters.date.slice(4)}'
    AND keibajo_code = '${filters.keibajoCode}'
    AND race_bango = '${filters.raceBango}'
    AND ${venuePredicate(source)}`;

const runnerColumns = `
    kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    ketto_toroku_bango, umaban, bamei, kishumei_ryakusho,
    chokyoshimei_ryakusho, kakutei_chakujun, tansho_ninkijun,
    tansho_odds, time_sa, kohan_3f, corner_1, corner_2,
    corner_3, corner_4, bataiju`;

const raceColumns = `
    kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    kyori, track_code, grade_code, kyoso_joken_code, shusso_tosu,
    babajotai_code_shiba, babajotai_code_dirt, zenhan_3f, tenko_code`;

const normalisedRecSelect = (
  source: RunningStyleRawSource,
  runnerCte: string,
  raceCte: string,
): string => `SELECT
    '${source.rowSource}' AS source,
    concat(se.kaisai_nen, se.kaisai_tsukihi) AS race_date,
    floor(
      to_unixtime(
        to_timestamp(
          concat(se.kaisai_nen, se.kaisai_tsukihi, ' 00:00:00'),
          '%Y%m%d %H:%M:%S'
        )
      ) / 86400
    ) AS race_dt,
    se.kaisai_nen,
    se.kaisai_tsukihi,
    lpad(se.keibajo_code, 2, '0') AS keibajo_code,
    lpad(se.race_bango, 2, '0') AS race_bango,
    se.ketto_toroku_bango,
    try_cast(nullif(se.umaban, '') AS INT) AS umaban,
    se.bamei,
    se.kishumei_ryakusho,
    se.chokyoshimei_ryakusho,
    try_cast(nullif(ra.kyori, '') AS INT) AS kyori,
    ra.track_code,
    ra.grade_code,
    ra.kyoso_joken_code,
    try_cast(nullif(ra.shusso_tosu, '00') AS INT) AS shusso_tosu,
    try_cast(nullif(se.kakutei_chakujun, '00') AS INT) AS finish_position,
    CASE
      WHEN try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) > 1
        AND try_cast(nullif(se.kakutei_chakujun, '00') AS DOUBLE) IS NOT NULL
      THEN (try_cast(nullif(se.kakutei_chakujun, '00') AS DOUBLE) - 1)
        / (try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) - 1)
      ELSE NULL
    END AS finish_norm,
    try_cast(nullif(se.time_sa, '0000') AS DOUBLE) / 10.0 AS time_sa,
    try_cast(nullif(se.kohan_3f, '000') AS DOUBLE) / 10.0 AS kohan_3f,
    CASE WHEN try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) > 1
      THEN (try_cast(nullif(se.corner_1, '00') AS DOUBLE) - 1)
        / (try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) - 1) ELSE NULL END AS corner1_norm,
    CASE WHEN try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) > 1
      THEN (try_cast(nullif(se.corner_2, '00') AS DOUBLE) - 1)
        / (try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) - 1) ELSE NULL END AS corner2_norm,
    CASE WHEN try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) > 1
      THEN (try_cast(nullif(se.corner_3, '00') AS DOUBLE) - 1)
        / (try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) - 1) ELSE NULL END AS corner3_norm,
    CASE WHEN try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) > 1
      THEN (try_cast(nullif(se.corner_4, '00') AS DOUBLE) - 1)
        / (try_cast(nullif(ra.shusso_tosu, '00') AS DOUBLE) - 1) ELSE NULL END AS corner4_norm,
    ra.babajotai_code_shiba,
    ra.babajotai_code_dirt,
    try_cast(nullif(se.tansho_ninkijun, '00') AS INT) AS tansho_ninkijun,
    try_cast(nullif(se.tansho_odds, '0000') AS DOUBLE) / 10.0 AS tansho_odds,
    try_cast(nullif(se.bataiju, '000') AS INT) AS bataiju,
    try_cast(nullif(ra.zenhan_3f, '000') AS DOUBLE) / 10.0 AS zenhan_3f,
    ra.tenko_code
  FROM ${runnerCte} se
  INNER JOIN ${raceCte} ra
    ON ra.kaisai_nen = se.kaisai_nen
    AND ra.kaisai_tsukihi = se.kaisai_tsukihi
    AND ra.keibajo_code = se.keibajo_code
    AND ra.race_bango = se.race_bango
  WHERE nullif(btrim(coalesce(se.ketto_toroku_bango, '')), '') IS NOT NULL
    AND try_cast(nullif(se.umaban, '') AS INT) IS NOT NULL`;

const targetCte = (source: RunningStyleRawSource): string => `target_counts AS (
  SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    count(*) AS runner_count
  FROM target_rec
  WHERE source = '${source.rowSource}'
  GROUP BY source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
),
target AS (
  SELECT
    r.source, r.race_date, r.race_dt, r.kaisai_nen, r.kaisai_tsukihi,
    r.keibajo_code, r.race_bango, r.ketto_toroku_bango, r.umaban, r.bamei,
    CASE WHEN r.source = 'jra' THEN 'jra'
      WHEN r.keibajo_code = '83' THEN 'ban-ei' ELSE 'nar' END AS category,
    r.kyori, r.track_code, r.grade_code,
    coalesce(nullif(r.shusso_tosu, 0), counts.runner_count) AS shusso_tosu,
    r.finish_position, r.finish_norm, r.kishumei_ryakusho,
    r.chokyoshimei_ryakusho, r.kyoso_joken_code,
    CASE WHEN r.source = 'nar' AND r.keibajo_code NOT IN ('65', '83')
      THEN nullif(left(btrim(coalesce(r.kyoso_joken_code, '')), 1), '')
      ELSE NULL END AS nar_subclass,
    r.babajotai_code_shiba, r.babajotai_code_dirt,
    r.corner1_norm AS target_corner_1_norm,
    r.corner2_norm AS target_corner_2_norm,
    r.corner3_norm AS target_corner_3_norm,
    r.corner4_norm AS target_corner_4_norm,
    CASE WHEN r.corner1_norm IS NULL THEN NULL
      WHEN r.corner1_norm = 0 THEN 0
      WHEN r.corner1_norm <= 0.3 THEN 1
      WHEN r.corner1_norm <= 0.7 THEN 2 ELSE 3 END AS target_running_style_class,
    'v1' AS feature_schema_version,
    try_cast(substr(r.race_date, 1, 4) AS INT) AS race_year,
    r.bataiju, r.tenko_code
  FROM target_rec r
  INNER JOIN target_counts counts
    ON counts.source = r.source
    AND counts.kaisai_nen = r.kaisai_nen
    AND counts.kaisai_tsukihi = r.kaisai_tsukihi
    AND counts.keibajo_code = r.keibajo_code
    AND counts.race_bango = r.race_bango
  WHERE r.source = '${source.rowSource}'
)`;

export const buildRunningStyleFeaturesQuery = (
  env: R2SqlCatalogConfig,
  filters: RunningStyleFeatureFilters,
): string => {
  validateFilters(filters);
  const source = sourceConfig(filters.source);
  const historyWhere = historyPredicates(filters, source);
  const targetWhere = targetPredicates(filters, source);
  const historySe = tableName(env, source.runnerTable);
  const historyRa = tableName(env, source.raceTable);
  const master = tableName(env, source.masterTable);
  return `WITH history_se AS (
  SELECT${runnerColumns}
  FROM ${historySe}
  WHERE ${historyWhere}
),
history_ra AS (
  SELECT${raceColumns}
  FROM ${historyRa}
  WHERE ${historyWhere}
),
target_se AS (
  SELECT${runnerColumns}
  FROM ${historySe}
  WHERE ${targetWhere}
),
target_ra AS (
  SELECT${raceColumns}
  FROM ${historyRa}
  WHERE ${targetWhere}
),
rec AS (
  ${normalisedRecSelect(source, "history_se", "history_ra")}
),
target_rec AS (
  ${normalisedRecSelect(source, "target_se", "target_ra")}
),
${targetCte(source)},
${runningStyleFeatureCtesSql(master)}`;
};

export const buildRunningStyleExplainQuery = (
  env: R2SqlCatalogConfig,
  filters: RunningStyleFeatureFilters,
): string => `EXPLAIN FORMAT JSON ${buildRunningStyleFeaturesQuery(env, filters)}`;
