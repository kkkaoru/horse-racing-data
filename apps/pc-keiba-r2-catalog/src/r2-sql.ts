import type { Fetcher, R2SqlCatalogConfig, RaceFeatureFilters, SourceScope } from "./types";

const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const DATE_PATTERN = /^\d{8}$/u;
const CODE_PATTERN = /^\d{2}$/u;

const RUNNER_COLUMNS = `
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    ketto_toroku_bango,
    wakuban,
    umaban,
    bamei,
    seibetsu_code,
    barei,
    futan_juryo,
    kishumei_ryakusho,
    chokyoshimei_ryakusho,
    banushimei,
    kakutei_chakujun,
    tansho_ninkijun,
    tansho_odds,
    soha_time,
    time_sa,
    kohan_3f,
    corner_1,
    corner_2,
    corner_3,
    corner_4,
    bataiju,
    zogen_fugo,
    zogen_sa`;

const RACE_COLUMNS = `
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    kyosomei_hondai,
    kyosomei_fukudai,
    hasso_jikoku,
    track_code,
    grade_code,
    kyoso_shubetsu_code,
    juryo_shubetsu_code,
    kyoso_joken_code,
    babajotai_code_shiba,
    babajotai_code_dirt,
    kyori,
    shusso_tosu`;

interface RawSourceConfig {
  prefix: "jra" | "nar";
  raceTable: "jvd_ra" | "nvd_ra";
  runnerTable: "jvd_se" | "nvd_se";
  source: "jra" | "nar";
}

interface RawFeatureSourceSql {
  ctes: string[];
  select: string;
}

const JRA_CONFIG: RawSourceConfig = {
  prefix: "jra",
  raceTable: "jvd_ra",
  runnerTable: "jvd_se",
  source: "jra",
};

const NAR_CONFIG: RawSourceConfig = {
  prefix: "nar",
  raceTable: "nvd_ra",
  runnerTable: "nvd_se",
  source: "nar",
};

const requireIdentifier = (value: string, label: string): string => {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`${label} must be an unquoted SQL identifier`);
  }
  return value;
};

const requireDate = (date: string): string => {
  if (!DATE_PATTERN.test(date)) throw new Error("date must match YYYYMMDD");
  return date;
};

const requireOptionalCode = (value: string | undefined, label: string): string | undefined => {
  if (value !== undefined && !CODE_PATTERN.test(value)) {
    throw new Error(`${label} must contain two digits`);
  }
  return value;
};

const namespaceName = (env: R2SqlCatalogConfig): string =>
  requireIdentifier(env.R2_SQL_NAMESPACE, "R2_SQL_NAMESPACE");

const rawTableName = (
  env: R2SqlCatalogConfig,
  table: RawSourceConfig["raceTable"] | RawSourceConfig["runnerTable"],
): string => `${namespaceName(env)}.${table}`;

const splitDate = (date: string): { kaisaiNen: string; kaisaiTsukihi: string } => {
  const checked = requireDate(date);
  return { kaisaiNen: checked.slice(0, 4), kaisaiTsukihi: checked.slice(4, 8) };
};

const sourceConfigs = (source: SourceScope): RawSourceConfig[] => {
  if (source === "jra") return [JRA_CONFIG];
  if (source === "nar" || source === "ban-ei") return [NAR_CONFIG];
  return [JRA_CONFIG, NAR_CONFIG];
};

const rawPredicates = (filters: RaceFeatureFilters, config: RawSourceConfig): string[] => {
  const { kaisaiNen, kaisaiTsukihi } = splitDate(filters.date);
  const keibajoCode = requireOptionalCode(filters.keibajoCode, "keibajoCode");
  const raceBango = requireOptionalCode(filters.raceBango, "raceBango");
  const venueScope =
    config.source === "nar" && filters.source === "nar"
      ? "keibajo_code <> '83'"
      : config.source === "nar" && filters.source === "ban-ei"
        ? "keibajo_code = '83'"
        : null;
  return [
    `kaisai_nen = '${kaisaiNen}'`,
    `kaisai_tsukihi = '${kaisaiTsukihi}'`,
    ...(venueScope === null ? [] : [venueScope]),
    ...(keibajoCode === undefined ? [] : [`keibajo_code = '${keibajoCode}'`]),
    ...(raceBango === undefined ? [] : [`race_bango = '${raceBango}'`]),
  ];
};

const buildRawCte = (
  name: string,
  table: string,
  columns: string,
  predicates: string[],
): string => `${name} AS (
  SELECT${columns}
  FROM ${table}
  WHERE ${predicates.join(" AND ")}
)`;

const normalisedFeatureSelect = (config: RawSourceConfig): string => {
  const fieldSize = `coalesce(
    try_cast(nullif(ra.shusso_tosu, '00') AS INT),
    counts.entry_count
  )`;
  return `SELECT
  '${config.source}' AS source,
  concat(ra.kaisai_nen, ra.kaisai_tsukihi) AS race_date,
  ra.kaisai_nen,
  ra.kaisai_tsukihi,
  lpad(ra.keibajo_code, 2, '0') AS keibajo_code,
  lpad(ra.race_bango, 2, '0') AS race_bango,
  se.ketto_toroku_bango,
  nullif(btrim(coalesce(se.wakuban, '')), '') AS wakuban,
  try_cast(nullif(se.umaban, '') AS INT) AS umaban,
  se.bamei,
  coalesce(
    nullif(btrim(coalesce(ra.kyosomei_hondai, '')), ''),
    nullif(btrim(coalesce(ra.kyosomei_fukudai, '')), ''),
    '一般競走'
  ) AS race_name,
  nullif(btrim(coalesce(ra.hasso_jikoku, '')), '') AS hasso_jikoku,
  ra.track_code,
  ra.grade_code,
  ra.kyoso_shubetsu_code,
  ra.juryo_shubetsu_code,
  ra.kyoso_joken_code,
  ra.babajotai_code_shiba,
  ra.babajotai_code_dirt,
  try_cast(nullif(ra.kyori, '') AS INT) AS kyori,
  ${fieldSize} AS shusso_tosu,
  se.seibetsu_code,
  try_cast(nullif(se.barei, '00') AS INT) AS barei,
  try_cast(nullif(se.futan_juryo, '000') AS FLOAT) / 10.0 AS futan_juryo,
  se.kishumei_ryakusho,
  se.chokyoshimei_ryakusho,
  se.banushimei,
  try_cast(nullif(se.kakutei_chakujun, '00') AS INT) AS finish_position,
  CASE
    WHEN ${fieldSize} > 1
      AND try_cast(nullif(se.kakutei_chakujun, '00') AS FLOAT) IS NOT NULL
    THEN (
      try_cast(nullif(se.kakutei_chakujun, '00') AS FLOAT) - 1
    ) / (
      ${fieldSize} - 1
    )
    ELSE NULL
  END AS finish_norm,
  try_cast(nullif(se.tansho_ninkijun, '00') AS INT) AS tansho_ninkijun,
  try_cast(nullif(se.tansho_odds, '0000') AS FLOAT) / 10.0 AS tansho_odds,
  try_cast(nullif(se.soha_time, '0000') AS INT) AS soha_time,
  try_cast(nullif(se.time_sa, '0000') AS FLOAT) / 10.0 AS time_sa,
  try_cast(nullif(se.kohan_3f, '000') AS FLOAT) / 10.0 AS kohan_3f,
  CASE
    WHEN ${fieldSize} > 1
      AND try_cast(nullif(se.corner_1, '00') AS FLOAT) IS NOT NULL
    THEN (try_cast(nullif(se.corner_1, '00') AS FLOAT) - 1)
      / (${fieldSize} - 1)
    ELSE NULL
  END AS corner1_norm,
  CASE
    WHEN ${fieldSize} > 1
      AND try_cast(nullif(se.corner_2, '00') AS FLOAT) IS NOT NULL
    THEN (try_cast(nullif(se.corner_2, '00') AS FLOAT) - 1)
      / (${fieldSize} - 1)
    ELSE NULL
  END AS corner2_norm,
  CASE
    WHEN ${fieldSize} > 1
      AND try_cast(nullif(se.corner_3, '00') AS FLOAT) IS NOT NULL
    THEN (try_cast(nullif(se.corner_3, '00') AS FLOAT) - 1)
      / (${fieldSize} - 1)
    ELSE NULL
  END AS corner3_norm,
  CASE
    WHEN ${fieldSize} > 1
      AND try_cast(nullif(se.corner_4, '00') AS FLOAT) IS NOT NULL
    THEN (try_cast(nullif(se.corner_4, '00') AS FLOAT) - 1)
      / (${fieldSize} - 1)
    ELSE NULL
  END AS corner4_norm,
  try_cast(nullif(se.corner_1, '00') AS INT) AS corner_1,
  try_cast(nullif(se.corner_2, '00') AS INT) AS corner_2,
  try_cast(nullif(se.corner_3, '00') AS INT) AS corner_3,
  try_cast(nullif(se.corner_4, '00') AS INT) AS corner_4,
  try_cast(nullif(se.bataiju, '000') AS INT) AS bataiju,
  nullif(btrim(coalesce(se.zogen_fugo, '')), '') AS zogen_fugo,
  try_cast(nullif(se.zogen_sa, '000') AS INT) AS zogen_sa
FROM ${config.prefix}_se se
INNER JOIN ${config.prefix}_ra ra
  ON ra.kaisai_nen = se.kaisai_nen
  AND ra.kaisai_tsukihi = se.kaisai_tsukihi
  AND ra.keibajo_code = se.keibajo_code
  AND ra.race_bango = se.race_bango
INNER JOIN ${config.prefix}_counts counts
  ON counts.kaisai_nen = se.kaisai_nen
  AND counts.kaisai_tsukihi = se.kaisai_tsukihi
  AND counts.keibajo_code = se.keibajo_code
  AND counts.race_bango = se.race_bango
WHERE nullif(btrim(coalesce(se.ketto_toroku_bango, '')), '') IS NOT NULL
  AND try_cast(nullif(se.umaban, '') AS INT) IS NOT NULL
  AND try_cast(nullif(ra.kyori, '') AS INT) IS NOT NULL
  AND try_cast(nullif(ra.shusso_tosu, '') AS INT) IS NOT NULL
  AND try_cast(nullif(ra.keibajo_code, '') AS INT) IS NOT NULL
  AND try_cast(nullif(ra.race_bango, '') AS INT) IS NOT NULL`;
};

const buildRunnerCountsCte = (config: RawSourceConfig): string =>
  `${config.prefix}_counts AS (
  SELECT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    count(*) AS entry_count
  FROM ${config.prefix}_se
  WHERE nullif(btrim(coalesce(ketto_toroku_bango, '')), '') IS NOT NULL
    AND try_cast(nullif(umaban, '') AS INT) IS NOT NULL
  GROUP BY kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
)`;

const buildRawFeatureSource = (
  env: R2SqlCatalogConfig,
  filters: RaceFeatureFilters,
  config: RawSourceConfig,
): RawFeatureSourceSql => {
  const predicates = rawPredicates(filters, config);
  return {
    ctes: [
      buildRawCte(
        `${config.prefix}_se`,
        rawTableName(env, config.runnerTable),
        RUNNER_COLUMNS,
        predicates,
      ),
      buildRawCte(
        `${config.prefix}_ra`,
        rawTableName(env, config.raceTable),
        RACE_COLUMNS,
        predicates,
      ),
      buildRunnerCountsCte(config),
    ],
    select: normalisedFeatureSelect(config),
  };
};

const buildRawRaceKeySelect = (
  env: R2SqlCatalogConfig,
  config: RawSourceConfig,
  date: string,
): string => {
  const { kaisaiNen, kaisaiTsukihi } = splitDate(date);
  return `SELECT
  '${config.source}' AS source,
  concat(kaisai_nen, kaisai_tsukihi) AS race_date,
  kaisai_nen,
  kaisai_tsukihi,
  lpad(keibajo_code, 2, '0') AS keibajo_code,
  lpad(race_bango, 2, '0') AS race_bango
FROM ${rawTableName(env, config.raceTable)}
WHERE kaisai_nen = '${kaisaiNen}' AND kaisai_tsukihi = '${kaisaiTsukihi}'`;
};

export const buildRaceKeysQuery = (env: R2SqlCatalogConfig, date: string): string => `
${buildRawRaceKeySelect(env, JRA_CONFIG, date)}
UNION ALL
${buildRawRaceKeySelect(env, NAR_CONFIG, date)}
ORDER BY source, keibajo_code, race_bango`;

export const buildRaceFeaturesQuery = (
  env: R2SqlCatalogConfig,
  filters: RaceFeatureFilters,
): string => {
  const sources = sourceConfigs(filters.source).map((config) =>
    buildRawFeatureSource(env, filters, config),
  );
  return `
WITH ${sources.flatMap((source) => source.ctes).join(",\n")}
${sources.map((source) => source.select).join("\nUNION ALL\n")}
ORDER BY source, keibajo_code, race_bango, umaban`;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseJson = (text: string): unknown => {
  try {
    const parsed: unknown = JSON.parse(text);
    return parsed;
  } catch {
    throw new Error("R2 SQL returned malformed JSON");
  }
};

// Thrown for every non-2xx or success:false R2 SQL response. Carries the
// first Cloudflare error entry's `code` (when present) as a typed field so
// callers can react to a *specific* R2 SQL failure (e.g. code 40018,
// "query expression too deep") without parsing the message string -- see
// running-style-features's ORDER BY fallback in worker.ts.
export class R2SqlQueryError extends Error {
  readonly code: number | string | undefined;

  constructor(message: string, code: number | string | undefined) {
    super(message);
    this.name = "R2SqlQueryError";
    this.code = code;
  }
}

const cloudflareErrors = (payload: unknown): Record<string, unknown>[] => {
  if (!isRecord(payload) || !Array.isArray(payload.errors)) return [];
  return payload.errors.filter(isRecord);
};

const cloudflareErrorText = (payload: unknown): string | null => {
  const details = cloudflareErrors(payload).flatMap((error): string[] => {
    if (typeof error.message !== "string") return [];
    const code =
      typeof error.code === "number" || typeof error.code === "string"
        ? `${String(error.code)} `
        : "";
    return [`${code}${error.message}`];
  });
  return details.length === 0 ? null : details.join("; ");
};

const cloudflareErrorCode = (payload: unknown): number | string | undefined => {
  const first = cloudflareErrors(payload)[0];
  if (first === undefined) return undefined;
  return typeof first.code === "number" || typeof first.code === "string" ? first.code : undefined;
};

const parseHttpErrorPayload = (text: string): unknown => {
  try {
    const parsed: unknown = JSON.parse(text);
    return parsed;
  } catch {
    return null;
  }
};

const parseRows = (payload: unknown): Record<string, unknown>[] => {
  if (!isRecord(payload) || payload.success !== true || !isRecord(payload.result)) {
    const details = cloudflareErrorText(payload);
    throw new R2SqlQueryError(
      details === null ? "R2 SQL query failed" : `R2 SQL query failed: ${details}`,
      cloudflareErrorCode(payload),
    );
  }
  const rows = payload.result.rows;
  if (!Array.isArray(rows) || !rows.every(isRecord)) {
    throw new Error("R2 SQL response has invalid rows");
  }
  return rows;
};

export const executeR2Sql = async (
  env: R2SqlCatalogConfig,
  query: string,
  fetchImpl: Fetcher,
): Promise<Record<string, unknown>[]> => {
  const warehouse = `${env.R2_SQL_ACCOUNT_ID}_${env.R2_SQL_BUCKET_NAME}`;
  const endpoint = `https://api.sql.cloudflarestorage.com/api/v1/accounts/${encodeURIComponent(env.R2_SQL_ACCOUNT_ID)}/r2-sql/query/${encodeURIComponent(env.R2_SQL_BUCKET_NAME)}`;
  const response = await fetchImpl(endpoint, {
    body: JSON.stringify({ query, warehouse }),
    headers: {
      Authorization: `Bearer ${env.R2_SQL_TOKEN}`,
      "Content-Type": "application/json",
    },
    method: "POST",
  });
  const text = await response.text();
  if (!response.ok) {
    const payload = parseHttpErrorPayload(text);
    const details = cloudflareErrorText(payload);
    const suffix = details === null ? "" : `: ${details}`;
    throw new R2SqlQueryError(
      `R2 SQL HTTP ${String(response.status)}${suffix}`,
      cloudflareErrorCode(payload),
    );
  }
  return parseRows(parseJson(text));
};
