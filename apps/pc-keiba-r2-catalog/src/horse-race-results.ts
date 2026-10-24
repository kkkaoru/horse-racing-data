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
import { earliestRaceYear } from "./race-history-read";

interface HistoryTableSet {
  raceTable: "jvd_ra" | "nvd_ra";
  runnerTable: "jvd_se" | "nvd_se";
}

// Race days in [from, to); from = null means no lower date bound.
export interface HorseHistoryRange {
  from: string | null;
  settled: boolean;
  to: string;
}

export interface HorseHistoryQueryInput {
  env: R2SqlCatalogConfig;
  filters: HorseRaceResultsFilters;
  horseIds: readonly string[];
  range: HorseHistoryRange;
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

const HISTORY_RACE_COLUMNS: readonly string[] = [
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "kyosomei_hondai",
  "kyosomei_fukudai",
  "kyosomei_kakkonai",
  "grade_code",
  "kyoso_shubetsu_code",
  "kyoso_kigo_code",
  "juryo_shubetsu_code",
  "kyoso_joken_code",
  "kyoso_joken_meisho",
  "kyori",
  "track_code",
  "hasso_jikoku",
  "shusso_tosu",
  "tenko_code",
  "babajotai_code_shiba",
  "babajotai_code_dirt",
];
const HISTORY_RUNNER_COLUMNS: readonly string[] = [
  "wakuban",
  "umaban",
  "ketto_toroku_bango",
  "bamei",
  "seibetsu_code",
  "barei",
  "futan_juryo",
  "kishumei_ryakusho",
  "chokyoshimei_ryakusho",
  "banushimei",
  "bataiju",
  "zogen_fugo",
  "zogen_sa",
  "kakutei_chakujun",
  "tansho_odds",
  "tansho_ninkijun",
  "soha_time",
  "time_sa",
  "corner_1",
  "corner_2",
  "corner_3",
  "corner_4",
  "kohan_3f",
  "blinker_shiyo_kubun",
];
const HISTORY_SELECT_LIST: string = [
  ...HISTORY_RACE_COLUMNS.map((column) => `ra.${column}`),
  ...HISTORY_RUNNER_COLUMNS.map((column) => `se.${column}`),
].join(",\n    ");
const HISTORY_OUTPUT_LIST: string = [...HISTORY_RACE_COLUMNS, ...HISTORY_RUNNER_COLUMNS].join(
  ",\n  ",
);
const NUMERIC_PATTERN: RegExp = /^\d+$/u;

const checkedFilters = (filters: HorseRaceResultsFilters): HorseRaceResultsFilters => ({
  date: requireDate(filters.date),
  keibajoCode: requireCode(filters.keibajoCode, "keibajoCode"),
  raceBango: requireCode(filters.raceBango, "raceBango"),
  source: filters.source,
  sourceScope: filters.sourceScope,
});

// The volatile part (jockey, umaban, age) of the requested race. Only the
// race's own year partition is read, so it stays at the per-query minimum.
export const buildCurrentRunnersQuery = (
  env: R2SqlCatalogConfig,
  filters: HorseRaceResultsFilters,
): string => {
  const checked = checkedFilters(filters);
  return `
SELECT
  lpad(btrim(coalesce(umaban, '')), 2, '0') AS current_umaban,
  ketto_toroku_bango,
  seibetsu_code AS current_seibetsu_code,
  barei AS current_barei,
  ${namedOrUnknownSql("kishumei_ryakusho")} AS current_jockey
FROM ${tableName(env, currentRunnerTable(checked.source))}
WHERE kaisai_nen = '${checked.date.slice(0, 4)}'
  AND kaisai_tsukihi = '${checked.date.slice(4)}'
  AND keibajo_code = '${checked.keibajoCode}'
  AND race_bango = '${checked.raceBango}'
  AND ${registeredHorseSql("ketto_toroku_bango")}`;
};

// Sorted and de-duplicated so the same runners always produce the same SQL
// text, which is the key of the R2 SQL result cache.
export const currentHorseIds = (rows: ReadonlyArray<Record<string, unknown>>): string[] =>
  [
    ...new Set(rows.map((row) => stringOrNull(row.ketto_toroku_bango)).filter((id) => id !== null)),
  ].toSorted();

// Settled history (every race day before firstUnsettled) never changes and
// may be cached for hours; the remaining days up to the race are read fresh.
export const horseHistoryRanges = (date: string, firstUnsettled: string): HorseHistoryRange[] =>
  date <= firstUnsettled
    ? [{ from: null, settled: true, to: date }]
    : [
        { from: null, settled: true, to: firstUnsettled },
        { from: firstUnsettled, settled: false, to: date },
      ];

const yearBoundsSql = (alias: string, lower: number | null, upper: string): string =>
  [
    ...(lower === null ? [] : [`${alias}.kaisai_nen >= '${String(lower)}'`]),
    `${alias}.kaisai_nen <= '${upper}'`,
  ].join(" AND ");

const historySelectSql = (input: HorseHistoryQueryInput, tables: HistoryTableSet): string => {
  const earliest: number | null = earliestRaceYear(input.horseIds, input.filters.date);
  const lower: number | null =
    input.range.from === null
      ? earliest
      : Math.max(earliest ?? 0, Number(input.range.from.slice(0, 4)));
  const upper: string = input.range.to.slice(0, 4);
  return `SELECT
    ${HISTORY_SELECT_LIST}
  FROM ${tableName(input.env, tables.runnerTable)} se
  INNER JOIN ${tableName(input.env, tables.raceTable)} ra
    ON ra.kaisai_nen = se.kaisai_nen
    AND ra.kaisai_tsukihi = se.kaisai_tsukihi
    AND ra.keibajo_code = se.keibajo_code
    AND ra.race_bango = se.race_bango
  WHERE se.ketto_toroku_bango IN (${input.horseIds.map((id) => `'${id.replaceAll("'", "''")}'`).join(", ")})
    AND ${yearBoundsSql("se", lower, upper)}
    AND ${yearBoundsSql("ra", lower, upper)}`;
};

// Past starts of the given horses in [range.from, range.to). The bare
// kaisai_nen bounds (foal year and range) let R2 SQL prune year partitions.
export const buildHorseHistoryQuery = (input: HorseHistoryQueryInput): string => {
  const checked = checkedFilters(input.filters);
  const historySql = historyTables(checked.sourceScope)
    .map((tables) => historySelectSql({ ...input, filters: checked }, tables))
    .join("\n  UNION ALL\n  ");
  const lowerSql =
    input.range.from === null
      ? ""
      : `\n  AND concat(kaisai_nen, kaisai_tsukihi) >= '${requireDate(input.range.from)}'`;
  return `
WITH history AS (
  ${historySql}
)
SELECT
  ${HISTORY_OUTPUT_LIST}
FROM history
WHERE concat(kaisai_nen, kaisai_tsukihi) < '${requireDate(input.range.to)}'${lowerSql}`;
};

const umabanOrder = (row: Record<string, unknown>): number => {
  const umaban = stringOrNull(row.current_umaban);
  return umaban !== null && NUMERIC_PATTERN.test(umaban)
    ? Number(umaban)
    : Number.POSITIVE_INFINITY;
};

const descending = (left: unknown, right: unknown): number =>
  Number(String(left) < String(right)) - Number(String(left) > String(right));

// Same order as the former single query: try_cast(current_umaban AS INT)
// (non-numeric last), then the most recent start first.
const compareMergedRows = (left: Record<string, unknown>, right: Record<string, unknown>): number =>
  Number(umabanOrder(left) > umabanOrder(right)) - Number(umabanOrder(left) < umabanOrder(right)) ||
  descending(left.kaisai_nen, right.kaisai_nen) ||
  descending(left.kaisai_tsukihi, right.kaisai_tsukihi) ||
  descending(left.race_bango, right.race_bango);

// Joins each past start to the horse's current entry, as the former
// current_horses join did, and orders the rows like its ORDER BY.
export const mergeHorseRaceResults = (
  current: ReadonlyArray<Record<string, unknown>>,
  history: ReadonlyArray<Record<string, unknown>>,
): Record<string, unknown>[] =>
  history
    .flatMap((past) =>
      current
        .filter((entry) => entry.ketto_toroku_bango === past.ketto_toroku_bango)
        .map((entry) => ({ ...entry, ...past })),
    )
    .toSorted(compareMergedRows);

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
