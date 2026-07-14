const EXCLUDED_FEATURE_COLUMNS = new Set([
  "bamei",
  "category",
  "feature_schema_version",
  "finish_norm",
  "finish_position",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "ketto_toroku_bango",
  "race_bango",
  "race_date",
  "race_id",
  "race_year",
  "source",
  "target_corner_1_norm",
  "target_corner_2_norm",
  "target_corner_3_norm",
  "target_corner_4_norm",
  "target_running_style_class",
  "umaban",
]);

const PEER_INPUT_COLUMNS: ReadonlyArray<[string, string]> = [
  ["past_nige_rate_self", "pastNigeRate"],
  ["past_senkou_rate_self", "pastSenkouRate"],
  ["past_sashi_rate_self", "pastSashiRate"],
  ["past_oikomi_rate_self", "pastOikomiRate"],
  ["past_corner_1_norm_avg_5", "pastCorner1NormAvg5"],
  ["speed_index_avg_5", "speedIndexAvg5"],
  ["speed_index_best_5", "speedIndexBest5"],
  ["past_first_3f_avg_5", "pastFirst3fAvg5"],
  ["kohan3f_avg_5", "kohan3fAvg5"],
  ["career_win_rate", "careerWinRate"],
];

const numberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value !== "string") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const stringOrNull = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value.trim() || null;
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return String(value);
  }
  return null;
};

const requiredString = (value: unknown, field: string): string => {
  const normalised = stringOrNull(value);
  if (normalised === null) throw new Error(`R2 SQL row is missing ${field}`);
  return normalised;
};

const featureNamesFor = (rows: ReadonlyArray<Record<string, unknown>>): string[] => {
  const first = rows[0];
  if (first === undefined) return [];
  return Object.keys(first).filter(
    (name) => !EXCLUDED_FEATURE_COLUMNS.has(name) && !name.startsWith("self_"),
  );
};

const numericMap = (
  row: Record<string, unknown>,
  columns: ReadonlyArray<string>,
): Record<string, number | null> =>
  Object.fromEntries(columns.map((name) => [name, numberOrNull(row[name])]));

const peerInputs = (row: Record<string, unknown>): Record<string, number | null> =>
  Object.fromEntries(PEER_INPUT_COLUMNS.map(([column, name]) => [name, numberOrNull(row[column])]));

const normaliseRow = (
  row: Record<string, unknown>,
  featureNames: ReadonlyArray<string>,
): Record<string, unknown> => {
  const source = requiredString(row.source, "source");
  const kaisaiNen = requiredString(row.kaisai_nen, "kaisai_nen");
  const kaisaiTsukihi = requiredString(row.kaisai_tsukihi, "kaisai_tsukihi");
  const keibajoCode = requiredString(row.keibajo_code, "keibajo_code").padStart(2, "0");
  const raceBango = requiredString(row.race_bango, "race_bango").padStart(2, "0");
  return {
    bamei: stringOrNull(row.bamei),
    category: stringOrNull(row.category) ?? source,
    gradeCode: stringOrNull(row.grade_code),
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    kettoTorokuBango: requiredString(row.ketto_toroku_bango, "ketto_toroku_bango"),
    kyori: numberOrNull(row.kyori),
    kyosoJokenCode: stringOrNull(row.kyoso_joken_code),
    narSubClass: stringOrNull(row.nar_subclass),
    peerInputs: peerInputs(row),
    perHorseFeatures: numericMap(row, featureNames),
    raceBango,
    raceKey: `${source}:${kaisaiNen}${kaisaiTsukihi}:${keibajoCode}:${raceBango}`,
    shussoTosu: numberOrNull(row.shusso_tosu),
    source,
    trackCode: stringOrNull(row.track_code),
    umaban: numberOrNull(row.umaban) ?? 0,
  };
};

export interface RunningStyleResponseBody {
  featureNames: string[];
  generation: "raw-iceberg-v1";
  rows: Record<string, unknown>[];
}

export const normaliseRunningStyleRows = (
  rows: ReadonlyArray<Record<string, unknown>>,
): RunningStyleResponseBody => {
  const featureNames = featureNamesFor(rows);
  return {
    featureNames,
    generation: "raw-iceberg-v1",
    rows: rows.map((row) => normaliseRow(row, featureNames)),
  };
};
