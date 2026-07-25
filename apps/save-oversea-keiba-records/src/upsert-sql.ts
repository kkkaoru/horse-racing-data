// This file runs with Bun.
import type { JvdRaRow, JvdSeRow, SqlStatement } from "./types";

interface UpsertConfig {
  readonly table: string;
  readonly columns: readonly string[];
  readonly keyColumns: readonly string[];
}

interface BuildUpsertInput {
  readonly config: UpsertConfig;
  readonly row: Readonly<Record<string, string>>;
}

const JVD_RA_COLUMNS: readonly string[] = [
  "record_id",
  "data_kubun",
  "data_sakusei_nengappi",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "kaisai_kai",
  "kaisai_nichime",
  "race_bango",
  "yobi_code",
  "tokubetsu_kyoso_bango",
  "kyosomei_hondai",
  "kyosomei_fukudai",
  "kyosomei_kakkonai",
  "kyosomei_hondai_eur",
  "kyosomei_fukudai_eur",
  "kyosomei_kakkonai_eur",
  "kyosomei_ryakusho_10",
  "kyosomei_ryakusho_6",
  "kyosomei_ryakusho_3",
  "kyosomei_kubun",
  "jusho_kaiji",
  "grade_code",
  "grade_code_henkomae",
  "kyoso_shubetsu_code",
  "kyoso_kigo_code",
  "juryo_shubetsu_code",
  "kyoso_joken_code_2sai",
  "kyoso_joken_code_3sai",
  "kyoso_joken_code_4sai",
  "kyoso_joken_code_5sai_ijo",
  "kyoso_joken_code",
  "kyoso_joken_meisho",
  "kyori",
  "kyori_henkomae",
  "track_code",
  "track_code_henkomae",
  "course_kubun",
  "course_kubun_henkomae",
  "honshokin",
  "honshokin_henkomae",
  "fukashokin",
  "fukashokin_henkomae",
  "hasso_jikoku",
  "hasso_jikoku_henkomae",
  "toroku_tosu",
  "shusso_tosu",
  "nyusen_tosu",
  "tenko_code",
  "babajotai_code_shiba",
  "babajotai_code_dirt",
  "lap_time",
  "shogai_mile_time",
  "zenhan_3f",
  "zenhan_4f",
  "kohan_3f",
  "kohan_4f",
  "corner_tsuka_juni_1",
  "corner_tsuka_juni_2",
  "corner_tsuka_juni_3",
  "corner_tsuka_juni_4",
  "record_koshin_kubun",
] satisfies readonly string[];

const JVD_SE_COLUMNS: readonly string[] = [
  "record_id",
  "data_kubun",
  "data_sakusei_nengappi",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "kaisai_kai",
  "kaisai_nichime",
  "race_bango",
  "wakuban",
  "umaban",
  "ketto_toroku_bango",
  "bamei",
  "umakigo_code",
  "seibetsu_code",
  "hinshu_code",
  "moshoku_code",
  "barei",
  "tozai_shozoku_code",
  "chokyoshi_code",
  "chokyoshimei_ryakusho",
  "banushi_code",
  "banushimei",
  "fukushoku_hyoji",
  "yobi_1",
  "futan_juryo",
  "futan_juryo_henkomae",
  "blinker_shiyo_kubun",
  "yobi_2",
  "kishu_code",
  "kishu_code_henkomae",
  "kishumei_ryakusho",
  "kishumei_ryakusho_henkomae",
  "kishu_minarai_code",
  "kishu_minarai_code_henkomae",
  "bataiju",
  "zogen_fugo",
  "zogen_sa",
  "ijo_kubun_code",
  "nyusen_juni",
  "kakutei_chakujun",
  "dochaku_kubun",
  "dochaku_tosu",
  "soha_time",
  "chakusa_code_1",
  "chakusa_code_2",
  "chakusa_code_3",
  "corner_1",
  "corner_2",
  "corner_3",
  "corner_4",
  "tansho_odds",
  "tansho_ninkijun",
  "kakutoku_honshokin",
  "kakutoku_fukashokin",
  "yobi_3",
  "yobi_4",
  "kohan_4f",
  "kohan_3f",
  "aiteuma_joho_1",
  "aiteuma_joho_2",
  "aiteuma_joho_3",
  "time_sa",
  "record_koshin_kubun",
  "mining_kubun",
  "yoso_soha_time",
  "yoso_gosa_plus",
  "yoso_gosa_minus",
  "yoso_juni",
  "kyakushitsu_hantei",
] satisfies readonly string[];

const JVD_RA_CONFIG: UpsertConfig = {
  table: "jvd_ra",
  columns: JVD_RA_COLUMNS,
  keyColumns: ["kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango"],
};

const JVD_SE_CONFIG: UpsertConfig = {
  table: "jvd_se",
  columns: JVD_SE_COLUMNS,
  keyColumns: [
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "umaban",
    "ketto_toroku_bango",
  ],
};

const TRIM_CHARACTERS: string = " 　";
const ZERO_ONLY_PATTERN: string = "^0+$";

const buildPreservingAssignment = (table: string, column: string): string =>
  `${column} = CASE WHEN NULLIF(btrim(excluded.${column}, '${TRIM_CHARACTERS}'), '') IS NULL OR btrim(excluded.${column}, '${TRIM_CHARACTERS}') ~ '${ZERO_ONLY_PATTERN}' THEN ${table}.${column} ELSE excluded.${column} END`;

const readColumnValue = (row: Readonly<Record<string, string>>, column: string): string => {
  const value: string | undefined = row[column];
  if (value === undefined) {
    throw new Error(`Missing required UPSERT column: ${column}`);
  }
  return value;
};

const buildUpsert = ({ config, row }: BuildUpsertInput): SqlStatement => {
  const placeholders: string[] = config.columns.map(
    (_column: string, index: number): string => `$${index + 1}`,
  );
  const keyColumnSet: ReadonlySet<string> = new Set(config.keyColumns);
  const updates: string[] = config.columns
    .filter((column: string): boolean => !keyColumnSet.has(column))
    .map((column: string): string => buildPreservingAssignment(config.table, column));

  return {
    text: `INSERT INTO ${config.table} (${config.columns.join(", ")}) VALUES (${placeholders.join(", ")}) ON CONFLICT (${config.keyColumns.join(", ")}) DO UPDATE SET ${updates.join(", ")}`,
    values: config.columns.map((column: string): string => readColumnValue(row, column)),
  };
};

export const buildJvdRaUpsert = (row: JvdRaRow): SqlStatement =>
  buildUpsert({ config: JVD_RA_CONFIG, row });

export const buildJvdSeUpsert = (row: JvdSeRow): SqlStatement =>
  buildUpsert({ config: JVD_SE_CONFIG, row });
