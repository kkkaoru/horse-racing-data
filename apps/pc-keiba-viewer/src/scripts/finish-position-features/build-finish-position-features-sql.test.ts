import { expect, test } from "vitest";

import {
  ALL_FEATURE_COLUMNS,
  buildCreateTableSql,
  buildIndexSqls,
  buildSkeletonUpsertSql,
  FEATURE_TABLE_NAME,
  HORSE_CAREER_COLUMNS,
  JOCKEY_TRAINER_COLUMNS,
  LEGACY_FIVE_COLUMNS,
  META_COLUMNS,
  PEDIGREE_COLUMNS,
  PRIMARY_KEY_COLUMNS,
  RACE_CONTEXT_COLUMNS,
  RECENT_FORM_COLUMNS,
  RELATIONSHIP_R1_COLUMNS,
} from "./build-finish-position-features-sql";

const splitCreateTableDdl = (sql: string): readonly string[] =>
  sql.split(",").map((part) => part.trim());

const findFeatureDdl = (sql: string, column: string): string | undefined =>
  splitCreateTableDdl(sql).find((part) => part.startsWith(`${column} `));

test("FEATURE_TABLE_NAME is the expected target table", () => {
  expect(FEATURE_TABLE_NAME).toBe("race_finish_position_features");
});

test("PRIMARY_KEY_COLUMNS lists the six identifier columns", () => {
  expect(PRIMARY_KEY_COLUMNS).toStrictEqual([
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
  ]);
});

test("META_COLUMNS includes category and shusso_tosu", () => {
  expect(META_COLUMNS).toContain("category");
  expect(META_COLUMNS).toContain("shusso_tosu");
});

test("HORSE_CAREER_COLUMNS contains 18 entries", () => {
  expect(HORSE_CAREER_COLUMNS.length).toBe(18);
});

test("HORSE_CAREER_COLUMNS includes the three weight trend columns", () => {
  expect(HORSE_CAREER_COLUMNS.indexOf("weight_trend_5") >= 0).toStrictEqual(true);
  expect(HORSE_CAREER_COLUMNS.indexOf("weight_volatility_5") >= 0).toStrictEqual(true);
  expect(HORSE_CAREER_COLUMNS.indexOf("weight_zscore") >= 0).toStrictEqual(true);
});

test("JOCKEY_TRAINER_COLUMNS contains 20 entries", () => {
  expect(JOCKEY_TRAINER_COLUMNS.length).toBe(20);
});

test("JOCKEY_TRAINER_COLUMNS includes the five season-aware jockey columns", () => {
  expect(JOCKEY_TRAINER_COLUMNS.indexOf("jockey_season_win_rate") >= 0).toStrictEqual(true);
  expect(JOCKEY_TRAINER_COLUMNS.indexOf("jockey_season_keibajo_win_rate") >= 0).toStrictEqual(true);
  expect(JOCKEY_TRAINER_COLUMNS.indexOf("jockey_keibajo_distance_win_rate") >= 0).toStrictEqual(
    true,
  );
  expect(
    JOCKEY_TRAINER_COLUMNS.indexOf("jockey_season_keibajo_distance_win_rate") >= 0,
  ).toStrictEqual(true);
  expect(JOCKEY_TRAINER_COLUMNS.indexOf("jockey_season_keibajo_distance_count") >= 0).toStrictEqual(
    true,
  );
});

test("JOCKEY_TRAINER_COLUMNS includes the three class/surface trainer columns", () => {
  expect(JOCKEY_TRAINER_COLUMNS.indexOf("trainer_grade_win_rate") >= 0).toStrictEqual(true);
  expect(
    JOCKEY_TRAINER_COLUMNS.indexOf("trainer_class_surface_season_win_rate") >= 0,
  ).toStrictEqual(true);
  expect(JOCKEY_TRAINER_COLUMNS.indexOf("trainer_class_surface_season_count") >= 0).toStrictEqual(
    true,
  );
});

test("PEDIGREE_COLUMNS contains 6 entries", () => {
  expect(PEDIGREE_COLUMNS.length).toBe(6);
});

test("RACE_CONTEXT_COLUMNS contains 9 entries", () => {
  expect(RACE_CONTEXT_COLUMNS.length).toBe(9);
});

test("RECENT_FORM_COLUMNS contains 7 entries", () => {
  expect(RECENT_FORM_COLUMNS.length).toBe(7);
});

test("LEGACY_FIVE_COLUMNS preserves the heuristic five", () => {
  expect(LEGACY_FIVE_COLUMNS).toStrictEqual([
    "avg_finish",
    "recent_finish",
    "popularity_score",
    "odds_score",
    "same_day_jockey_win_score",
  ]);
});

test("ALL_FEATURE_COLUMNS has no duplicates", () => {
  expect(ALL_FEATURE_COLUMNS.length).toBe(new Set(ALL_FEATURE_COLUMNS).size);
});

test("ALL_FEATURE_COLUMNS aggregates to 77 entries", () => {
  expect(ALL_FEATURE_COLUMNS.length).toBe(77);
});

test("RELATIONSHIP_R1_COLUMNS lists the twelve iter-26 relationship features", () => {
  expect(RELATIONSHIP_R1_COLUMNS).toStrictEqual([
    "bataiju_futan_ratio",
    "futan_per_barei",
    "bataiju_per_kyori_log",
    "bataiju_diff_from_race_mean",
    "bataiju_rank_in_race",
    "futan_minus_bataiju_zscore_in_race",
    "barei_diff_from_race_mean",
    "past_speed_kg_normalized_avg5",
    "past_speed_futan_normalized_avg5",
    "past_speed_age_adjusted_avg5",
    "past_speed_volatility_5",
    "past_finish_position_volatility_5",
  ]);
});

test("ALL_FEATURE_COLUMNS includes bataiju_futan_ratio", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("bataiju_futan_ratio") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes futan_per_barei", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("futan_per_barei") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes bataiju_per_kyori_log", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("bataiju_per_kyori_log") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes bataiju_diff_from_race_mean", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("bataiju_diff_from_race_mean") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes bataiju_rank_in_race", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("bataiju_rank_in_race") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes futan_minus_bataiju_zscore_in_race", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("futan_minus_bataiju_zscore_in_race") >= 0).toStrictEqual(
    true,
  );
});

test("ALL_FEATURE_COLUMNS includes barei_diff_from_race_mean", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("barei_diff_from_race_mean") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes past_speed_kg_normalized_avg5", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("past_speed_kg_normalized_avg5") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes past_speed_futan_normalized_avg5", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("past_speed_futan_normalized_avg5") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes past_speed_age_adjusted_avg5", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("past_speed_age_adjusted_avg5") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes past_speed_volatility_5", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("past_speed_volatility_5") >= 0).toStrictEqual(true);
});

test("ALL_FEATURE_COLUMNS includes past_finish_position_volatility_5", () => {
  expect(ALL_FEATURE_COLUMNS.indexOf("past_finish_position_volatility_5") >= 0).toStrictEqual(true);
});

test("buildCreateTableSql declares bataiju_rank_in_race as integer", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "bataiju_rank_in_race")).toStrictEqual(
    "bataiju_rank_in_race integer",
  );
});

test("buildCreateTableSql declares bataiju_futan_ratio as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "bataiju_futan_ratio")).toStrictEqual(
    "bataiju_futan_ratio numeric",
  );
});

test("buildCreateTableSql declares futan_per_barei as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "futan_per_barei")).toStrictEqual(
    "futan_per_barei numeric",
  );
});

test("buildCreateTableSql declares bataiju_per_kyori_log as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "bataiju_per_kyori_log")).toStrictEqual(
    "bataiju_per_kyori_log numeric",
  );
});

test("buildCreateTableSql declares bataiju_diff_from_race_mean as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "bataiju_diff_from_race_mean")).toStrictEqual(
    "bataiju_diff_from_race_mean numeric",
  );
});

test("buildCreateTableSql declares futan_minus_bataiju_zscore_in_race as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "futan_minus_bataiju_zscore_in_race")).toStrictEqual(
    "futan_minus_bataiju_zscore_in_race numeric",
  );
});

test("buildCreateTableSql declares barei_diff_from_race_mean as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "barei_diff_from_race_mean")).toStrictEqual(
    "barei_diff_from_race_mean numeric",
  );
});

test("buildCreateTableSql declares past_speed_kg_normalized_avg5 as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "past_speed_kg_normalized_avg5")).toStrictEqual(
    "past_speed_kg_normalized_avg5 numeric",
  );
});

test("buildCreateTableSql declares past_speed_futan_normalized_avg5 as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "past_speed_futan_normalized_avg5")).toStrictEqual(
    "past_speed_futan_normalized_avg5 numeric",
  );
});

test("buildCreateTableSql declares past_speed_age_adjusted_avg5 as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "past_speed_age_adjusted_avg5")).toStrictEqual(
    "past_speed_age_adjusted_avg5 numeric",
  );
});

test("buildCreateTableSql declares past_speed_volatility_5 as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "past_speed_volatility_5")).toStrictEqual(
    "past_speed_volatility_5 numeric",
  );
});

test("buildCreateTableSql declares past_finish_position_volatility_5 as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "past_finish_position_volatility_5")).toStrictEqual(
    "past_finish_position_volatility_5 numeric",
  );
});

test("buildCreateTableSql declares jockey_season_keibajo_distance_count as integer", () => {
  expect(
    findFeatureDdl(buildCreateTableSql(), "jockey_season_keibajo_distance_count"),
  ).toStrictEqual("jockey_season_keibajo_distance_count integer");
});

test("buildCreateTableSql declares trainer_class_surface_season_count as integer", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "trainer_class_surface_season_count")).toStrictEqual(
    "trainer_class_surface_season_count integer",
  );
});

test("buildCreateTableSql declares weight_zscore as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "weight_zscore")).toStrictEqual(
    "weight_zscore numeric",
  );
});

test("buildCreateTableSql declares jockey_season_win_rate as numeric", () => {
  expect(findFeatureDdl(buildCreateTableSql(), "jockey_season_win_rate")).toStrictEqual(
    "jockey_season_win_rate numeric",
  );
});

test("buildCreateTableSql contains the table name", () => {
  expect(buildCreateTableSql()).toContain("race_finish_position_features");
});

test("buildCreateTableSql declares feature_schema_version default v1", () => {
  expect(buildCreateTableSql()).toContain("feature_schema_version text not null default 'v1'");
});

test("buildCreateTableSql declares each horse-career feature column", () => {
  const sql = buildCreateTableSql();
  expect(sql).toContain("speed_index_avg_5 numeric");
  expect(sql).toContain("days_since_last_race integer");
  expect(sql).toContain("consecutive_race_count integer");
});

test("buildCreateTableSql declares is_grade_race as smallint", () => {
  expect(buildCreateTableSql()).toContain("is_grade_race smallint");
});

test("buildCreateTableSql declares the composite primary key", () => {
  expect(buildCreateTableSql()).toContain(
    "primary key (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)",
  );
});

test("buildIndexSqls returns four idempotent index statements", () => {
  expect(buildIndexSqls().length).toBe(4);
});

test("buildIndexSqls every statement is idempotent", () => {
  const statements = buildIndexSqls();
  expect(statements.every((sql) => sql.includes("if not exists"))).toBe(true);
});

test("buildIndexSqls includes category lookup index", () => {
  const statements = buildIndexSqls();
  const categoryIndex = statements.find((sql) => sql.includes("category_date_idx"));
  expect(categoryIndex).toContain("(category, race_date desc)");
});

test("buildSkeletonUpsertSql narrows jra category", () => {
  const sql = buildSkeletonUpsertSql("jra", "v1");
  expect(sql).toContain("and source = 'jra'");
  expect(sql).toContain("'jra'::text as category");
});

test("buildSkeletonUpsertSql narrows nar category and excludes ban-ei", () => {
  const sql = buildSkeletonUpsertSql("nar", "v1");
  expect(sql).toContain("and source = 'nar' and keibajo_code <> '83'");
  expect(sql).toContain("'nar'::text as category");
});

test("buildSkeletonUpsertSql narrows ban-ei using keibajo_code 83", () => {
  const sql = buildSkeletonUpsertSql("ban-ei", "v1");
  expect(sql).toContain("and source = 'nar' and keibajo_code = '83'");
  expect(sql).toContain("'ban-ei'::text as category");
});

test("buildSkeletonUpsertSql leaves all category unfiltered", () => {
  const sql = buildSkeletonUpsertSql("all", "v1");
  expect(sql).not.toContain("and source =");
  expect(sql).toContain("case when source='jra' then 'jra'");
});

test("buildSkeletonUpsertSql passes the schema version literal", () => {
  expect(buildSkeletonUpsertSql("jra", "v2-beta")).toContain("'v2-beta'::text");
});

test("buildSkeletonUpsertSql sources rows from race_entry_corner_features", () => {
  expect(buildSkeletonUpsertSql("jra", "v1")).toContain("from race_entry_corner_features");
});

test("buildSkeletonUpsertSql uses the race_date BETWEEN $1 AND $2 window", () => {
  expect(buildSkeletonUpsertSql("jra", "v1")).toContain("where race_date between $1 and $2");
});

test("buildSkeletonUpsertSql upserts via primary key conflict", () => {
  const sql = buildSkeletonUpsertSql("jra", "v1");
  expect(sql).toContain(
    "on conflict (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)",
  );
});

test("buildSkeletonUpsertSql updates finish_position on conflict", () => {
  expect(buildSkeletonUpsertSql("jra", "v1")).toContain(
    "finish_position = excluded.finish_position",
  );
});

test("buildSkeletonUpsertSql refreshes the schema version on conflict", () => {
  expect(buildSkeletonUpsertSql("jra", "v1")).toContain(
    "feature_schema_version = excluded.feature_schema_version",
  );
});
