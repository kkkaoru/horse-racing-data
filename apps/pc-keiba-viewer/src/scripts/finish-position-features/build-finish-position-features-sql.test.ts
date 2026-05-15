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
} from "./build-finish-position-features-sql";

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

test("HORSE_CAREER_COLUMNS contains 15 entries", () => {
  expect(HORSE_CAREER_COLUMNS.length).toBe(15);
});

test("JOCKEY_TRAINER_COLUMNS contains 12 entries", () => {
  expect(JOCKEY_TRAINER_COLUMNS.length).toBe(12);
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

test("ALL_FEATURE_COLUMNS aggregates to 54 entries", () => {
  expect(ALL_FEATURE_COLUMNS.length).toBe(54);
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
