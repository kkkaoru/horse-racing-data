import { expect, test } from "vitest";

import {
  buildRelationshipPhysicsUpdateSql,
  JRA_RUNNER_TABLE,
  NAR_RUNNER_TABLE,
  RACE_PARTITION_COLUMNS,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
} from "./build-relationship-physics-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toStrictEqual("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toStrictEqual("race_finish_position_features");
});

test("JRA_RUNNER_TABLE points to jvd_se", () => {
  expect(JRA_RUNNER_TABLE).toStrictEqual("jvd_se");
});

test("NAR_RUNNER_TABLE points to nvd_se", () => {
  expect(NAR_RUNNER_TABLE).toStrictEqual("nvd_se");
});

test("RACE_PARTITION_COLUMNS exposes the five-column race composite key", () => {
  expect(RACE_PARTITION_COLUMNS).toStrictEqual([
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
  ]);
});

test("buildRelationshipPhysicsUpdateSql narrows JRA target", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.category = 'jra'")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql narrows NAR target", () => {
  const sql = buildRelationshipPhysicsUpdateSql("nar");
  expect(sql.includes("target.category = 'nar'")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql narrows ban-ei target", () => {
  const sql = buildRelationshipPhysicsUpdateSql("ban-ei");
  expect(sql.includes("target.category = 'ban-ei'")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql passes through when category is all", () => {
  const sql = buildRelationshipPhysicsUpdateSql("all");
  expect(sql.includes("and true")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql does not leak JRA filter when category is all", () => {
  const sql = buildRelationshipPhysicsUpdateSql("all");
  expect(sql.includes("target.category = 'jra'")).toStrictEqual(false);
});

test("buildRelationshipPhysicsUpdateSql does not leak NAR filter when category is jra", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.category = 'nar'")).toStrictEqual(false);
});

test("buildRelationshipPhysicsUpdateSql does not leak ban-ei filter when category is nar", () => {
  const sql = buildRelationshipPhysicsUpdateSql("nar");
  expect(sql.includes("target.category = 'ban-ei'")).toStrictEqual(false);
});

test("buildRelationshipPhysicsUpdateSql joins jvd_se for JRA bataiju", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("left join jvd_se tj")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql joins nvd_se for NAR bataiju", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("left join nvd_se tn")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql joins race_entry_corner_features for futan kyori barei", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("join race_entry_corner_features rec")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql safely casts JRA bataiju", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("trim(coalesce(tj.bataiju::text, '')) ~ '^-?[0-9]+$'")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql safely casts NAR bataiju", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("trim(coalesce(tn.bataiju::text, '')) ~ '^-?[0-9]+$'")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql parameterizes date range via $1 and $2", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.race_date between $1 and $2")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql emits bataiju_futan_ratio with nullif divisor", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("futan_juryo / nullif(bataiju, 0) as bataiju_futan_ratio")).toStrictEqual(
    true,
  );
});

test("buildRelationshipPhysicsUpdateSql emits futan_per_barei with nullif divisor", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("futan_juryo / nullif(barei, 0) as futan_per_barei")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql emits bataiju_per_kyori_log via ln(1 + kyori)", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("bataiju / ln(1 + kyori) as bataiju_per_kyori_log")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql emits bataiju_diff_from_race_mean over race window", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(
    sql.includes("bataiju - avg(bataiju) over race_window as bataiju_diff_from_race_mean"),
  ).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql emits bataiju_rank_in_race via rank desc nulls last", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("order by bataiju desc nulls last")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql labels rank output as bataiju_rank_in_race", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes(") as bataiju_rank_in_race")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql emits barei_diff_from_race_mean over race window", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(
    sql.includes("barei - avg(barei) over race_window as barei_diff_from_race_mean"),
  ).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql guards zscore against null stddev", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("windowed.joint_ratio_stddev is null")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql guards zscore against zero stddev", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("windowed.joint_ratio_stddev = 0")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql centers zscore on joint_ratio_mean", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(
    sql.includes(
      "(windowed.joint_ratio - windowed.joint_ratio_mean) / windowed.joint_ratio_stddev",
    ),
  ).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql derives joint ratio for zscore numerator", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("(futan_juryo / nullif(bataiju, 0)) as joint_ratio")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql aggregates joint ratio mean over race window", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(
    sql.includes("avg(futan_juryo / nullif(bataiju, 0)) over race_window as joint_ratio_mean"),
  ).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql aggregates joint ratio stddev_pop over race window", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(
    sql.includes(
      "stddev_pop(futan_juryo / nullif(bataiju, 0)) over race_window as joint_ratio_stddev",
    ),
  ).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql defines race_window with source partition column", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("window race_window as (")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql partitions race_window by source", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("        source")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql partitions race_window by kaisai_nen", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("        kaisai_nen")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql partitions race_window by kaisai_tsukihi", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("        kaisai_tsukihi")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql partitions race_window by keibajo_code", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("        keibajo_code")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql partitions race_window by race_bango", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("        race_bango")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql assigns bataiju_futan_ratio from windowed CTE", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("bataiju_futan_ratio = windowed.bataiju_futan_ratio")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql assigns futan_per_barei from windowed CTE", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("futan_per_barei = windowed.futan_per_barei")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql assigns bataiju_per_kyori_log from windowed CTE", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("bataiju_per_kyori_log = windowed.bataiju_per_kyori_log")).toStrictEqual(
    true,
  );
});

test("buildRelationshipPhysicsUpdateSql assigns bataiju_diff_from_race_mean from windowed CTE", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(
    sql.includes("bataiju_diff_from_race_mean = windowed.bataiju_diff_from_race_mean"),
  ).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql assigns bataiju_rank_in_race from windowed CTE", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("bataiju_rank_in_race = windowed.bataiju_rank_in_race")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql assigns futan_minus_bataiju_zscore_in_race output column", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("futan_minus_bataiju_zscore_in_race =")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql assigns barei_diff_from_race_mean from windowed CTE", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(
    sql.includes("barei_diff_from_race_mean = windowed.barei_diff_from_race_mean"),
  ).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql refreshes updated_at", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("updated_at = now()")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql joins target and windowed via source", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.source = windowed.source")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql joins target and windowed via kaisai_nen", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.kaisai_nen = windowed.kaisai_nen")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql joins target and windowed via kaisai_tsukihi", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.kaisai_tsukihi = windowed.kaisai_tsukihi")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql joins target and windowed via keibajo_code", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.keibajo_code = windowed.keibajo_code")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql joins target and windowed via race_bango", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.race_bango = windowed.race_bango")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql joins target and windowed via ketto_toroku_bango", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("target.ketto_toroku_bango = windowed.ketto_toroku_bango")).toStrictEqual(
    true,
  );
});

test("buildRelationshipPhysicsUpdateSql casts bataiju to numeric for arithmetic safety", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("::numeric as bataiju")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql casts futan_juryo to numeric for arithmetic safety", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("rec.futan_juryo::numeric as futan_juryo")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql casts barei to numeric for arithmetic safety", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("rec.barei::numeric as barei")).toStrictEqual(true);
});

test("buildRelationshipPhysicsUpdateSql casts kyori to numeric for arithmetic safety", () => {
  const sql = buildRelationshipPhysicsUpdateSql("jra");
  expect(sql.includes("rec.kyori::numeric as kyori")).toStrictEqual(true);
});
