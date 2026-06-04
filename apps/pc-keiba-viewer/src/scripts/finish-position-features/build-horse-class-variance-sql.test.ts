import { expect, test } from "vitest";

import {
  buildHorseClassVarianceUpdateSql,
  CLASS_VARIANCE_MIN_RACES,
  CLASS_VARIANCE_WINDOW,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  JRA_CLASS_LEVELS,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
} from "./build-horse-class-variance-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("HISTORY_LOOKBACK_DAYS_YYYYMMDD is ten years", () => {
  expect(HISTORY_LOOKBACK_DAYS_YYYYMMDD).toBe(100000);
});

test("CLASS_VARIANCE_WINDOW is five races", () => {
  expect(CLASS_VARIANCE_WINDOW).toBe(5);
});

test("CLASS_VARIANCE_MIN_RACES requires at least two races for variance", () => {
  expect(CLASS_VARIANCE_MIN_RACES).toBe(2);
});

test("JRA_CLASS_LEVELS maps maiden through G1 onto a 0..6 scale", () => {
  expect(JRA_CLASS_LEVELS).toStrictEqual({
    "000": 0,
    "005": 1,
    "010": 2,
    "016": 3,
    "701": 4,
    "703": 5,
    "999": 6,
  });
});

test("buildHorseClassVarianceUpdateSql narrows JRA target and history", () => {
  const sql = buildHorseClassVarianceUpdateSql("jra");
  expect(sql).toContain("target.category = 'jra'");
  expect(sql).toContain("history.source = 'jra'");
});

test("buildHorseClassVarianceUpdateSql narrows NAR excluding ban-ei keibajo 83", () => {
  expect(buildHorseClassVarianceUpdateSql("nar")).toContain(
    "history.source = 'nar' and history.keibajo_code <> '83'",
  );
});

test("buildHorseClassVarianceUpdateSql narrows NAR target category", () => {
  expect(buildHorseClassVarianceUpdateSql("nar")).toContain("target.category = 'nar'");
});

test("buildHorseClassVarianceUpdateSql narrows ban-ei to keibajo 83", () => {
  expect(buildHorseClassVarianceUpdateSql("ban-ei")).toContain(
    "history.source = 'nar' and history.keibajo_code = '83'",
  );
});

test("buildHorseClassVarianceUpdateSql narrows ban-ei target category", () => {
  expect(buildHorseClassVarianceUpdateSql("ban-ei")).toContain("target.category = 'ban-ei'");
});

test("buildHorseClassVarianceUpdateSql passes through when category is all", () => {
  const sql = buildHorseClassVarianceUpdateSql("all");
  expect(sql).toContain("and true");
  expect(sql).not.toContain("target.category = 'jra'");
  expect(sql).not.toContain("history.source = 'jra'");
});

test("buildHorseClassVarianceUpdateSql enforces strict less-than for leak prevention", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain("history.race_date < target.race_date");
});

test("buildHorseClassVarianceUpdateSql bounds history lookback to ten years", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "history.race_date >= (target.race_date::integer - 100000)::text",
  );
});

test("buildHorseClassVarianceUpdateSql excludes unmapped kyoso_joken_code from window via is-not-null filter", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain("else null end is not null");
});

test("buildHorseClassVarianceUpdateSql class level case branches over kyoso_joken_code", () => {
  const sql = buildHorseClassVarianceUpdateSql("jra");
  expect(sql).toContain("case history.kyoso_joken_code");
  expect(sql).toContain("when '999' then 6");
  expect(sql).toContain("when '000' then 0");
});

test("buildHorseClassVarianceUpdateSql ranks only the mapped history rows", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "order by history.race_date desc\n        ) as mapped_rank",
  );
});

test("buildHorseClassVarianceUpdateSql aliases the class level expression as history_class_level", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain("else null end as history_class_level");
});

test("buildHorseClassVarianceUpdateSql uses population stddev for the variance feature", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "stddev_pop(history_class_level::numeric)\n                 filter (where mapped_rank <= 5)",
  );
});

test("buildHorseClassVarianceUpdateSql guards horse_recent_class_variance by two-race minimum", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "count(*) filter (where mapped_rank <= 5) >= 2",
  );
});

test("buildHorseClassVarianceUpdateSql aliases the aggregate as horse_recent_class_variance", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain("end as horse_recent_class_variance");
});

test("buildHorseClassVarianceUpdateSql assigns horse_recent_class_variance via history_agg", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "horse_recent_class_variance = history_agg.horse_recent_class_variance",
  );
});

test("buildHorseClassVarianceUpdateSql refreshes updated_at", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildHorseClassVarianceUpdateSql joins target and history_agg via source", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain("target.source = history_agg.source");
});

test("buildHorseClassVarianceUpdateSql joins target and history_agg via ketto_toroku_bango", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "target.ketto_toroku_bango = history_agg.ketto_toroku_bango",
  );
});

test("buildHorseClassVarianceUpdateSql joins via kaisai_nen", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "target.kaisai_nen = history_agg.kaisai_nen",
  );
});

test("buildHorseClassVarianceUpdateSql joins via kaisai_tsukihi", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "target.kaisai_tsukihi = history_agg.kaisai_tsukihi",
  );
});

test("buildHorseClassVarianceUpdateSql joins via keibajo_code", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "target.keibajo_code = history_agg.keibajo_code",
  );
});

test("buildHorseClassVarianceUpdateSql joins via race_bango", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "target.race_bango = history_agg.race_bango",
  );
});

test("buildHorseClassVarianceUpdateSql targets the race_finish_position_features table", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "update race_finish_position_features target",
  );
});

test("buildHorseClassVarianceUpdateSql reads history from race_entry_corner_features", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "join race_entry_corner_features history",
  );
});

test("buildHorseClassVarianceUpdateSql partitions row_number by target primary key", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "partition by\n            target.source,\n            target.kaisai_nen,\n            target.kaisai_tsukihi,\n            target.keibajo_code,\n            target.race_bango,\n            target.ketto_toroku_bango",
  );
});

test("buildHorseClassVarianceUpdateSql uses date range parameters $1 and $2", () => {
  const sql = buildHorseClassVarianceUpdateSql("jra");
  expect(sql).toContain("target.race_date between $1 and $2");
});

test("buildHorseClassVarianceUpdateSql joins by horse via ketto_toroku_bango", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain(
    "history.ketto_toroku_bango = target.ketto_toroku_bango",
  );
});

test("buildHorseClassVarianceUpdateSql joins by source consistency", () => {
  expect(buildHorseClassVarianceUpdateSql("jra")).toContain("history.source = target.source");
});
