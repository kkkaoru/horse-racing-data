import { expect, test } from "vitest";

import {
  buildClassPromotionVelocityUpdateSql,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  PROMOTION_LEVEL_BUFFER,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  WINNING_FINISH_POSITION,
} from "./build-class-promotion-velocity-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("HISTORY_LOOKBACK_DAYS_YYYYMMDD is ten years in YYYYMMDD arithmetic", () => {
  expect(HISTORY_LOOKBACK_DAYS_YYYYMMDD).toBe(100000);
});

test("PROMOTION_LEVEL_BUFFER tolerates a one-step level descent for prior wins", () => {
  expect(PROMOTION_LEVEL_BUFFER).toBe(1);
});

test("WINNING_FINISH_POSITION restricts history to wins only", () => {
  expect(WINNING_FINISH_POSITION).toBe(1);
});

test("buildClassPromotionVelocityUpdateSql narrows JRA target and history", () => {
  const sql = buildClassPromotionVelocityUpdateSql("jra");
  expect(sql).toContain("target.category = 'jra'");
  expect(sql).toContain("history.source = 'jra'");
});

test("buildClassPromotionVelocityUpdateSql narrows NAR excluding ban-ei keibajo 83", () => {
  const sql = buildClassPromotionVelocityUpdateSql("nar");
  expect(sql).toContain("target.category = 'nar'");
  expect(sql).toContain("history.source = 'nar' and history.keibajo_code <> '83'");
});

test("buildClassPromotionVelocityUpdateSql narrows ban-ei to keibajo 83", () => {
  const sql = buildClassPromotionVelocityUpdateSql("ban-ei");
  expect(sql).toContain("target.category = 'ban-ei'");
  expect(sql).toContain("history.source = 'nar' and history.keibajo_code = '83'");
});

test("buildClassPromotionVelocityUpdateSql passes through when category is all", () => {
  const sql = buildClassPromotionVelocityUpdateSql("all");
  expect(sql).toContain("and true");
  expect(sql).not.toContain("target.category = 'jra'");
  expect(sql).not.toContain("history.source = 'jra'");
});

test("buildClassPromotionVelocityUpdateSql enforces strict less-than for leak prevention", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain(
    "history.race_date < target.target_race_date",
  );
});

test("buildClassPromotionVelocityUpdateSql bounds history lookback to ten years", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain(
    "history.race_date >= (target.target_race_date::integer - 100000)::text",
  );
});

test("buildClassPromotionVelocityUpdateSql filters history to wins only", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain("history.finish_position = 1");
});

test("buildClassPromotionVelocityUpdateSql requires non-null current class level", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain(
    "target.target_class_level is not null",
  );
});

test("buildClassPromotionVelocityUpdateSql restricts history to one level below current", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain(">= target.target_class_level - 1");
});

test("buildClassPromotionVelocityUpdateSql ranks past wins by descending race_date", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain("order by history.race_date desc");
});

test("buildClassPromotionVelocityUpdateSql derives velocity from to_date subtraction of most recent win", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain(
    "to_date(max(target_race_date) filter (where promotion_rank = 1), 'YYYYMMDD')\n            - to_date(max(history_race_date) filter (where promotion_rank = 1), 'YYYYMMDD')",
  );
});

test("buildClassPromotionVelocityUpdateSql labels the output column class_promotion_velocity", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain(") as class_promotion_velocity");
});

test("buildClassPromotionVelocityUpdateSql maps kyoso_joken_code to class level for target and history", () => {
  const sql = buildClassPromotionVelocityUpdateSql("jra");
  expect(sql).toContain("case rec.kyoso_joken_code");
  expect(sql).toContain("case history.kyoso_joken_code");
  expect(sql).toContain("when '999' then 6");
  expect(sql).toContain("else null end");
});

test("buildClassPromotionVelocityUpdateSql updates class_promotion_velocity assignment", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain(
    "class_promotion_velocity = history_agg.class_promotion_velocity",
  );
});

test("buildClassPromotionVelocityUpdateSql refreshes updated_at", () => {
  expect(buildClassPromotionVelocityUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildClassPromotionVelocityUpdateSql joins target and history_agg via full primary key", () => {
  const sql = buildClassPromotionVelocityUpdateSql("jra");
  expect(sql).toContain("target.source = history_agg.source");
  expect(sql).toContain("target.kaisai_nen = history_agg.kaisai_nen");
  expect(sql).toContain("target.kaisai_tsukihi = history_agg.kaisai_tsukihi");
  expect(sql).toContain("target.keibajo_code = history_agg.keibajo_code");
  expect(sql).toContain("target.race_bango = history_agg.race_bango");
  expect(sql).toContain("target.ketto_toroku_bango = history_agg.ketto_toroku_bango");
});

test("buildClassPromotionVelocityUpdateSql parameterizes date range via $1 and $2", () => {
  const sql = buildClassPromotionVelocityUpdateSql("jra");
  expect(sql).toContain("target.race_date between $1 and $2");
});
