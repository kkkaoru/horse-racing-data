import { expect, test } from "vitest";

import {
  buildRecentFormUpdateSql,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  JRA_CLASS_LEVELS,
  RECENT_THREE_WINDOW,
  RECENT_TREND_WINDOW,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  TREND_MIN_RACES,
} from "./build-recent-form-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("HISTORY_LOOKBACK_DAYS_YYYYMMDD is ten years", () => {
  expect(HISTORY_LOOKBACK_DAYS_YYYYMMDD).toBe(100000);
});

test("RECENT_TREND_WINDOW is five races", () => {
  expect(RECENT_TREND_WINDOW).toBe(5);
});

test("RECENT_THREE_WINDOW is three races", () => {
  expect(RECENT_THREE_WINDOW).toBe(3);
});

test("TREND_MIN_RACES requires at least three races for a slope", () => {
  expect(TREND_MIN_RACES).toBe(3);
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

test("buildRecentFormUpdateSql narrows JRA target and history", () => {
  const sql = buildRecentFormUpdateSql("jra");
  expect(sql).toContain("target.category = 'jra'");
  expect(sql).toContain("history.source = 'jra'");
});

test("buildRecentFormUpdateSql narrows NAR excluding ban-ei keibajo 83", () => {
  expect(buildRecentFormUpdateSql("nar")).toContain(
    "history.source = 'nar' and history.keibajo_code <> '83'",
  );
});

test("buildRecentFormUpdateSql narrows ban-ei to keibajo 83", () => {
  expect(buildRecentFormUpdateSql("ban-ei")).toContain(
    "history.source = 'nar' and history.keibajo_code = '83'",
  );
});

test("buildRecentFormUpdateSql enforces strict less-than for leak prevention", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain("history.race_date < target.race_date");
});

test("buildRecentFormUpdateSql bounds history lookback to ten years", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain(
    "history.race_date >= (target.race_date::integer - 100000)::text",
  );
});

test("buildRecentFormUpdateSql derives last_race_finish_norm from recent_rank = 1", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain(
    "max(finish_norm) filter (where recent_rank = 1) as last_race_finish_norm",
  );
});

test("buildRecentFormUpdateSql derives last_race_margin_to_winner from time_sa", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain(
    "max(time_sa) filter (where recent_rank = 1) as last_race_margin_to_winner",
  );
});

test("buildRecentFormUpdateSql derives last_race_corner_pass_norm from corner3_norm", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain(
    "max(corner3_norm) filter (where recent_rank = 1) as last_race_corner_pass_norm",
  );
});

test("buildRecentFormUpdateSql class diff goes target_class - history_class", () => {
  const sql = buildRecentFormUpdateSql("jra");
  expect(sql).toContain(
    "max(target_class_level) filter (where recent_rank = 1)\n            - max(history_class_level) filter (where recent_rank = 1)",
  );
});

test("buildRecentFormUpdateSql distance diff goes history_kyori - target_kyori", () => {
  const sql = buildRecentFormUpdateSql("jra");
  expect(sql).toContain(
    "max(history_kyori) filter (where recent_rank = 1)\n            - max(target_kyori) filter (where recent_rank = 1)",
  );
});

test("buildRecentFormUpdateSql uses regr_slope for finish_trend_5", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain(
    "regr_slope(finish_norm, recent_rank::numeric)\n                 filter (where recent_rank <= 5)",
  );
});

test("buildRecentFormUpdateSql guards finish_trend_5 by three-race minimum", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain(
    "count(*) filter (where recent_rank <= 5) >= 3",
  );
});

test("buildRecentFormUpdateSql averages the three most recent finish_norm", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain(
    "avg(finish_norm) filter (where recent_rank <= 3) as last_3_avg_finish_norm",
  );
});

test("buildRecentFormUpdateSql maps kyoso_joken_code to class level inside target and history", () => {
  const sql = buildRecentFormUpdateSql("jra");
  expect(sql).toContain("case rec.kyoso_joken_code");
  expect(sql).toContain("case history.kyoso_joken_code");
  expect(sql).toContain("when '999' then 6");
});

test("buildRecentFormUpdateSql refreshes updated_at", () => {
  expect(buildRecentFormUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildRecentFormUpdateSql joins target and history_agg via full primary key", () => {
  const sql = buildRecentFormUpdateSql("jra");
  expect(sql).toContain("target.source = history_agg.source");
  expect(sql).toContain("target.ketto_toroku_bango = history_agg.ketto_toroku_bango");
});
