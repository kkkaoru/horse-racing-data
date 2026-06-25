import { expect, test } from "vitest";

import {
  buildWeightUpdateSql,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  JRA_RUNNER_TABLE,
  NAR_RUNNER_TABLE,
  RECENT_HISTORY_WINDOW_SIZE,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  WEIGHT_TREND_MIN_RACES,
  WEIGHT_ZSCORE_CLAMP,
  WEIGHT_ZSCORE_MIN_VOLATILITY,
} from "./build-weight-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("JRA_RUNNER_TABLE points to jvd_se", () => {
  expect(JRA_RUNNER_TABLE).toBe("jvd_se");
});

test("NAR_RUNNER_TABLE points to nvd_se", () => {
  expect(NAR_RUNNER_TABLE).toBe("nvd_se");
});

test("HISTORY_LOOKBACK_DAYS_YYYYMMDD is ten years", () => {
  expect(HISTORY_LOOKBACK_DAYS_YYYYMMDD).toBe(100000);
});

test("RECENT_HISTORY_WINDOW_SIZE is five races", () => {
  expect(RECENT_HISTORY_WINDOW_SIZE).toBe(5);
});

test("buildWeightUpdateSql narrows JRA target and history", () => {
  const sql = buildWeightUpdateSql("jra");
  expect(sql).toContain("target.category = 'jra'");
  expect(sql).toContain("history.source = 'jra'");
});

test("buildWeightUpdateSql narrows NAR excluding ban-ei", () => {
  expect(buildWeightUpdateSql("nar")).toContain(
    "history.source = 'nar' and history.keibajo_code <> '83'",
  );
});

test("buildWeightUpdateSql narrows ban-ei to keibajo 83", () => {
  expect(buildWeightUpdateSql("ban-ei")).toContain(
    "history.source = 'nar' and history.keibajo_code = '83'",
  );
});

test("buildWeightUpdateSql joins jvd_se for JRA bataiju on the target", () => {
  const sql = buildWeightUpdateSql("jra");
  expect(sql).toContain("left join jvd_se tj");
  expect(sql).toContain("target.source = 'jra'");
});

test("buildWeightUpdateSql joins nvd_se for NAR bataiju on the target", () => {
  expect(buildWeightUpdateSql("jra")).toContain("left join nvd_se tn");
});

test("buildWeightUpdateSql joins both jvd_se and nvd_se on the history side", () => {
  const sql = buildWeightUpdateSql("jra");
  expect(sql).toContain("left join jvd_se hj");
  expect(sql).toContain("left join nvd_se hn");
});

test("buildWeightUpdateSql safely casts JRA and NAR bataiju values", () => {
  const sql = buildWeightUpdateSql("jra");
  expect(sql).toContain("trim(coalesce(tj.bataiju::text, '')) ~ '^-?[0-9]+$'");
  expect(sql).toContain("trim(coalesce(tn.bataiju::text, '')) ~ '^-?[0-9]+$'");
  expect(sql).toContain("coalesce(");
});

test("buildWeightUpdateSql enforces strict less-than race_date for leak prevention", () => {
  expect(buildWeightUpdateSql("jra")).toContain("history.race_date < target.race_date");
});

test("buildWeightUpdateSql bounds history lookback to ten years", () => {
  expect(buildWeightUpdateSql("jra")).toContain(
    "history.race_date >= (target.race_date::integer - 100000)::text",
  );
});

test("buildWeightUpdateSql averages bataiju over the recent five races", () => {
  expect(buildWeightUpdateSql("jra")).toContain(
    "avg(history_bataiju) filter (where recent_rank <= 5) as weight_avg_5",
  );
});

test("buildWeightUpdateSql derives weight_diff_from_avg as current minus avg", () => {
  expect(buildWeightUpdateSql("jra")).toContain(
    "weight_diff_from_avg = history_agg.current_bataiju::numeric - history_agg.weight_avg_5",
  );
});

test("WEIGHT_TREND_MIN_RACES requires at least two points to avoid NaN slope", () => {
  expect(WEIGHT_TREND_MIN_RACES).toBe(2);
});

test("WEIGHT_ZSCORE_MIN_VOLATILITY floors volatility at one kilogram", () => {
  expect(WEIGHT_ZSCORE_MIN_VOLATILITY).toBe(1);
});

test("WEIGHT_ZSCORE_CLAMP bounds the z-score magnitude at five", () => {
  expect(WEIGHT_ZSCORE_CLAMP).toBe(5);
});

test("buildWeightUpdateSql derives weight_trend_5 via regr_slope guarded by a two-race minimum", () => {
  expect(buildWeightUpdateSql("jra")).toContain(
    "case when count(history_bataiju) filter (where recent_rank <= 5) >= 2\n             then regr_slope(history_bataiju, (-recent_rank)::double) filter (where recent_rank <= 5)\n             else null end as weight_trend_5",
  );
});

test("buildWeightUpdateSql derives weight_volatility_5 via stddev_pop over recent five", () => {
  expect(buildWeightUpdateSql("jra")).toContain(
    "stddev_pop(history_bataiju) filter (where recent_rank <= 5) as weight_volatility_5",
  );
});

test("buildWeightUpdateSql clamps weight_zscore and floors volatility at one kilogram", () => {
  expect(buildWeightUpdateSql("jra")).toContain(
    "weight_zscore = least(greatest((history_agg.current_bataiju::numeric - history_agg.weight_avg_5) / nullif(greatest(history_agg.weight_volatility_5, 1), 0), -5), 5)",
  );
});

test("buildWeightUpdateSql assigns weight_trend_5 and weight_volatility_5 from history_agg", () => {
  const sql = buildWeightUpdateSql("jra");
  expect(sql).toContain("weight_trend_5 = history_agg.weight_trend_5");
  expect(sql).toContain("weight_volatility_5 = history_agg.weight_volatility_5");
});

test("buildWeightUpdateSql refreshes updated_at", () => {
  expect(buildWeightUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildWeightUpdateSql joins target and history_agg via full primary key", () => {
  const sql = buildWeightUpdateSql("jra");
  expect(sql).toContain("target.source = history_agg.source");
  expect(sql).toContain("target.ketto_toroku_bango = history_agg.ketto_toroku_bango");
});
