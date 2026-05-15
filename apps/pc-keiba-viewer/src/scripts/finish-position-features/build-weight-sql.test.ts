import { expect, test } from "vitest";

import {
  buildWeightUpdateSql,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  JRA_RUNNER_TABLE,
  NAR_RUNNER_TABLE,
  RECENT_HISTORY_WINDOW_SIZE,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
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

test("buildWeightUpdateSql coalesces JRA and NAR bataiju and nulls empty strings", () => {
  expect(buildWeightUpdateSql("jra")).toContain(
    "coalesce(nullif(tj.bataiju, '')::integer, nullif(tn.bataiju, '')::integer)",
  );
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

test("buildWeightUpdateSql refreshes updated_at", () => {
  expect(buildWeightUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildWeightUpdateSql joins target and history_agg via full primary key", () => {
  const sql = buildWeightUpdateSql("jra");
  expect(sql).toContain("target.source = history_agg.source");
  expect(sql).toContain("target.ketto_toroku_bango = history_agg.ketto_toroku_bango");
});
