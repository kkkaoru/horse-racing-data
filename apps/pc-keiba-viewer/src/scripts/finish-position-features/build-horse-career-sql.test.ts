import { expect, test } from "vitest";

import {
  buildHorseCareerUpdateSql,
  CONSECUTIVE_RACE_WINDOW_DAYS,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  RECENT_HISTORY_WINDOW_SIZE,
  SAME_DISTANCE_TOLERANCE_METERS,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
} from "./build-horse-career-sql";

test("SOURCE_FEATURE_TABLE points to the existing aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the new finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("HISTORY_LOOKBACK_DAYS_YYYYMMDD is ten years in YYYYMMDD arithmetic", () => {
  expect(HISTORY_LOOKBACK_DAYS_YYYYMMDD).toBe(100000);
});

test("RECENT_HISTORY_WINDOW_SIZE is five races", () => {
  expect(RECENT_HISTORY_WINDOW_SIZE).toBe(5);
});

test("SAME_DISTANCE_TOLERANCE_METERS is 200m", () => {
  expect(SAME_DISTANCE_TOLERANCE_METERS).toBe(200);
});

test("CONSECUTIVE_RACE_WINDOW_DAYS is 30 days", () => {
  expect(CONSECUTIVE_RACE_WINDOW_DAYS).toBe(30);
});

test("buildHorseCareerUpdateSql narrows JRA target and history", () => {
  const sql = buildHorseCareerUpdateSql("jra");
  expect(sql).toContain("target.category = 'jra'");
  expect(sql).toContain("history.source = 'jra'");
});

test("buildHorseCareerUpdateSql narrows NAR excluding ban-ei", () => {
  const sql = buildHorseCareerUpdateSql("nar");
  expect(sql).toContain("target.category = 'nar'");
  expect(sql).toContain("history.source = 'nar' and history.keibajo_code <> '83'");
});

test("buildHorseCareerUpdateSql narrows ban-ei keibajo 83", () => {
  const sql = buildHorseCareerUpdateSql("ban-ei");
  expect(sql).toContain("target.category = 'ban-ei'");
  expect(sql).toContain("history.source = 'nar' and history.keibajo_code = '83'");
});

test("buildHorseCareerUpdateSql passes through when category is all", () => {
  const sql = buildHorseCareerUpdateSql("all");
  expect(sql).toContain("and true");
  expect(sql).not.toContain("target.category = 'jra'");
  expect(sql).not.toContain("history.source = 'jra'");
});

test("buildHorseCareerUpdateSql enforces strict less-than for leak prevention", () => {
  const sql = buildHorseCareerUpdateSql("jra");
  expect(sql).toContain("history.race_date < target.race_date");
});

test("buildHorseCareerUpdateSql bounds history lookback to ten years", () => {
  const sql = buildHorseCareerUpdateSql("jra");
  expect(sql).toContain("history.race_date >= (target.race_date::integer - 100000)::text");
});

test("buildHorseCareerUpdateSql updates speed_index_avg_5 from recent window", () => {
  const sql = buildHorseCareerUpdateSql("jra");
  expect(sql).toContain("avg(time_sa) filter (where recent_rank <= 5) as speed_index_avg_5");
  expect(sql).toContain("speed_index_avg_5 = history_agg.speed_index_avg_5");
});

test("buildHorseCareerUpdateSql derives speed_index_best_5 as minimum time_sa", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "min(time_sa) filter (where recent_rank <= 5) as speed_index_best_5",
  );
});

test("buildHorseCareerUpdateSql derives kohan3f_avg_5 over recent window", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "avg(kohan_3f) filter (where recent_rank <= 5) as kohan3f_avg_5",
  );
});

test("buildHorseCareerUpdateSql derives corner_pass_avg_5 over recent window", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "avg(corner4_norm) filter (where recent_rank <= 5) as corner_pass_avg_5",
  );
});

test("buildHorseCareerUpdateSql computes career_win_rate over all history", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "avg(case when finish_position = 1 then 1 else 0 end) as career_win_rate",
  );
});

test("buildHorseCareerUpdateSql computes career_place_rate over all history", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "avg(case when finish_position between 1 and 3 then 1 else 0 end) as career_place_rate",
  );
});

test("buildHorseCareerUpdateSql counts career top1 finishes", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "count(*) filter (where finish_position = 1) as career_top1_count",
  );
});

test("buildHorseCareerUpdateSql derives same_keibajo_win_rate", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "filter (where history_keibajo_code = target_keibajo_code) as same_keibajo_win_rate",
  );
});

test("buildHorseCareerUpdateSql derives same_distance_win_rate with 200m tolerance", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "filter (where abs(history_kyori - target_kyori) <= 200) as same_distance_win_rate",
  );
});

test("buildHorseCareerUpdateSql derives same_track_win_rate by track_code first digit", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)",
  );
});

test("buildHorseCareerUpdateSql derives same_grade_win_rate via coalesce", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "coalesce(history_grade_code, '') = coalesce(target_grade_code, '')",
  );
});

test("buildHorseCareerUpdateSql days_since_last_race uses to_date subtraction", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "to_date(max(target_race_date), 'YYYYMMDD') - to_date(max(history_race_date) filter (where recent_rank = 1), 'YYYYMMDD')",
  );
});

test("buildHorseCareerUpdateSql consecutive_race_count uses to_date diff window", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain(
    "count(*) filter (where to_date(target_race_date, 'YYYYMMDD') - to_date(history_race_date, 'YYYYMMDD') <= 30) as consecutive_race_count",
  );
});

test("buildHorseCareerUpdateSql refreshes updated_at", () => {
  expect(buildHorseCareerUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildHorseCareerUpdateSql joins target and history_agg via full primary key", () => {
  const sql = buildHorseCareerUpdateSql("jra");
  expect(sql).toContain("target.source = history_agg.source");
  expect(sql).toContain("target.ketto_toroku_bango = history_agg.ketto_toroku_bango");
});
