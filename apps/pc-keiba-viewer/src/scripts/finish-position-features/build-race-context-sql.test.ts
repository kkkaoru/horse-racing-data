import { expect, test } from "vitest";

import {
  BABAJOTAI_NORMALIZED_VALUES,
  buildRaceContextUpdateSql,
  MAX_FIELD_SIZE,
  RIVAL_DISTANCE_THRESHOLD,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  TOP_SPEED_HORSE_COUNT,
} from "./build-race-context-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("TOP_SPEED_HORSE_COUNT is three", () => {
  expect(TOP_SPEED_HORSE_COUNT).toBe(3);
});

test("RIVAL_DISTANCE_THRESHOLD is 30 percent", () => {
  expect(RIVAL_DISTANCE_THRESHOLD).toBe(0.3);
});

test("MAX_FIELD_SIZE is 18", () => {
  expect(MAX_FIELD_SIZE).toBe(18);
});

test("BABAJOTAI_NORMALIZED_VALUES maps good=0 .. unrideable=1", () => {
  expect(BABAJOTAI_NORMALIZED_VALUES).toStrictEqual({
    "1": "0",
    "2": "0.3",
    "3": "0.6",
    "4": "1.0",
  });
});

test("buildRaceContextUpdateSql restricts target to JRA category", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain("target.category = 'jra'");
});

test("buildRaceContextUpdateSql restricts target to NAR category", () => {
  expect(buildRaceContextUpdateSql("nar")).toContain("target.category = 'nar'");
});

test("buildRaceContextUpdateSql restricts target to ban-ei category", () => {
  expect(buildRaceContextUpdateSql("ban-ei")).toContain("target.category = 'ban-ei'");
});

test("buildRaceContextUpdateSql passes through when category is all", () => {
  const sql = buildRaceContextUpdateSql("all");
  expect(sql).toContain("and true");
  expect(sql).not.toContain("target.category = 'jra'");
});

test("buildRaceContextUpdateSql sources race horses from race_finish_position_features", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain("from race_finish_position_features target");
});

test("buildRaceContextUpdateSql aggregates race_avg_speed", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain("avg(speed_index_avg_5) as race_avg_speed");
});

test("buildRaceContextUpdateSql counts strong distance specialists per race", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain(
    "count(*) filter (where same_distance_win_rate > 0.3) as race_strong_count",
  );
});

test("buildRaceContextUpdateSql ranks by best speed (lowest time) for top 3", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain("order by speed_index_best_5 asc nulls last");
});

test("buildRaceContextUpdateSql limits top speed window to 3 horses", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain("where speed_rank <= 3");
});

test("buildRaceContextUpdateSql averages top-3 speeds as field_strength_top3_speed", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain(
    "field_strength_top3_speed = rts.race_top_speed",
  );
});

test("buildRaceContextUpdateSql excludes self when computing rival_count_at_distance", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain(
    "greatest(0, rfa.race_strong_count - case when tc.target_same_distance_win_rate > 0.3 then 1 else 0 end)",
  );
});

test("buildRaceContextUpdateSql normalises field size to [0,1]", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain(
    "least(1, greatest(0, coalesce(tc.target_shusso_tosu, 0)::numeric / 18))",
  );
});

test("buildRaceContextUpdateSql derives is_grade_race only for graded stakes (A-D/G/H)", () => {
  const sql = buildRaceContextUpdateSql("jra");
  expect(sql).toContain(
    "btrim(coalesce(tc.target_grade_code, '')) in ('A', 'B', 'C', 'D', 'G', 'H')",
  );
});

test("buildRaceContextUpdateSql derives track_condition from turf vs dirt path", () => {
  const sql = buildRaceContextUpdateSql("jra");
  expect(sql).toContain("when left(coalesce(tc.target_track_code, ''), 1) = '1' then");
  expect(sql).toContain("when '1' then 0::numeric");
  expect(sql).toContain("when '4' then 1.0::numeric");
});

test("buildRaceContextUpdateSql joins target_context via the full primary key", () => {
  const sql = buildRaceContextUpdateSql("jra");
  expect(sql).toContain("target.source = tc.source");
  expect(sql).toContain("target.ketto_toroku_bango = tc.ketto_toroku_bango");
});

test("buildRaceContextUpdateSql refreshes updated_at", () => {
  expect(buildRaceContextUpdateSql("jra")).toContain("updated_at = now()");
});
