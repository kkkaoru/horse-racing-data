import { expect, test } from "vitest";

import {
  buildJockeyUpdateSql,
  buildSourceFeatureLookupIndexSqls,
  buildTrainerUpdateSql,
  HISTORY_LOOKBACK_DAYS_YYYYMMDD,
  RECENT_WINDOW_DAYS,
  SAME_DISTANCE_TOLERANCE_METERS,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
} from "./build-jockey-trainer-sql";

test("SOURCE_FEATURE_TABLE points to the existing aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the new finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("HISTORY_LOOKBACK_DAYS_YYYYMMDD is ten years in YYYYMMDD arithmetic", () => {
  expect(HISTORY_LOOKBACK_DAYS_YYYYMMDD).toBe(100000);
});

test("RECENT_WINDOW_DAYS is 60 days", () => {
  expect(RECENT_WINDOW_DAYS).toBe(60);
});

test("SAME_DISTANCE_TOLERANCE_METERS is 200m", () => {
  expect(SAME_DISTANCE_TOLERANCE_METERS).toBe(200);
});

test("buildJockeyUpdateSql narrows JRA target and history", () => {
  const sql = buildJockeyUpdateSql("jra");
  expect(sql).toContain("target.category = 'jra'");
  expect(sql).toContain("history.source = 'jra'");
});

test("buildJockeyUpdateSql narrows NAR excluding ban-ei", () => {
  const sql = buildJockeyUpdateSql("nar");
  expect(sql).toContain("target.category = 'nar'");
  expect(sql).toContain("history.source = 'nar' and history.keibajo_code <> '83'");
});

test("buildJockeyUpdateSql narrows ban-ei to keibajo 83", () => {
  const sql = buildJockeyUpdateSql("ban-ei");
  expect(sql).toContain("history.source = 'nar' and history.keibajo_code = '83'");
});

test("buildJockeyUpdateSql joins history by kishumei_ryakusho", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "history.kishumei_ryakusho = target.target_partner",
  );
});

test("buildJockeyUpdateSql enforces strict less-than race_date for leak prevention", () => {
  expect(buildJockeyUpdateSql("jra")).toContain("history.race_date < target.race_date");
});

test("buildJockeyUpdateSql bounds history lookback to ten years", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "history.race_date >= (target.race_date::integer - 100000)::text",
  );
});

test("buildJockeyUpdateSql derives jockey_career_win_rate over all history", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "avg(case when finish_position = 1 then 1 else 0 end) as jockey_career_win_rate",
  );
});

test("buildJockeyUpdateSql derives jockey_recent_win_rate within the 60-day window", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "to_date(history_race_date, 'YYYYMMDD') >= to_date(target_race_date, 'YYYYMMDD') - 60",
  );
});

test("buildJockeyUpdateSql derives jockey_keibajo_win_rate via filter", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "filter (where history_keibajo_code = target_keibajo_code) as jockey_keibajo_win_rate",
  );
});

test("buildJockeyUpdateSql derives jockey_distance_win_rate with 200m tolerance", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "filter (where abs(history_kyori - target_kyori) <= 200) as jockey_distance_win_rate",
  );
});

test("buildJockeyUpdateSql derives jockey_track_win_rate by track-code first digit", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)",
  );
});

test("buildJockeyUpdateSql derives jockey_grade_win_rate via coalesce", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "coalesce(history_grade_code, '') = coalesce(target_grade_code, '')",
  );
});

test("buildJockeyUpdateSql derives jockey_horse_pair_count via history_horse equality", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "count(*) filter (where history_horse = target_horse) as jockey_horse_pair_count",
  );
});

test("buildJockeyUpdateSql derives jockey_horse_pair_win_rate", () => {
  expect(buildJockeyUpdateSql("jra")).toContain(
    "filter (where history_horse = target_horse) as jockey_horse_pair_win_rate",
  );
});

test("buildJockeyUpdateSql refreshes updated_at", () => {
  expect(buildJockeyUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildJockeyUpdateSql requires non-empty jockey name in target", () => {
  const sql = buildJockeyUpdateSql("jra");
  expect(sql).toContain("rec.kishumei_ryakusho is not null");
  expect(sql).toContain("btrim(rec.kishumei_ryakusho) <> ''");
});

test("buildTrainerUpdateSql joins history by chokyoshimei_ryakusho", () => {
  expect(buildTrainerUpdateSql("jra")).toContain(
    "history.chokyoshimei_ryakusho = target.target_partner",
  );
});

test("buildTrainerUpdateSql derives trainer_career_win_rate", () => {
  expect(buildTrainerUpdateSql("jra")).toContain(
    "avg(case when finish_position = 1 then 1 else 0 end) as trainer_career_win_rate",
  );
});

test("buildTrainerUpdateSql derives trainer_keibajo_win_rate", () => {
  expect(buildTrainerUpdateSql("jra")).toContain(
    "filter (where history_keibajo_code = target_keibajo_code) as trainer_keibajo_win_rate",
  );
});

test("buildTrainerUpdateSql derives trainer_distance_win_rate with tolerance", () => {
  expect(buildTrainerUpdateSql("jra")).toContain(
    "filter (where abs(history_kyori - target_kyori) <= 200) as trainer_distance_win_rate",
  );
});

test("buildTrainerUpdateSql derives trainer_horse_win_rate", () => {
  expect(buildTrainerUpdateSql("jra")).toContain(
    "filter (where history_horse = target_horse) as trainer_horse_win_rate",
  );
});

test("buildTrainerUpdateSql does not emit jockey-only columns", () => {
  const sql = buildTrainerUpdateSql("jra");
  expect(sql).not.toContain("jockey_recent_win_rate");
  expect(sql).not.toContain("jockey_horse_pair_count");
});

test("buildSourceFeatureLookupIndexSqls returns idempotent jockey and trainer indexes", () => {
  const statements = buildSourceFeatureLookupIndexSqls();
  expect(statements.length).toBe(2);
  expect(statements.every((sql) => sql.includes("if not exists"))).toBe(true);
});

test("buildSourceFeatureLookupIndexSqls jockey index covers source/name/date", () => {
  const statements = buildSourceFeatureLookupIndexSqls();
  const jockeyIndex = statements.find((sql) => sql.includes("jockey_date_idx"));
  expect(jockeyIndex).toContain("(source, kishumei_ryakusho, race_date)");
  expect(jockeyIndex).toContain("where kishumei_ryakusho is not null");
});

test("buildSourceFeatureLookupIndexSqls trainer index covers source/name/date", () => {
  const statements = buildSourceFeatureLookupIndexSqls();
  const trainerIndex = statements.find((sql) => sql.includes("trainer_date_idx"));
  expect(trainerIndex).toContain("(source, chokyoshimei_ryakusho, race_date)");
});
