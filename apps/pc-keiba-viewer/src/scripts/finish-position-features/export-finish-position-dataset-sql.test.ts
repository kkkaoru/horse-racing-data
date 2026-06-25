import { expect, test } from "vitest";

import {
  buildExportSelectSql,
  EXPORT_COLUMN_ORDER,
  FEATURE_COLUMNS,
  FEATURE_TABLE,
  LABEL_COLUMNS,
  META_COLUMNS,
} from "./export-finish-position-dataset-sql";

test("FEATURE_TABLE points to the new finish-position features", () => {
  expect(FEATURE_TABLE).toBe("race_finish_position_features");
});

test("META_COLUMNS includes the identifier and target descriptors", () => {
  expect(META_COLUMNS).toContain("source");
  expect(META_COLUMNS).toContain("ketto_toroku_bango");
  expect(META_COLUMNS).toContain("category");
  expect(META_COLUMNS).toContain("kyori");
});

test("LABEL_COLUMNS exposes finish position and finish_norm", () => {
  expect(LABEL_COLUMNS).toStrictEqual(["finish_position", "finish_norm"]);
});

test("FEATURE_COLUMNS covers all 65 columns", () => {
  expect(FEATURE_COLUMNS.length).toBe(65);
});

test("FEATURE_COLUMNS exposes the deferred-then-completed columns", () => {
  expect(FEATURE_COLUMNS).toContain("weight_avg_5");
  expect(FEATURE_COLUMNS).toContain("weight_diff_from_avg");
  expect(FEATURE_COLUMNS).toContain("weather_normalized");
  expect(FEATURE_COLUMNS).toContain("track_bias_inside");
  expect(FEATURE_COLUMNS).toContain("track_bias_front");
});

test("FEATURE_COLUMNS exposes the three weight trend columns", () => {
  expect(FEATURE_COLUMNS).toContain("weight_trend_5");
  expect(FEATURE_COLUMNS).toContain("weight_volatility_5");
  expect(FEATURE_COLUMNS).toContain("weight_zscore");
});

test("FEATURE_COLUMNS exposes the five season-aware jockey columns", () => {
  expect(FEATURE_COLUMNS).toContain("jockey_season_win_rate");
  expect(FEATURE_COLUMNS).toContain("jockey_season_keibajo_win_rate");
  expect(FEATURE_COLUMNS).toContain("jockey_keibajo_distance_win_rate");
  expect(FEATURE_COLUMNS).toContain("jockey_season_keibajo_distance_win_rate");
  expect(FEATURE_COLUMNS).toContain("jockey_season_keibajo_distance_count");
});

test("FEATURE_COLUMNS exposes the three class/surface trainer columns", () => {
  expect(FEATURE_COLUMNS).toContain("trainer_grade_win_rate");
  expect(FEATURE_COLUMNS).toContain("trainer_class_surface_season_win_rate");
  expect(FEATURE_COLUMNS).toContain("trainer_class_surface_season_count");
});

test("FEATURE_COLUMNS preserves the heuristic five at the tail", () => {
  expect(FEATURE_COLUMNS.slice(-5)).toStrictEqual([
    "avg_finish",
    "recent_finish",
    "popularity_score",
    "odds_score",
    "same_day_jockey_win_score",
  ]);
});

test("EXPORT_COLUMN_ORDER places race_id between labels and features", () => {
  const raceIdIndex = EXPORT_COLUMN_ORDER.indexOf("race_id");
  expect(raceIdIndex).toBe(META_COLUMNS.length + LABEL_COLUMNS.length);
});

test("EXPORT_COLUMN_ORDER has no duplicates", () => {
  expect(new Set(EXPORT_COLUMN_ORDER).size).toBe(EXPORT_COLUMN_ORDER.length);
});

test("buildExportSelectSql narrows JRA", () => {
  expect(buildExportSelectSql("jra", "v1")).toContain("category = 'jra'");
});

test("buildExportSelectSql narrows NAR", () => {
  expect(buildExportSelectSql("nar", "v1")).toContain("category = 'nar'");
});

test("buildExportSelectSql narrows ban-ei", () => {
  expect(buildExportSelectSql("ban-ei", "v1")).toContain("category = 'ban-ei'");
});

test("buildExportSelectSql passes through when category is all", () => {
  const sql = buildExportSelectSql("all", "v1");
  expect(sql).toContain("and true");
  expect(sql).not.toContain("category = 'jra'");
});

test("buildExportSelectSql binds the race_date window", () => {
  expect(buildExportSelectSql("jra", "v1")).toContain("where race_date between $1 and $2");
});

test("buildExportSelectSql inlines the feature_schema_version literal", () => {
  expect(buildExportSelectSql("jra", "v1")).toContain("feature_schema_version = 'v1'");
});

test("buildExportSelectSql escapes single quotes in schema version", () => {
  expect(buildExportSelectSql("jra", "v'1")).toContain("feature_schema_version = 'v''1'");
});

test("buildExportSelectSql emits a synthetic race_id column", () => {
  expect(buildExportSelectSql("jra", "v1")).toContain(
    "source || ':' || kaisai_nen || ':' || kaisai_tsukihi || ':' || keibajo_code || ':' || race_bango",
  );
});

test("buildExportSelectSql sorts by race_date then race key then umaban", () => {
  expect(buildExportSelectSql("jra", "v1")).toContain(
    "order by race_date, source, keibajo_code, race_bango, umaban",
  );
});

test("buildExportSelectSql sources from race_finish_position_features", () => {
  expect(buildExportSelectSql("jra", "v1")).toContain("from race_finish_position_features");
});
