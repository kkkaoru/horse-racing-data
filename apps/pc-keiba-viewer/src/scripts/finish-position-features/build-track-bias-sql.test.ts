import { expect, test } from "vitest";

import {
  buildTrackBiasIndexSqls,
  buildTrackBiasUpdateSql,
  FRONT_CORNER_THRESHOLD,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
  TRACK_BIAS_WINDOW_DAYS,
} from "./build-track-bias-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("TRACK_BIAS_WINDOW_DAYS is five days", () => {
  expect(TRACK_BIAS_WINDOW_DAYS).toBe(5);
});

test("FRONT_CORNER_THRESHOLD treats top third of the field as front runners", () => {
  expect(FRONT_CORNER_THRESHOLD).toBe("0.33");
});

test("buildTrackBiasUpdateSql narrows JRA target and history", () => {
  const sql = buildTrackBiasUpdateSql("jra");
  expect(sql).toContain("target.category = 'jra'");
  expect(sql).toContain("history.source = 'jra'");
});

test("buildTrackBiasUpdateSql narrows NAR excluding ban-ei keibajo 83", () => {
  expect(buildTrackBiasUpdateSql("nar")).toContain(
    "history.source = 'nar' and history.keibajo_code <> '83'",
  );
});

test("buildTrackBiasUpdateSql narrows ban-ei to keibajo 83", () => {
  expect(buildTrackBiasUpdateSql("ban-ei")).toContain(
    "history.source = 'nar' and history.keibajo_code = '83'",
  );
});

test("buildTrackBiasUpdateSql joins history by keibajo only", () => {
  expect(buildTrackBiasUpdateSql("jra")).toContain("history.keibajo_code = target.keibajo_code");
});

test("buildTrackBiasUpdateSql enforces strict less-than race_date for leak prevention", () => {
  expect(buildTrackBiasUpdateSql("jra")).toContain("history.race_date < target.race_date");
});

test("buildTrackBiasUpdateSql limits the window to five days via to_date diff", () => {
  expect(buildTrackBiasUpdateSql("jra")).toContain("to_date(target.race_date, 'YYYYMMDD') - 5");
});

test("buildTrackBiasUpdateSql defines inside as umaban * 2 <= shusso_tosu + 1", () => {
  expect(buildTrackBiasUpdateSql("jra")).toContain("history.umaban * 2 <= history.shusso_tosu + 1");
});

test("buildTrackBiasUpdateSql defines front-runner as corner1_norm <= 0.33", () => {
  expect(buildTrackBiasUpdateSql("jra")).toContain("history.corner1_norm::numeric <= 0.33");
});

test("buildTrackBiasUpdateSql derives both bias columns", () => {
  const sql = buildTrackBiasUpdateSql("jra");
  expect(sql).toContain("track_bias_inside = bias.inside_win_rate");
  expect(sql).toContain("track_bias_front = bias.front_win_rate");
});

test("buildTrackBiasUpdateSql refreshes updated_at", () => {
  expect(buildTrackBiasUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildTrackBiasUpdateSql joins back to target via full primary key", () => {
  const sql = buildTrackBiasUpdateSql("jra");
  expect(sql).toContain("target.source = bias.source");
  expect(sql).toContain("target.ketto_toroku_bango = bias.ketto_toroku_bango");
});

test("buildTrackBiasIndexSqls returns an idempotent partial index", () => {
  const statements = buildTrackBiasIndexSqls();
  expect(statements.length).toBe(1);
  const idx = statements[0];
  expect(idx).toBeDefined();
  if (idx === undefined) return;
  expect(idx).toContain("if not exists");
  expect(idx).toContain("(source, keibajo_code, race_date)");
  expect(idx).toContain("where finish_position is not null");
});
