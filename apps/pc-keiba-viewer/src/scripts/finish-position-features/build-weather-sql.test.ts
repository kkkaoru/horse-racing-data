import { expect, test } from "vitest";

import {
  buildWeatherBinding,
  buildWeatherUpdateSql,
  TARGET_FEATURE_TABLE,
  TENKO_CODE_TO_WEATHER_VALUE,
} from "./build-weather-sql";

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("TENKO_CODE_TO_WEATHER_VALUE maps clear..snow onto a 0..1 scale", () => {
  expect(TENKO_CODE_TO_WEATHER_VALUE).toStrictEqual({
    "1": "0",
    "2": "0.3",
    "3": "0.7",
    "4": "0.7",
    "5": "1.0",
    "6": "1.0",
  });
});

test("buildWeatherBinding returns jvd_ra for JRA", () => {
  expect(buildWeatherBinding("jra")).toStrictEqual({
    raceMasterTable: "jvd_ra",
    targetFilter: "target.category = 'jra'",
  });
});

test("buildWeatherBinding returns nvd_ra for NAR", () => {
  expect(buildWeatherBinding("nar")).toStrictEqual({
    raceMasterTable: "nvd_ra",
    targetFilter: "target.category = 'nar'",
  });
});

test("buildWeatherBinding returns nvd_ra for ban-ei", () => {
  expect(buildWeatherBinding("ban-ei")).toStrictEqual({
    raceMasterTable: "nvd_ra",
    targetFilter: "target.category = 'ban-ei'",
  });
});

test("buildWeatherBinding returns null when category is all", () => {
  expect(buildWeatherBinding("all")).toBe(null);
});

test("buildWeatherUpdateSql is a no-op when binding is null", () => {
  const sql = buildWeatherUpdateSql("all");
  expect(sql).toContain("where false");
  expect(sql).not.toContain("weather_lookup");
});

test("buildWeatherUpdateSql joins jvd_ra for JRA", () => {
  const sql = buildWeatherUpdateSql("jra");
  expect(sql).toContain("join jvd_ra ra");
  expect(sql).toContain("target.category = 'jra'");
});

test("buildWeatherUpdateSql joins nvd_ra for NAR", () => {
  expect(buildWeatherUpdateSql("nar")).toContain("join nvd_ra ra");
});

test("buildWeatherUpdateSql maps tenko_code via case expression", () => {
  const sql = buildWeatherUpdateSql("jra");
  expect(sql).toContain("case weather_lookup.tenko_code");
  expect(sql).toContain("when '1' then 0::numeric");
  expect(sql).toContain("when '3' then 0.7::numeric");
  expect(sql).toContain("when '5' then 1.0::numeric");
});

test("buildWeatherUpdateSql joins target to ra by race PK", () => {
  const sql = buildWeatherUpdateSql("jra");
  expect(sql).toContain("ra.kaisai_nen = target.kaisai_nen");
  expect(sql).toContain("ra.race_bango = target.race_bango");
});

test("buildWeatherUpdateSql binds the race_date window", () => {
  expect(buildWeatherUpdateSql("jra")).toContain("target.race_date between $1 and $2");
});

test("buildWeatherUpdateSql refreshes updated_at", () => {
  expect(buildWeatherUpdateSql("jra")).toContain("updated_at = now()");
});

test("buildWeatherUpdateSql joins back to target via full primary key", () => {
  const sql = buildWeatherUpdateSql("jra");
  expect(sql).toContain("target.source = weather_lookup.source");
  expect(sql).toContain("target.ketto_toroku_bango = weather_lookup.ketto_toroku_bango");
});
