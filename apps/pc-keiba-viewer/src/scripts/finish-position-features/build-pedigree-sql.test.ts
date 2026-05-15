import { expect, test } from "vitest";

import {
  buildPedigreeBinding,
  buildPedigreeUpdateSql,
  DISTANCE_BAND_WIDTH_METERS,
  MIN_RACES_FOR_RELIABLE_STAT,
  PEDIGREE_COMPOSITE_DIVISOR,
  SOURCE_FEATURE_TABLE,
  TARGET_FEATURE_TABLE,
} from "./build-pedigree-sql";

test("SOURCE_FEATURE_TABLE points to the aggregated source", () => {
  expect(SOURCE_FEATURE_TABLE).toBe("race_entry_corner_features");
});

test("TARGET_FEATURE_TABLE points to the finish-position features", () => {
  expect(TARGET_FEATURE_TABLE).toBe("race_finish_position_features");
});

test("DISTANCE_BAND_WIDTH_METERS bins distances at 400m", () => {
  expect(DISTANCE_BAND_WIDTH_METERS).toBe(400);
});

test("MIN_RACES_FOR_RELIABLE_STAT guards rare-bucket noise at five races", () => {
  expect(MIN_RACES_FOR_RELIABLE_STAT).toBe(5);
});

test("PEDIGREE_COMPOSITE_DIVISOR averages three pedigree signals", () => {
  expect(PEDIGREE_COMPOSITE_DIVISOR).toBe(3);
});

test("buildPedigreeBinding returns the JRA binding", () => {
  const binding = buildPedigreeBinding("jra");
  expect(binding).toStrictEqual({
    horseMasterTable: "jvd_um",
    sourceFilter: "source = 'jra'",
    targetFilter: "target.category = 'jra'",
  });
});

test("buildPedigreeBinding returns null for NAR (deferred)", () => {
  expect(buildPedigreeBinding("nar")).toBe(null);
});

test("buildPedigreeBinding returns null for ban-ei (deferred)", () => {
  expect(buildPedigreeBinding("ban-ei")).toBe(null);
});

test("buildPedigreeBinding returns null for all (no single master table)", () => {
  expect(buildPedigreeBinding("all")).toBe(null);
});

test("buildPedigreeUpdateSql is a no-op when category has no binding", () => {
  const sql = buildPedigreeUpdateSql("nar");
  expect(sql).toContain("where false");
  expect(sql).not.toContain("sire_distance_stats");
});

test("buildPedigreeUpdateSql joins jvd_um for JRA", () => {
  const sql = buildPedigreeUpdateSql("jra");
  expect(sql).toContain("join jvd_um um");
  expect(sql).toContain("rec.source = 'jra'");
});

test("buildPedigreeUpdateSql bins kyori at 400m wide bands", () => {
  const sql = buildPedigreeUpdateSql("jra");
  expect(sql).toContain("(coalesce(rec.kyori, 0) / 400)::int");
});

test("buildPedigreeUpdateSql derives surface from track_code first digit", () => {
  expect(buildPedigreeUpdateSql("jra")).toContain("left(coalesce(rec.track_code, ''), 1)");
});

test("buildPedigreeUpdateSql groups sire_distance_stats by sire and band", () => {
  expect(buildPedigreeUpdateSql("jra")).toContain(
    "group by um.ketto_joho_01b, (coalesce(rec.kyori, 0) / 400)::int",
  );
});

test("buildPedigreeUpdateSql derives damsire stats from ketto_joho_05b", () => {
  expect(buildPedigreeUpdateSql("jra")).toContain("um.ketto_joho_05b as damsire");
});

test("buildPedigreeUpdateSql guards pedigree values by race count >= 5", () => {
  const sql = buildPedigreeUpdateSql("jra");
  expect(sql).toContain(
    "case when sds.race_count >= 5 then sds.sire_distance_win_rate_val else null end",
  );
});

test("buildPedigreeUpdateSql composite score averages three signals over 3", () => {
  expect(buildPedigreeUpdateSql("jra")).toContain(
    "((coalesce(sds.sire_distance_win_rate_val, 0)\n       + coalesce(dsd.dam_sire_distance_win_rate_val, 0)\n       + coalesce(sts.sire_track_win_rate_val, 0))\n      / 3::numeric)",
  );
});

test("buildPedigreeUpdateSql updates all six pedigree columns", () => {
  const sql = buildPedigreeUpdateSql("jra");
  expect(sql).toContain("sire_distance_win_rate = ");
  expect(sql).toContain("sire_track_win_rate = ");
  expect(sql).toContain("dam_sire_distance_win_rate = ");
  expect(sql).toContain("sire_avg_finish_at_distance = ");
  expect(sql).toContain("damsire_avg_finish_at_track = ");
  expect(sql).toContain("pedigree_score_for_race = ");
});

test("buildPedigreeUpdateSql restricts target to JRA category", () => {
  expect(buildPedigreeUpdateSql("jra")).toContain("target.category = 'jra'");
});

test("buildPedigreeUpdateSql joins to target via full primary key", () => {
  const sql = buildPedigreeUpdateSql("jra");
  expect(sql).toContain("target.source = twp.source");
  expect(sql).toContain("target.ketto_toroku_bango = twp.ketto_toroku_bango");
});

test("buildPedigreeUpdateSql ignores empty sire names", () => {
  const sql = buildPedigreeUpdateSql("jra");
  expect(sql).toContain("um.ketto_joho_01b is not null");
  expect(sql).toContain("btrim(um.ketto_joho_01b) <> ''");
});
