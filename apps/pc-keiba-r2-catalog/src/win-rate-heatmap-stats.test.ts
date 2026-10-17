import { buildArcBloodlineLabelsQuery } from "./win-rate-heatmap-stats";
// Runs with bun via Vitest.
import { expect, it } from "vitest";
import { DuckDBInstance } from "@duckdb/node-api";

import type { R2SqlCatalogConfig, WinRateHeatmapStatsFilters } from "./types";
import {
  buildWinRateHeatmapBloodlineQuery,
  buildWinRateHeatmapDayKnownBloodlineQuery,
  buildWinRateHeatmapKnownBloodlineCountQuery,
  dayKnownBloodlineKey,
  usesDayKnownBloodlineCount,
  buildWinRateHeatmapSimilarQuery,
  cellRouterDistanceBandFilterSql,
  currentRaceIdentitySql,
  isBanEiKeibajo,
  trackSurfaceSql,
  normaliseBloodlineRow,
  normaliseSimilarRow,
  normaliseWinRateHeatmapStatsPayload,
} from "./win-rate-heatmap-stats";

const config: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "token",
};

const jraFilters: WinRateHeatmapStatsFilters = {
  date: "20260715",
  includeDistance: true,
  includeSurface: true,
  includeTurn: true,
  includeVenue: true,
  keibajoCode: "05",
  raceBango: "01",
  source: "jra",
  years: 10,
};

it("qualifies every runner identity field when joins share those fields", () => {
  expect(currentRaceIdentitySql(jraFilters, "se")).toBe(
    "se.kaisai_nen = '2026'\n    AND se.kaisai_tsukihi = '0715'\n    AND se.keibajo_code = '05'\n    AND se.race_bango = '01'",
  );
});

it("preserves unqualified identity for single-table selectors", () => {
  expect(currentRaceIdentitySql(jraFilters)).toBe(
    "kaisai_nen = '2026'\n    AND kaisai_tsukihi = '0715'\n    AND keibajo_code = '05'\n    AND race_bango = '01'",
  );
});

it("shares the surface classifier with partnership history queries", () => {
  expect(trackSurfaceSql("ra.track_code")).toMatch("THEN '芝'");
  expect(trackSurfaceSql("ra.track_code")).toMatch("THEN 'ダート'");
  expect(trackSurfaceSql("ra.track_code")).toMatch("THEN '障害'");
  expect(trackSurfaceSql("ra.track_code")).toMatch("ELSE ''");
});

it("uses R2 SQL regexp_match null checks rather than unsupported DuckDB regexp_matches", () => {
  const sql = buildWinRateHeatmapBloodlineQuery(config, jraFilters);
  expect(sql).not.toContain("regexp_matches(");
  expect(sql).toContain("regexp_match(");
  for (const grade of ["A", "B", "C"]) expect(sql).toContain(`IS NOT NULL THEN '${grade}'`);
});

it("keeps all provider master values ahead of verified pedigree supplements", () => {
  const sql = buildWinRateHeatmapBloodlineQuery(config, jraFilters);
  const provider = sql.indexOf("tertiary_um.ketto_joho_03b");
  const supplement = sql.indexOf("CASE WHEN se.ketto_toroku_bango = '2020190007'");
  expect(provider).toBeGreaterThan(-1);
  expect(supplement).toBeGreaterThan(provider);
  expect(sql).toContain("THEN 'Sadler''s Wells' ELSE NULL END");
  expect(sql).toContain("primary_um.ketto_joho_02b");
});

it("builds aggregate-only JRA bloodline SQL with heatmap default similar-section filters", () => {
  const sql = buildWinRateHeatmapBloodlineQuery(config, jraFilters);
  expect(sql).toMatch("FROM pc_keiba.jvd_se");
  expect(sql).toMatch("INNER JOIN pc_keiba.jvd_ra ra");
  expect(sql).toMatch("FROM pc_keiba.jvd_ra");
  expect(sql).toMatch("LEFT JOIN pc_keiba.jvd_um primary_um");
  expect(sql).toMatch("LEFT JOIN pc_keiba.nvd_um secondary_um");
  expect(sql).toMatch("LEFT JOIN pc_keiba.nvd_nu tertiary_um");
  expect(sql).toMatch("ketto_joho_01b");
  expect(sql).toMatch("ketto_joho_03b");
  expect(sql).toMatch("ketto_joho_05b");
  expect(sql).toMatch("ketto_joho_07b");
  expect(sql).toMatch("ketto_joho_09b");
  expect(sql).toMatch("ketto_joho_11b");
  expect(sql).toMatch("ketto_joho_13b");
  expect(sql).toMatch("'sire' AS category");
  expect(sql).toMatch("'sireSire' AS category");
  expect(sql).toMatch("'damSire' AS category");
  expect(sql).toMatch("'sireSireSire' AS category");
  expect(sql).toMatch("'sireDamSire' AS category");
  expect(sql).toMatch("'damSireSire' AS category");
  expect(sql).toMatch("'damDamSire' AS category");
  expect(sql).toMatch("kaisai_nen = '2026'");
  expect(sql).toMatch("kaisai_tsukihi = '0715'");
  expect(sql).toMatch("keibajo_code = '05'");
  expect(sql).toMatch("race_bango = '01'");
  expect(sql).toMatch("concat(ra.kaisai_nen, ra.kaisai_tsukihi) < '20260715'");
  expect(sql).toMatch("concat(ra.kaisai_nen, ra.kaisai_tsukihi) >= '20160715'");
  expect(sql).toMatch("AND ra.keibajo_code = '05'");
  expect(sql).toMatch("cr.kyori_int IS NOT NULL");
  expect(sql).toMatch("THEN '芝'");
  expect(sql).toMatch("THEN 'ダート'");
  expect(sql).toMatch("THEN 'サンド'");
  expect(sql).toMatch("THEN '障害'");
  expect(sql).toMatch("THEN '左'");
  expect(sql).toMatch("THEN '右'");
  expect(sql).toMatch("THEN '直線'");
  expect(sql).toMatch("END = ''");
  expect(sql).toMatch("replace(coalesce(primary_um.ketto_joho_01b, ''), chr(12288), '')");
  expect(sql).toMatch("count(*) AS starts");
  expect(sql).toMatch("sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS wins");
  expect(sql).toMatch("sum(CASE WHEN finish_position IN (1, 2) THEN 1 ELSE 0 END) AS places");
  expect(sql).toMatch("sum(CASE WHEN finish_position IN (1, 2, 3) THEN 1 ELSE 0 END) AS shows");
  expect(sql).toMatch("GROUP BY category, name");
  expect(sql).not.toMatch("jsonb_agg");
  expect(sql).not.toMatch("regexp_replace");
  expect(sql).not.toMatch("nvd_se");
});

it("builds finish-position cell-router distance bands for JRA and NAR", () => {
  const jraBand = cellRouterDistanceBandFilterSql(jraFilters);
  const narBand = cellRouterDistanceBandFilterSql({
    ...jraFilters,
    keibajoCode: "43",
    source: "nar",
  });
  expect(jraBand).toMatch("WHEN cr.kyori_int < 1200 THEN 'sprint'");
  expect(jraBand).toMatch("WHEN cr.kyori_int < 1600 THEN 'mile'");
  expect(narBand).toMatch("WHEN cr.kyori_int < 1400 THEN 'sprint'");
  expect(narBand).toMatch("WHEN cr.kyori_int <= 1500 THEN 'extended_sprint'");
  expect(narBand).toMatch("WHEN cr.kyori_int <= 1800 THEN 'mile'");
});

it("treats historic Ban-ei venues as the same circuit", () => {
  expect(isBanEiKeibajo("81")).toBe(true);
  expect(isBanEiKeibajo("82")).toBe(true);
  expect(isBanEiKeibajo("83")).toBe(true);
  expect(isBanEiKeibajo("84")).toBe(true);
  expect(isBanEiKeibajo("05")).toBe(false);
});

it("builds NAR similar SQL and omits optional similar-section filters when they are off", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    date: "20260715",
    includeDistance: false,
    includeOwner: false,
    includeSurface: false,
    includeTurn: false,
    includeVenue: true,
    keibajoCode: "83",
    raceBango: "09",
    source: "nar",
    years: 5,
  });
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.nvd_ra ra");
  expect(sql).toMatch("FROM pc_keiba.nvd_ra");
  expect(sql).toMatch("'jockey' AS kind");
  expect(sql).toMatch("'trainer' AS kind");
  expect(sql).toMatch("kishumei_ryakusho");
  expect(sql).toMatch("chokyoshimei_ryakusho");
  expect(sql).toMatch("concat(ra.kaisai_nen, ra.kaisai_tsukihi) >= '20210715'");
  expect(sql).toMatch("AND ra.keibajo_code IN ('81', '82', '83', '84')");
  expect(sql).not.toMatch("AND ra.keibajo_code = '83'");
  expect(sql).not.toMatch("cr.kyori_int IS NOT NULL");
  expect(sql).not.toMatch("THEN '芝'");
  expect(sql).not.toMatch("THEN '左'");
  expect(sql).not.toMatch("jvd_se");
  expect(sql).not.toMatch("jsonb_agg");
  expect(sql).not.toMatch("regexp_replace");
  expect(sql).not.toMatch("'owner' AS kind");
  expect(sql).not.toMatch("banushimei");
  expect(sql).not.toMatch("'jockeyFrame' AS kind");
  expect(sql).not.toMatch("wakuban AS frame");
  expect(sql).not.toMatch("GROUP BY kind, name, frame");
  expect(sql).toMatch("GROUP BY kind, name");
});

it("adds owner matching to similar SQL when includeOwner is enabled", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    includeOwner: true,
  });
  expect(sql).toMatch("'owner' AS kind");
  expect(sql).toMatch("banushimei");
  expect(sql).toMatch("tn.kind = 'owner'");
  expect(sql).toMatch("'jockey' AS kind");
  expect(sql).toMatch("'trainer' AS kind");
  expect(sql).not.toMatch("'jockeyFrame' AS kind");
});

it("adds jockey-in-frame matching to similar SQL when includeJockeyFrame is enabled", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    includeJockeyFrame: true,
  });
  expect(sql).toMatch("'jockeyFrame' AS kind");
  expect(sql).toMatch("wakuban AS frame");
  expect(sql).toMatch("lpad(btrim(coalesce(se.wakuban, '')), 2, '0')");
  expect(sql).toMatch("tn.kind = 'jockeyFrame'");
  expect(sql).toMatch("WHERE jockey <> '不明' AND wakuban <> '' AND wakuban <> '00'");
  expect(sql).toMatch("GROUP BY kind, name, frame");
  expect(sql).toMatch("coalesce(stats.frame, '') = coalesce(cp.frame, '')");
});

it("filters similar history by current-race grade and track when those flags are on", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    includeGrade: true,
    includeSurface: false,
    includeTrackCode: true,
    includeTurn: false,
  });
  expect(sql).toMatch("coalesce(ra.grade_code, '')");
  expect(sql).toMatch("coalesce(cr.grade_code, '')");
  expect(sql).toMatch("'JPN[[:space:]]*III([^I]|$)') IS NOT NULL THEN 'C'");
  expect(sql).toMatch("'JPN[[:space:]]*II([^I]|$)') IS NOT NULL THEN 'B'");
  expect(sql).toMatch("'JPN[[:space:]]*I([^I]|$)') IS NOT NULL THEN 'A'");
  expect(sql).toMatch("END = CASE");
  expect(sql).toMatch("btrim(coalesce(ra.track_code, '')) = btrim(coalesce(cr.track_code, ''))");
  expect(sql).not.toMatch("THEN '芝'");
});

it("lets includeGrade skip matching when the current grade is not listed-or-higher", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    includeGrade: true,
    includeSurface: false,
    includeTurn: false,
  });
  expect(sql).toMatch("END NOT IN ('A', 'B', 'C', 'D', 'F', 'G', 'H', 'L', 'S')");
  expect(sql).toMatch("'JPN[[:space:]]*III([^I]|$)') IS NOT NULL THEN 'C'");
  expect(sql).toMatch("END = CASE");
});

it("omits grade and exact track-code matching when those flags are off", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, jraFilters);
  expect(sql).not.toMatch(
    "btrim(coalesce(ra.grade_code, '')) = btrim(coalesce(cr.grade_code, ''))",
  );
  expect(sql).not.toMatch(
    "btrim(coalesce(ra.track_code, '')) = btrim(coalesce(cr.track_code, ''))",
  );
});

it("always excludes graded races from ungraded open current races", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, jraFilters);
  expect(sql).toMatch("kyoso_joken_code");
  expect(sql).toMatch("kyoso_joken_meisho");
  expect(sql).toMatch("kyosomei_hondai");
  expect(sql).toMatch("kyoso_shubetsu_code");
  expect(sql).toMatch("btrim(coalesce(cr.kyoso_joken_code, '')) <> '999'");
  expect(sql).toMatch("'JPN[[:space:]]*III([^I]|$)') IS NOT NULL THEN 'C'");
  expect(sql).toMatch("OR CASE");
});

it("compares condition keys as strings before combining the predicate with boolean filters", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    includeAge: true,
    includeClass: true,
    includeConditionKey: true,
    includeRaceTitle: true,
    includeSurface: false,
    includeTurn: false,
  });
  expect(sql).toMatch(
    "btrim(coalesce(ra.kyoso_shubetsu_code, '')) = btrim(coalesce(cr.kyoso_shubetsu_code, ''))",
  );
  expect(sql).toMatch(
    "btrim(coalesce(ra.kyoso_joken_code, '')) = btrim(coalesce(cr.kyoso_joken_code, ''))",
  );
  expect(sql).toMatch("WHEN btrim(coalesce(ra.kyoso_joken_code, '')) = '005' THEN '1勝クラス'");
  expect(sql).toMatch("WHEN btrim(coalesce(ra.kyoso_joken_code, '')) = '999' THEN 'オープン'");
  expect(sql).toMatch(
    "ELSE nullif(split_part(btrim(coalesce(ra.kyoso_joken_meisho, '')), ' ', 1), '')",
  );
  expect(sql).toMatch(
    "END = CASE\n      WHEN btrim(coalesce(cr.kyoso_joken_code, '')) = '005' THEN '1勝クラス'",
  );
  expect(sql).toMatch("END IS NOT NULL\n    AND (");
  expect(sql).not.toMatch("IS NOT DISTINCT FROM");
  // Non-G1 named races must not be rejected merely because the UI enables title matching.
  // G1 targets still require G1 history as well as an exact title match.
  expect(sql).toMatch(
    "btrim(coalesce(cr.grade_code, '')) NOT IN ('A', 'F')\n      OR btrim(coalesce(ra.grade_code, '')) IN ('A', 'F')",
  );
  expect(sql).toMatch("btrim(coalesce(ra.grade_code, '')) IN ('A', 'F')");
  expect(sql).toMatch("cr.kyosomei_hondai");
  expect(sql).not.toMatch("THEN '芝'");
});

it("omits age, class, condition key, and race title matching when those flags are off", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, jraFilters);
  expect(sql).not.toMatch(
    "btrim(coalesce(ra.kyoso_shubetsu_code, '')) = btrim(coalesce(cr.kyoso_shubetsu_code, ''))",
  );
  expect(sql).not.toMatch(
    "btrim(coalesce(ra.kyoso_joken_code, '')) = btrim(coalesce(cr.kyoso_joken_code, ''))",
  );
  expect(sql).not.toMatch("THEN '1勝クラス'");
  expect(sql).not.toMatch("IN ('A', 'F')");
});

it("keeps owner rows with an empty frame when includeOwner and includeJockeyFrame are both on", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    includeJockeyFrame: true,
    includeOwner: true,
  });
  expect(sql).toMatch("SELECT umaban, 'owner' AS kind, owner AS name, '' AS frame");
  expect(sql).toMatch("'jockeyFrame' AS kind");
  expect(sql).toMatch("tn.kind = 'owner'");
  expect(sql).toMatch("tn.kind = 'jockeyFrame'");
});

it("unions JRA and NAR history when venue matching is off", () => {
  const bloodline = buildWinRateHeatmapBloodlineQuery(config, {
    date: "20260715",
    includeDistance: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: false,
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    years: 10,
  });
  const similar = buildWinRateHeatmapSimilarQuery(config, {
    date: "20260715",
    includeDistance: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: false,
    keibajoCode: "05",
    raceBango: "01",
    source: "nar",
    years: 10,
  });
  expect(bloodline).toMatch("FROM pc_keiba.jvd_se se");
  expect(bloodline).toMatch("FROM pc_keiba.nvd_se se");
  expect(bloodline).toMatch("UNION ALL");
  expect(bloodline).not.toMatch("AND ra.keibajo_code = '05'");
  expect(similar).toMatch("FROM pc_keiba.jvd_se se");
  expect(similar).toMatch("FROM pc_keiba.nvd_se se");
  expect(similar).toMatch("UNION ALL");
  expect(similar).not.toMatch("AND ra.keibajo_code = '05'");
});

it("builds overseas heatmap queries without changing the venue code", () => {
  const filters: WinRateHeatmapStatsFilters = {
    ...jraFilters,
    date: "20261004",
    keibajoCode: "A8",
    raceBango: "05",
  };
  expect(buildWinRateHeatmapBloodlineQuery(config, filters)).toMatch("keibajo_code = 'A8'");
  expect(buildWinRateHeatmapSimilarQuery(config, filters)).toMatch("keibajo_code = 'A8'");
  expect(buildWinRateHeatmapKnownBloodlineCountQuery(config, filters)).toMatch(
    "keibajo_code = 'A8'",
  );
});

it.each(["a8", "A8'", "A8 OR 1=1", "../"])(
  "rejects malformed overseas venue codes: %s",
  (keibajoCode) => {
    expect(() => buildWinRateHeatmapSimilarQuery(config, { ...jraFilters, keibajoCode })).toThrow(
      "keibajoCode must contain two uppercase alphanumeric characters",
    );
  },
);

it("does not accept an overseas venue code as a race number", () => {
  expect(() =>
    buildWinRateHeatmapBloodlineQuery(config, { ...jraFilters, raceBango: "A8" }),
  ).toThrow("raceBango must contain two digits");
});

it("rejects unsafe namespace, dates, codes, and year windows", () => {
  expect(() =>
    buildWinRateHeatmapBloodlineQuery({ ...config, R2_SQL_NAMESPACE: "pc_keiba;drop" }, jraFilters),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  expect(() =>
    buildWinRateHeatmapSimilarQuery(config, { ...jraFilters, date: "2026-07-15" }),
  ).toThrow("date must match YYYYMMDD");
  expect(() =>
    buildWinRateHeatmapBloodlineQuery(config, { ...jraFilters, date: "20260231" }),
  ).toThrow("date must be a valid calendar date");
  expect(() =>
    buildWinRateHeatmapSimilarQuery(config, { ...jraFilters, keibajoCode: "5" }),
  ).toThrow("keibajoCode must contain two uppercase alphanumeric characters");
  expect(() =>
    buildWinRateHeatmapBloodlineQuery(config, { ...jraFilters, raceBango: "1;" }),
  ).toThrow("raceBango must contain two digits");
  expect(() => buildWinRateHeatmapSimilarQuery(config, { ...jraFilters, years: 0 })).toThrow(
    "years must be an integer from 1 to 50",
  );
  expect(() => buildWinRateHeatmapBloodlineQuery(config, { ...jraFilters, years: 51 })).toThrow(
    "years must be an integer from 1 to 50",
  );
  expect(() => buildWinRateHeatmapSimilarQuery(config, { ...jraFilters, years: 1.5 })).toThrow(
    "years must be an integer from 1 to 50",
  );
});

it("maps aggregate bloodline and similar rows and always clears details", () => {
  expect(
    normaliseWinRateHeatmapStatsPayload({
      bloodlineRows: [
        {
          category: "sire",
          name: true,
          places: "3",
          shows: 4n,
          starts: 10,
          umaban: "7",
          wins: 1,
        },
      ],
      similarRows: [
        {
          kind: "jockey",
          name: "Take",
          places: 2,
          shows: "3",
          starts: 8n,
          umaban: 1,
          wins: 1,
        },
      ],
    }),
  ).toStrictEqual({
    bloodlineRows: [
      {
        category: "sire",
        details: [],
        name: "true",
        places: 3,
        shows: 4,
        starts: 10,
        umaban: 7,
        wins: 1,
      },
    ],
    similarRows: [
      {
        details: [],
        kind: "jockey",
        name: "Take",
        places: 2,
        shows: 3,
        starts: 8,
        umaban: 1,
        wins: 1,
      },
    ],
  });
});

it("accepts every bloodline category and both similar kinds", () => {
  expect(
    normaliseBloodlineRow({
      category: "sireSire",
      name: "Sunday Silence",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 2,
      wins: 0,
    }).category,
  ).toBe("sireSire");
  expect(
    normaliseBloodlineRow({
      category: "damSire",
      name: "Halo",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 2,
      wins: 0,
    }).category,
  ).toBe("damSire");
  expect(
    normaliseBloodlineRow({
      category: "sireSireSire",
      name: "Hail to Reason",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 2,
      wins: 0,
    }).category,
  ).toBe("sireSireSire");
  expect(
    normaliseBloodlineRow({
      category: "sireDamSire",
      name: "Nureyev",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 2,
      wins: 0,
    }).category,
  ).toBe("sireDamSire");
  expect(
    normaliseBloodlineRow({
      category: "damSireSire",
      name: "Northern Dancer",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 2,
      wins: 0,
    }).category,
  ).toBe("damSireSire");
  expect(
    normaliseBloodlineRow({
      category: "damDamSire",
      name: "Mr. Prospector",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 2,
      wins: 0,
    }).category,
  ).toBe("damDamSire");
  expect(
    normaliseSimilarRow({
      kind: "trainer",
      name: "Fujisawa",
      places: 1,
      shows: 1,
      starts: 1,
      umaban: 3,
      wins: 0,
    }).kind,
  ).toBe("trainer");
  expect(
    normaliseSimilarRow({
      kind: "jockey",
      name: 8,
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 4,
      wins: 0,
    }).name,
  ).toBe("8");
  expect(
    normaliseSimilarRow({
      kind: "jockey",
      name: 8n,
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 4,
      wins: 0,
    }).name,
  ).toBe("8");
  expect(
    normaliseSimilarRow({
      kind: "owner",
      name: "Kaneko",
      places: 2,
      shows: 3,
      starts: 9,
      umaban: 5,
      wins: 1,
    }).kind,
  ).toBe("owner");
  expect(
    normaliseSimilarRow({
      kind: "jockeyFrame",
      name: "Take",
      places: 4,
      shows: 6,
      starts: 12,
      umaban: 6,
      wins: 2,
    }).kind,
  ).toBe("jockeyFrame");
});

it("rejects incomplete or invalid aggregate rows", () => {
  expect(() =>
    normaliseBloodlineRow({
      category: "owner",
      name: "Deep Impact",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 1,
      wins: 0,
    }),
  ).toThrow("R2 SQL row has invalid category: owner");
  expect(() =>
    normaliseSimilarRow({
      kind: "sire",
      name: "Take",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 1,
      wins: 0,
    }),
  ).toThrow("R2 SQL row has invalid kind: sire");
  expect(() =>
    normaliseBloodlineRow({
      category: "",
      name: "Deep Impact",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 1,
      wins: 0,
    }),
  ).toThrow("R2 SQL row is missing category");
  expect(() =>
    normaliseSimilarRow({
      kind: "jockey",
      name: "",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 1,
      wins: 0,
    }),
  ).toThrow("R2 SQL row is missing name");
  expect(() =>
    normaliseBloodlineRow({
      category: "sire",
      name: "Deep Impact",
      places: 1.5,
      shows: 0,
      starts: 0,
      umaban: 1,
      wins: 0,
    }),
  ).toThrow("R2 SQL row is missing places");
  expect(() =>
    normaliseSimilarRow({
      kind: "jockey",
      name: "Take",
      places: 0,
      shows: Number.NaN,
      starts: 0,
      umaban: 1,
      wins: 0,
    }),
  ).toThrow("R2 SQL row is missing shows");
  expect(() =>
    normaliseBloodlineRow({
      category: "sire",
      name: "Deep Impact",
      places: 0,
      shows: 0,
      starts: "nope",
      umaban: 1,
      wins: 0,
    }),
  ).toThrow("R2 SQL row is missing starts");
  expect(() =>
    normaliseSimilarRow({
      kind: "jockey",
      name: "Take",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: null,
      wins: 0,
    }),
  ).toThrow("R2 SQL row is missing umaban");
  expect(() =>
    normaliseBloodlineRow({
      category: "sire",
      name: "Deep Impact",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 1,
      wins: {},
    }),
  ).toThrow("R2 SQL row is missing wins");
  expect(() =>
    normaliseSimilarRow({
      kind: "jockey",
      name: "Take",
      places: 0,
      shows: 0,
      starts: 0,
      umaban: 1,
      wins: 2n ** 1024n,
    }),
  ).toThrow("R2 SQL row is missing wins");
});

it("counts the runners' known pedigree names without scanning race history", () => {
  const sql = buildWinRateHeatmapKnownBloodlineCountQuery(config, jraFilters);
  expect(sql.trimEnd().endsWith("SELECT count(*) AS known\nFROM current_bloodlines")).toBe(true);
  expect(/current_bloodlines AS \(/u.test(sql)).toBe(true);
  expect(/matched_history|current_race AS/u.test(sql)).toBe(false);
});

it("counts known pedigree names for every race of the source on one date", () => {
  const sql = buildWinRateHeatmapDayKnownBloodlineQuery(config, { ...jraFilters, source: "nar" });
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("WHERE se.kaisai_nen = '2026'\n    AND se.kaisai_tsukihi = '0715'");
  expect(sql).toMatch("sum(CASE WHEN sire <> '不明' THEN 1 ELSE 0 END)");
  expect(sql).toMatch("sum(CASE WHEN damDamSire <> '不明' THEN 1 ELSE 0 END) AS known");
  expect(sql).toMatch("GROUP BY keibajo_code, race_bango");
  expect(/matched_history|current_race AS|race_bango = '01'/u.test(sql)).toBe(false);
  expect(dayKnownBloodlineKey("05", "11")).toBe("05:11");
});

it("keeps current-profile (Arc) races on the per-race pedigree count", () => {
  expect(usesDayKnownBloodlineCount(jraFilters)).toBe(true);
  expect(
    usesDayKnownBloodlineCount({
      ...jraFilters,
      date: "20261004",
      keibajoCode: "A8",
      raceBango: "05",
    }),
  ).toBe(false);
});

it("uses provider-qualified full gates for both current targets and eligible overseas history", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    keibajoCode: "A8",
    includeJockeyFrame: true,
  });
  expect(
    sql.match(/LEFT JOIN pc_keiba.oversea_runner_source_id gate_mapping/gu)?.length,
  ).toBeGreaterThanOrEqual(2);
  expect(sql).toMatch("gate_mapping.race_source = 'jra' AND gate_mapping.source = 'netkeiba'");
  expect(sql).toMatch("gate_mapping.kaisai_nen = se.kaisai_nen");
  expect(sql).toMatch("gate_mapping.kaisai_tsukihi = se.kaisai_tsukihi");
  expect(sql).toMatch("gate_mapping.keibajo_code = se.keibajo_code");
  expect(sql).toMatch("gate_mapping.race_bango = se.race_bango");
  expect(sql).toMatch("gate_mapping.umaban = se.umaban");
  expect(sql).toMatch("cast(gate_mapping.gate_number AS VARCHAR)");
  expect(sql).toMatch("se.keibajo_code = cr.keibajo_code");
  expect(sql).not.toMatch("se.wakuban");
});
it("does not attach overseas gate mappings when jockeyFrame is unrequested", () => {
  expect(
    buildWinRateHeatmapSimilarQuery(config, {
      ...jraFilters,
      keibajoCode: "A8",
      includeJockeyFrame: false,
    }),
  ).not.toMatch("oversea_runner_source_id");
});
it("preserves zero-start jockeyFrame counts for the viewer unknown-rate conversion", () => {
  expect(
    normaliseSimilarRow({
      kind: "jockeyFrame",
      umaban: 4,
      name: "Jockey",
      starts: 0,
      wins: 0,
      places: 0,
      shows: 0,
    }),
  ).toMatchObject({ starts: 0, wins: 0, places: 0, shows: 0 });
});

it("rejects invalid nonpositive mapped gates without changing exact jockey name matching", () => {
  const sql = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    keibajoCode: "A8",
    includeJockeyFrame: true,
  });
  expect(sql).toMatch("gate_mapping.gate_number BETWEEN 1 AND 32767");
  expect(sql).toMatch("se.kishumei_ryakusho");
  expect(sql).toMatch("= tn.name");
});

it("omits unknown owners before target matching and the left join can manufacture zero counts", () => {
  const sql: string = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    includeOwner: true,
  });
  expect(sql).toMatch(
    "coalesce(nullif(btrim(replace(coalesce(se.banushimei, ''), chr(12288), '')), ''), '不明') AS owner",
  );
  expect(sql).toMatch(
    "SELECT umaban, 'owner' AS kind, owner AS name\n  FROM current_entries\n  WHERE owner <> '不明'",
  );
});

it("executes the owner target arm with sixteen blank/unknown owners and one genuine owner", async () => {
  const sql: string = buildWinRateHeatmapSimilarQuery(config, {
    ...jraFilters,
    includeOwner: true,
  });
  const ownerExpression: string | undefined = sql.match(/([^\n]+) AS owner/u)?.[1];
  const ownerArm: string | undefined = sql.match(
    /SELECT umaban, 'owner' AS kind, owner AS name\n  FROM current_entries\n  WHERE owner <> '不明'/u,
  )?.[0];
  expect(ownerExpression).toBeDefined();
  expect(ownerArm).toBeDefined();
  const db = await DuckDBInstance.create(":memory:");
  const connection = await db.connect();
  try {
    await connection.run("CREATE MACRO btrim(value) AS trim(value)");
    const result = await connection.runAndReadAll(`WITH se AS (
      SELECT range AS umaban, CASE WHEN range % 4 = 0 THEN '' WHEN range % 4 = 1 THEN '  ' WHEN range % 4 = 2 THEN '　' ELSE '不明' END AS banushimei FROM range(1, 17)
      UNION ALL SELECT 17, 'Known'
    ), current_entries AS (SELECT umaban, ${ownerExpression} AS owner FROM se)
    ${ownerArm}`);
    expect(result.getRowObjects()).toStrictEqual([{ umaban: 17n, kind: "owner", name: "Known" }]);
  } finally {
    connection.closeSync();
    db.closeSync();
  }
});

it("reads only unambiguous current source-qualified pedigree and preserves native priority", async () => {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  const filters: WinRateHeatmapStatsFilters = {
    ...jraFilters,
    date: "20261004",
    keibajoCode: "A8",
    raceBango: "05",
  };
  try {
    await connection.run("CREATE SCHEMA pc_keiba");
    await connection.run("CREATE MACRO btrim(value) AS trim(value)");
    await connection.run(
      "CREATE MACRO regexp_match(value, pattern) AS CASE WHEN regexp_matches(value, pattern) THEN [value] ELSE NULL END",
    );
    await connection.run(
      "CREATE TABLE pc_keiba.jvd_se AS SELECT '2026' AS kaisai_nen, '1004' AS kaisai_tsukihi, 'A8' AS keibajo_code, '05' AS race_bango, '01' AS umaban, '0000000000' AS ketto_toroku_bango",
    );
    const columns: string = ["01", "02", "03", "05", "07", "09", "11", "13"]
      .map((n) => `ketto_joho_${n}b VARCHAR`)
      .join(", ");
    await connection.run(`CREATE TABLE pc_keiba.jvd_um (ketto_toroku_bango VARCHAR, ${columns})`);
    await connection.run("CREATE TABLE pc_keiba.nvd_um AS SELECT * FROM pc_keiba.jvd_um");
    await connection.run("CREATE TABLE pc_keiba.nvd_nu AS SELECT * FROM pc_keiba.jvd_um");
    await connection.run(
      "CREATE TABLE pc_keiba.oversea_runner_source_id AS SELECT *, 'jra' AS race_source, 'netkeiba' AS source, '000a029d42' AS source_horse_id FROM pc_keiba.jvd_se",
    );
    await connection.run(
      "CREATE TABLE pc_keiba.oversea_horse_pedigree (source VARCHAR, source_horse_id VARCHAR, sire_name VARCHAR, sire_sire_name VARCHAR, dam_sire_name VARCHAR)",
    );
    const query: string = buildWinRateHeatmapKnownBloodlineCountQuery(config, filters);
    // R2 SQL rejects the zero-column UNION projection produced by count(*).
    expect(query).toMatch(/SELECT count\(name\) AS known\nFROM current_bloodlines/u);
    expect((await connection.runAndReadAll(query)).getRowObjects()).toStrictEqual([{ known: 0n }]);
    await connection.run(
      "INSERT INTO pc_keiba.oversea_horse_pedigree VALUES ('other','000a029d42','Wrong',NULL,NULL)",
    );
    expect((await connection.runAndReadAll(query)).getRowObjects()).toStrictEqual([{ known: 0n }]);
    await connection.run(
      "INSERT INTO pc_keiba.oversea_horse_pedigree VALUES ('netkeiba','000a029d42','Sea The Stars','Cape Cross','Selkirk')",
    );
    expect((await connection.runAndReadAll(query)).getRowObjects()).toStrictEqual([{ known: 3n }]);
    const namesQuery: string = query.replace(
      "SELECT count(name) AS known\nFROM current_bloodlines",
      "SELECT name FROM current_bloodlines ORDER BY name",
    );
    await connection.run(
      "INSERT INTO pc_keiba.jvd_um (ketto_toroku_bango,ketto_joho_01b) VALUES ('0000000000','Native sire')",
    );
    expect((await connection.runAndReadAll(namesQuery)).getRowObjects()).toStrictEqual([
      { name: "Cape Cross" },
      { name: "Sea The Stars" },
      { name: "Selkirk" },
    ]);
    await connection.run(
      "INSERT INTO pc_keiba.oversea_runner_source_id SELECT * FROM pc_keiba.oversea_runner_source_id",
    );
    expect((await connection.runAndReadAll(query)).getRowObjects()).toStrictEqual([{ known: 0n }]);
    await connection.run("DELETE FROM pc_keiba.oversea_runner_source_id");
    await connection.run(
      "INSERT INTO pc_keiba.oversea_runner_source_id SELECT *, 'jra', 'netkeiba', '0000000000' FROM pc_keiba.jvd_se",
    );
    expect((await connection.runAndReadAll(query)).getRowObjects()).toStrictEqual([{ known: 0n }]);
    expect(buildWinRateHeatmapBloodlineQuery(config, filters)).toMatch(
      "WHERE NOT cb.foreign_profile OR stats.starts > 0",
    );
    expect(buildWinRateHeatmapBloodlineQuery(config, jraFilters)).not.toMatch(
      "oversea_horse_pedigree",
    );
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
});

it("preserves stored foreign owner provenance through normalization", () => {
  expect(
    normaliseWinRateHeatmapStatsPayload({
      bloodlineRows: [],
      similarRows: [
        {
          kind: "owner",
          umaban: 1,
          name: "netkeiba:a002e7",
          starts: 1,
          wins: 0,
          places: 0,
          shows: 0,
          statsSource: "netkeiba",
          statsScope: "stored-results-10y",
          sourcePersonId: "a002e7",
        },
      ],
    }).similarRows[0],
  ).toMatchObject({
    statsSource: "netkeiba",
    statsScope: "stored-results-10y",
    sourcePersonId: "a002e7",
  });
});

it("preserves the distinct stored same-title owner scope", () => {
  expect(
    normaliseSimilarRow({
      kind: "owner",
      name: "Owner",
      umaban: 1,
      starts: 8,
      wins: 1,
      places: 2,
      shows: 3,
      statsSource: "netkeiba",
      statsScope: "stored-results-race-10y",
      sourcePersonId: "a002e7",
    }).statsScope,
  ).toBe("stored-results-race-10y");
});

it("limits native nonfinish rate semantics to the exact current Arc context", () => {
  const arc: WinRateHeatmapStatsFilters = {
    ...jraFilters,
    date: "20261004",
    keibajoCode: "A8",
    raceBango: "05",
  };
  expect(buildWinRateHeatmapBloodlineQuery(config, arc)).toMatch(
    "CASE WHEN btrim(coalesce(se.ijo_kubun_code, '')) IN ('4', '5') THEN 0 ELSE",
  );
  expect(buildWinRateHeatmapSimilarQuery(config, arc)).toMatch(
    "btrim(coalesce(se.ijo_kubun_code, '')) NOT IN ('1', '2')",
  );
  expect(buildWinRateHeatmapBloodlineQuery(config, jraFilters)).not.toMatch("ijo_kubun_code");
  expect(buildWinRateHeatmapSimilarQuery(config, { ...arc, raceBango: "04" })).not.toMatch(
    "ijo_kubun_code",
  );
});

it("reads exact Arc display labels without historical name aggregation", () => {
  expect(
    buildArcBloodlineLabelsQuery(config, {
      ...jraFilters,
      date: "20261004",
      keibajoCode: "A8",
      raceBango: "05",
    }),
  ).toMatch(/SELECT umaban, category, name FROM current_bloodlines$/u);
  expect(() => buildArcBloodlineLabelsQuery(config, jraFilters)).toThrow("exact Arc");
});
