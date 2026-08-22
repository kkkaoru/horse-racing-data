import { expect, it } from "vitest";

import type { R2SqlCatalogConfig, WinRateHeatmapStatsFilters } from "./types";
import {
  buildWinRateHeatmapBloodlineQuery,
  buildWinRateHeatmapSimilarQuery,
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
  expect(sql).toMatch("AND ra.keibajo_code = '83'");
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
  expect(sql).toMatch("WHERE wakuban <> '' AND wakuban <> '00'");
  expect(sql).toMatch("GROUP BY kind, name, frame");
  expect(sql).toMatch("coalesce(stats.frame, '') = coalesce(cp.frame, '')");
  expect(sql).toMatch("'' AS frame");
  expect(sql).toMatch("'jockey' AS kind");
  expect(sql).toMatch("'trainer' AS kind");
  expect(sql).toMatch("tn.kind = 'jockey'");
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
  ).toThrow("keibajoCode must contain two digits");
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
