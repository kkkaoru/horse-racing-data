import { expect, it } from "vitest";

import { buildRunningStyleExplainQuery, buildRunningStyleFeaturesQuery } from "./running-style-sql";
import type { R2SqlCatalogConfig } from "./types";

const config = (): R2SqlCatalogConfig => ({
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "token",
});

it("builds partition-pruned JRA raw Iceberg SQL with the production feature CTEs", () => {
  const sql = buildRunningStyleFeaturesQuery(
    config(),
    {
      date: "20260715",
      keibajoCode: "05",
      raceBango: "01",
      source: "jra",
    },
    true,
  );
  expect(sql.match(/FROM pc_keiba\.jvd_se/gu)).toHaveLength(2);
  expect(sql.match(/FROM pc_keiba\.jvd_ra/gu)).toHaveLength(2);
  expect(sql).toMatch("left join pc_keiba.jvd_um um");
  expect(sql).not.toMatch(/\busing\s*\(/i);
  expect(sql).toMatch("hc.source = t.source");
  expect(sql).not.toMatch(/nvd_(?:se|ra|um)/u);
  expect(sql).toMatch("kaisai_nen = '2021' AND kaisai_tsukihi >= '0715'");
  expect(sql).not.toMatch("concat(kaisai_nen, kaisai_tsukihi) >= '20210715'");
  expect(sql.match(/kaisai_nen = '2026'/gu)).toHaveLength(4);
  expect(sql.match(/kaisai_tsukihi = '0715'/gu)).toHaveLength(2);
  expect(sql.match(/keibajo_code = '05'/gu)).toHaveLength(2);
  expect(sql.match(/race_bango = '01'/gu)).toHaveLength(2);
  expect(sql).toMatch("to_unixtime(");
  expect(sql).toMatch("approx_percentile_cont(case when");
  expect(sql).not.toMatch(/\bpercentile_cont\s*\(/iu);
  expect(sql).toMatch("regr_slope(case when");
  expect(sql).not.toMatch(/\bover\s*\(/iu);
  expect(sql).toMatch("base_feature_ranks as");
  expect(sql).toMatch("order by umaban limit 18");
});

it("separates NAR and ban-ei scans while keeping raw row source nar", () => {
  const nar = buildRunningStyleFeaturesQuery(
    config(),
    {
      date: "20260715",
      keibajoCode: "42",
      raceBango: "09",
      source: "nar",
    },
    true,
  );
  const banEi = buildRunningStyleFeaturesQuery(
    config(),
    {
      date: "20260715",
      keibajoCode: "83",
      raceBango: "09",
      source: "ban-ei",
    },
    true,
  );
  expect(nar).toMatch("FROM pc_keiba.nvd_se");
  expect(nar).toMatch("left join pc_keiba.nvd_um um");
  expect(nar).not.toMatch(/jvd_(?:se|ra|um)/u);
  expect(nar.match(/keibajo_code <> '83'/gu)).toHaveLength(4);
  expect(banEi.match(/keibajo_code = '83'/gu)).toHaveLength(8);
  expect(banEi).toMatch("'nar' AS source");
});

it("emits only fixed raw-table R2 SQL syntax", () => {
  const sql = buildRunningStyleFeaturesQuery(
    config(),
    {
      date: "20260715",
      keibajoCode: "05",
      raceBango: "01",
      source: "jra",
    },
    true,
  );
  expect(sql).not.toMatch(/race_entry_corner_features|daily_race_entries|archive|neon/iu);
  expect(sql).not.toMatch(/::| filter \(where|\n\s*window\s/iu);
  expect(sql).not.toMatch(/then\s+case/iu);
  expect(sql).not.toMatch(/\b(?:insert|update|delete|create|drop|alter)\b/iu);
});

it("limits pedigree aggregation to sires and damsires used by target runners", () => {
  const sql = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "05", raceBango: "01", source: "jra" },
    false,
  );
  expect(sql).toMatch("target_sires as (");
  expect(sql).toMatch("target_damsires as (");
  expect(sql).toMatch("left join target_sires ts on ts.target_sire = um.ketto_joho_01b");
  expect(sql).toMatch("left join target_damsires td on td.target_damsire = um.ketto_joho_05b");
  expect(sql).toMatch("and (ts.target_sire is not null or td.target_damsire is not null)");
  expect(sql.indexOf("target_pedigree as (")).toBeLessThan(sql.indexOf("pedigree_rec_um as ("));
});

it("rejects unsafe filters and mismatched NAR categories", () => {
  const build = (overrides: Partial<Parameters<typeof buildRunningStyleFeaturesQuery>[1]>) =>
    buildRunningStyleFeaturesQuery(
      config(),
      {
        date: "20260715",
        keibajoCode: "05",
        raceBango: "01",
        source: "jra",
        ...overrides,
      },
      true,
    );
  expect(() => build({ date: "2026-07-15" })).toThrow("date must match YYYYMMDD");
  expect(() => build({ keibajoCode: "5" })).toThrow("keibajoCode must contain two digits");
  expect(() => build({ raceBango: "1;" })).toThrow("raceBango must contain two digits");
  expect(() => build({ umaban: 0 })).toThrow("umaban must be an integer from 1 to 18");
  expect(() => build({ umaban: 19 })).toThrow("umaban must be an integer from 1 to 18");
  expect(() => build({ keibajoCode: "05", source: "ban-ei" })).toThrow(
    "keibajoCode must be 83 for ban-ei",
  );
  expect(() => build({ keibajoCode: "83", source: "nar" })).toThrow(
    "source must be ban-ei for keibajoCode 83",
  );
  expect(() =>
    buildRunningStyleFeaturesQuery(
      { ...config(), R2_SQL_NAMESPACE: "pc_keiba;drop" },
      { date: "20260715", keibajoCode: "05", raceBango: "01", source: "jra" },
      true,
    ),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
});

it("filters one target runner and caps the query at one row", () => {
  const sql = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "05", raceBango: "01", source: "jra", umaban: 7 },
    false,
  );
  expect(sql.match(/try_cast\(nullif\(umaban, ''\) AS INT\) = 7/gu)).toHaveLength(1);
  expect(sql).toMatch("select * from final_features limit 1");
});

it("drops the target race filter and widens the cap when raceBango is omitted", () => {
  const venue = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "05", source: "jra" },
    true,
  );
  expect(venue).toMatch("select * from final_features order by race_bango, umaban limit 216");
  expect(venue).not.toMatch(/AND race_bango = /u);
  expect(venue.match(/keibajo_code = '05'/gu)).toHaveLength(2);
  expect(venue.match(/kaisai_tsukihi = '0715'/gu)).toHaveLength(2);
  expect(venue).toMatch("kaisai_nen = '2021' AND kaisai_tsukihi >= '0715'");
});

it("uses narrow NAR history for regular races and wide history for graded races", () => {
  const regular = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "43", raceBango: "01", source: "nar" },
    false,
  );
  const graded = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "43", raceBango: "01", source: "nar", gradeCode: "A" },
    false,
  );
  expect(regular).toMatch("kaisai_nen = '2023' AND kaisai_tsukihi >= '0715'");
  expect(graded).toMatch("kaisai_nen = '2016' AND kaisai_tsukihi >= '0715'");
  expect(
    buildRunningStyleFeaturesQuery(
      config(),
      { date: "20260715", keibajoCode: "43", raceBango: "01", source: "nar", gradeCode: "E" },
      false,
    ),
  ).toMatch("kaisai_nen = '2023' AND kaisai_tsukihi >= '0715'");
});

it("keeps the decade-wide history CTEs byte-identical between race and venue builds", () => {
  const race = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "05", raceBango: "01", source: "jra" },
    true,
  );
  const venue = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "05", source: "jra" },
    true,
  );
  const historyOf = (sql: string): string => sql.slice(0, sql.indexOf("target_se AS"));
  expect(historyOf(venue)).toBe(historyOf(race));
  expect(race).toMatch("select * from final_features order by umaban limit 18");
});

it("omits ORDER BY but keeps the widened cap for a venue-level fallback build", () => {
  const venue = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "05", source: "jra" },
    false,
  );
  expect(venue).toMatch("select * from final_features limit 216");
  expect(venue).not.toMatch("order by race_bango");
});

it("builds a fixed JSON EXPLAIN for the exact production query", () => {
  const explain = buildRunningStyleExplainQuery(
    config(),
    {
      date: "20260715",
      keibajoCode: "05",
      raceBango: "01",
      source: "jra",
    },
    true,
  );
  expect(explain).toMatch(/^EXPLAIN FORMAT JSON WITH history_se/u);
  expect(explain).toMatch("limit 18");
});

it("omits ORDER BY (keeping LIMIT) when includeOrderBy is false, for the R2 SQL expression-too-deep fallback", () => {
  const withOrderBy = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "05", raceBango: "01", source: "jra" },
    true,
  );
  const withoutOrderBy = buildRunningStyleFeaturesQuery(
    config(),
    { date: "20260715", keibajoCode: "05", raceBango: "01", source: "jra" },
    false,
  );
  expect(withOrderBy).toMatch("select * from final_features order by umaban limit 18");
  expect(withoutOrderBy).toMatch("select * from final_features limit 18");
  expect(withoutOrderBy).not.toMatch("order by umaban");
  // Everything up to the final SELECT (the entire 44-CTE chain) is
  // byte-identical -- only the trailing SELECT's ORDER BY differs.
  const finalSelectPattern = / final_features(?: order by umaban)? limit 18/u;
  expect(withOrderBy.replace(finalSelectPattern, "")).toBe(
    withoutOrderBy.replace(finalSelectPattern, ""),
  );
});
