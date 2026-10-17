// Run with bun. Deterministic local DuckDB fixtures, never production access.
import { DuckDBInstance } from "@duckdb/node-api";
import { expect, it } from "vitest";
import { arcTitleSql, isCurrentArc } from "./arc-cohort";
import { buildPartnershipCountsQuery } from "./heatmap-partnership-sql";
import type { PartnershipKind, PartnershipScope } from "./heatmap-partnership";
import {
  buildWinRateHeatmapBloodlineQuery,
  buildWinRateHeatmapSimilarQuery,
} from "./win-rate-heatmap-stats";
import { buildConditionFinishPositionStatsQuery } from "./condition-history-stats";
import type { R2SqlCatalogConfig, WinRateHeatmapStatsFilters } from "./types";

const config: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "a",
  R2_SQL_BUCKET_NAME: "b",
  R2_SQL_TOKEN: "t",
  R2_SQL_NAMESPACE: "pc_keiba",
};
const filters: WinRateHeatmapStatsFilters = {
  date: "20261004",
  source: "jra",
  keibajoCode: "A8",
  raceBango: "05",
  years: 10,
  includeVenue: false,
  includeRaceTitle: false,
  includeDistance: false,
  includeSurface: false,
  includeTurn: false,
};
const kinds: PartnershipKind[] = [
  "horseJockey",
  "jockeyVenue",
  "jockeyTrainerVenue",
  "ownerVenue",
  "jockeyTrainerOwner",
];

it.each([
  [{}, true],
  [{ raceBango: "04" }, false],
  [{ date: "20251004" }, false],
  [{ source: "nar" }, false],
  [{ keibajoCode: "A9" }, false],
])("recognizes only the exact current Arc context %j", (change, expected) => {
  expect(isCurrentArc({ ...filters, ...change })).toBe(expected);
});
it("uses only observed Japanese event titles", () => {
  expect(arcTitleSql("title")).toBe(
    "btrim(replace(title, chr(12288), '')) IN ('凱旋門賞', '凱旋門賞(GI)')",
  );
});

it("executes all five partnership cohorts and their target races against the same pretarget Arc predicate", async () => {
  const db = await DuckDBInstance.create(":memory:");
  const c = await db.connect();
  try {
    await c.run("CREATE SCHEMA pc_keiba; CREATE MACRO btrim(v) AS trim(v)");
    await c.run(`CREATE TABLE pc_keiba.jvd_ra AS SELECT y AS kaisai_nen, d AS kaisai_tsukihi, 'A8' AS keibajo_code, r AS race_bango, title AS kyosomei_hondai, '11' AS track_code FROM (VALUES
      ('2025','1004','05','凱旋門賞　　　　　　　　'), ('2025','1004','06','オペラ賞(GI)'),
      ('2026','1004','05','凱旋門賞'), ('2027','1004','05','凱旋門賞')) t(y,d,r,title)`);
    await c.run(`CREATE TABLE pc_keiba.jvd_se AS SELECT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
      '01' AS umaban, '2020100001' AS ketto_toroku_bango, '00001' AS kishu_code, '00002' AS chokyoshi_code, '000003' AS banushi_code, '02' AS kakutei_chakujun FROM pc_keiba.jvd_ra`);
    // Duplicate source rows do not multiply the single result identity.
    await c.run(
      "INSERT INTO pc_keiba.jvd_se SELECT * FROM pc_keiba.jvd_se WHERE kaisai_nen='2025' AND race_bango='05'",
    );
    await c.run(
      "CREATE TABLE pc_keiba.nvd_ra AS SELECT * FROM pc_keiba.jvd_ra; CREATE TABLE pc_keiba.nvd_se AS SELECT * FROM pc_keiba.jvd_se",
    );
    const rows = await Promise.all(
      kinds.map(async (kind) => {
        const scope: PartnershipScope = {
          ...filters,
          kind,
          revision: "fixture",
          surface: "芝",
          eventScope: "arc-cohort-policy-v2",
        };
        const input = { config, scope };
        const counts = (
          await c.runAndReadAll(buildPartnershipCountsQuery(input))
        ).getRowObjectsJson();
        return {
          kind,
          counts: counts.map((r) => [
            Number(r.starts),
            Number(r.wins),
            Number(r.places),
            Number(r.shows),
          ]),
        };
      }),
    );
    expect(rows.map((r) => r.counts)).toStrictEqual([
      [[1, 0, 1, 1]],
      [[1, 0, 1, 1]],
      [[1, 0, 1, 1]],
      [[1, 0, 1, 1]],
      [[1, 0, 1, 1]],
    ]);
  } finally {
    c.closeSync();
    db.closeSync();
  }
});

it("keeps unique pretarget Arc bloodlines including current horses, with source-qualified three-role precedence and unknown people unavailable", async () => {
  const db = await DuckDBInstance.create(":memory:");
  const c = await db.connect();
  try {
    await c.run(
      "CREATE SCHEMA pc_keiba; CREATE MACRO btrim(v) AS trim(v); CREATE MACRO regexp_match(v,p) AS CASE WHEN regexp_matches(v,p) THEN [v] ELSE NULL END",
    );
    await c.run(`CREATE TABLE pc_keiba.jvd_ra AS SELECT y AS kaisai_nen, d AS kaisai_tsukihi, 'A8' AS keibajo_code, r AS race_bango, title AS kyosomei_hondai, '11' AS track_code, 'A' AS grade_code, '14' AS kyoso_shubetsu_code, '999' AS kyoso_joken_code, '' AS kyoso_joken_meisho, '2400' AS kyori FROM (VALUES
      ('2025','1004','05','凱旋門賞(GI)'), ('2025','1004','06','オペラ賞(GI)'),
      ('2016','1003','05','凱旋門賞'), ('2016','1004','05','凱旋門賞'),
      ('2026','1004','05','凱旋門賞'), ('2027','1004','05','凱旋門賞')) t(y,d,r,title)`);
    await c.run(`CREATE TABLE pc_keiba.jvd_se AS SELECT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
      '01' AS umaban, '2020100001' AS ketto_toroku_bango, '02' AS kakutei_chakujun, '不明' AS kishumei_ryakusho, '' AS chokyoshimei_ryakusho FROM pc_keiba.jvd_ra`);
    await c.run(
      "INSERT INTO pc_keiba.jvd_se SELECT * FROM pc_keiba.jvd_se WHERE kaisai_nen='2025' AND race_bango='05'",
    );
    await c.run(
      `INSERT INTO pc_keiba.jvd_se VALUES ('2025','1004','A8','05','02','0000000000','03','Known jockey','Known trainer'), ('2026','1004','A8','05','02','0000000000','00','Known jockey','Known trainer')`,
    );
    const columns: string = ["01", "02", "03", "05", "07", "09", "11", "13"]
      .map((n) => `ketto_joho_${n}b VARCHAR`)
      .join(", ");
    await c.run(`CREATE TABLE pc_keiba.jvd_um (ketto_toroku_bango VARCHAR, ${columns})`);
    await c.run(
      "CREATE TABLE pc_keiba.nvd_um AS SELECT * FROM pc_keiba.jvd_um; CREATE TABLE pc_keiba.nvd_nu AS SELECT * FROM pc_keiba.jvd_um",
    );
    await c.run(
      "INSERT INTO pc_keiba.jvd_um (ketto_toroku_bango,ketto_joho_01b,ketto_joho_03b,ketto_joho_05b) VALUES ('2020100001','Native sire','Shared ancestor','Shared ancestor'), ('0000000000','Wrong zero sire','Wrong zero grandsire','Wrong zero damsire')",
    );
    await c.run(`CREATE TABLE pc_keiba.oversea_runner_source_id AS SELECT DISTINCT kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban,'netkeiba' AS source,'jra' AS race_source,
      CASE WHEN umaban='01' THEN '000a000001' ELSE '000a000002' END AS source_horse_id FROM pc_keiba.jvd_se`);
    await c.run(`CREATE TABLE pc_keiba.oversea_horse_pedigree AS SELECT * FROM (VALUES
      ('netkeiba','000a000001','Wrong native override','Wrong native grandsire','Wrong native damsire'),
      ('other','000a000002','Wrong source sire','Wrong source grandsire','Wrong source damsire'),
      ('netkeiba','000a000002','Native sire','Shared ancestor','Shared ancestor')) t(source,source_horse_id,sire_name,sire_sire_name,dam_sire_name)`);
    // Native rate eligibility reads the provider status column; these ranked fixtures are normal starts.
    await c.run("ALTER TABLE pc_keiba.jvd_se ADD COLUMN ijo_kubun_code VARCHAR DEFAULT '0'");
    const sql: string = buildWinRateHeatmapBloodlineQuery(config, filters);
    const rows = (await c.runAndReadAll(sql)).getRowObjectsJson();
    expect(
      rows.map((r) => [
        r.umaban,
        r.category,
        r.name,
        Number(r.starts),
        Number(r.wins),
        Number(r.places),
        Number(r.shows),
      ]),
    ).toStrictEqual([
      [1, "damSire", "Shared ancestor", 3, 0, 2, 3],
      [1, "sire", "Native sire", 3, 0, 2, 3],
      [1, "sireSire", "Shared ancestor", 3, 0, 2, 3],
      [2, "damSire", "Shared ancestor", 3, 0, 2, 3],
      [2, "sire", "Native sire", 3, 0, 2, 3],
      [2, "sireSire", "Shared ancestor", 3, 0, 2, 3],
    ]);
    const people = (
      await c.runAndReadAll(buildWinRateHeatmapSimilarQuery(config, filters))
    ).getRowObjectsJson();
    expect(
      people.map((r) => [r.umaban, r.kind, r.name, Number(r.starts), Number(r.wins)]),
    ).toStrictEqual([
      [2, "jockey", "Known jockey", 1, 0],
      [2, "trainer", "Known trainer", 1, 0],
    ]);
    await c.run(
      "ALTER TABLE pc_keiba.jvd_se ADD COLUMN soha_time VARCHAR DEFAULT '2300'; ALTER TABLE pc_keiba.jvd_se ADD COLUMN bamei VARCHAR DEFAULT 'Horse'; ALTER TABLE pc_keiba.jvd_se ADD COLUMN wakuban VARCHAR DEFAULT '00'; ALTER TABLE pc_keiba.jvd_se ADD COLUMN tansho_ninkijun VARCHAR DEFAULT '01'; ALTER TABLE pc_keiba.jvd_se ADD COLUMN tansho_odds VARCHAR DEFAULT '100'; ALTER TABLE pc_keiba.jvd_se ADD COLUMN banushimei VARCHAR DEFAULT 'Owner'; ALTER TABLE pc_keiba.jvd_se ADD COLUMN kohan_3f VARCHAR DEFAULT '345'",
    );
    const targets = (
      await c.runAndReadAll(buildConditionFinishPositionStatsQuery(config, filters))
    ).getRowObjectsJson();
    expect(
      [...new Set(targets.map((r) => `${r.target_race_date}:${r.race_bango}`))].toSorted(),
    ).toStrictEqual(["20161004:05", "20251004:05"]);
    await c.run(
      "INSERT INTO pc_keiba.oversea_runner_source_id SELECT * FROM pc_keiba.oversea_runner_source_id WHERE kaisai_nen='2025' AND umaban='02'",
    );
    expect(
      (await c.runAndReadAll(sql)).getRowObjectsJson().map((r) => Number(r.starts)),
    ).toStrictEqual([2, 2, 2, 2, 2, 2]);
    await c.run(
      "INSERT INTO pc_keiba.oversea_horse_pedigree SELECT * FROM pc_keiba.oversea_horse_pedigree WHERE source='netkeiba' AND source_horse_id='000a000002'",
    );
    expect(
      (await c.runAndReadAll(sql)).getRowObjectsJson().map((r) => [r.umaban, Number(r.starts)]),
    ).toStrictEqual([
      [1, 2],
      [1, 2],
      [1, 2],
    ]);
    await c.run(
      "INSERT INTO pc_keiba.jvd_um SELECT * FROM pc_keiba.jvd_um WHERE ketto_toroku_bango='2020100001'",
    );
    expect(
      (await c.runAndReadAll(sql)).getRowObjectsJson().map((r) => [r.name, Number(r.starts)]),
    ).toStrictEqual([
      ["Wrong native damsire", 2],
      ["Wrong native override", 2],
      ["Wrong native grandsire", 2],
    ]);
    await c.run(
      "INSERT INTO pc_keiba.jvd_se SELECT * REPLACE ('03' AS kakutei_chakujun) FROM pc_keiba.jvd_se WHERE kaisai_nen='2016' AND kaisai_tsukihi='1004'",
    );
    expect(
      (await c.runAndReadAll(sql)).getRowObjectsJson().map((r) => Number(r.starts)),
    ).toStrictEqual([1, 1, 1]);
  } finally {
    c.closeSync();
    db.closeSync();
  }
});
