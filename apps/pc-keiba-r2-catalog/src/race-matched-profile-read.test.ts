// Runs with bun through Vitest; no provider I/O.
import { DuckDBInstance } from "@duckdb/node-api";
import { expect, it } from "vitest";
import {
  buildMatchedRacesSql,
  buildTargetProfileSql,
  readTargetProfile,
  type MatchedProfileInput,
} from "./race-matched-profile-read";

const input: MatchedProfileInput = {
  namespace: "pc_keiba",
  source: "jra",
  raceDate: "20260920",
  years: 10,
  race: {
    keibajoCode: "06",
    kyori: "1600",
    kyosoShubetsuCode: "11",
    kyosoJokenCode: "999",
    kyosoJokenMeisho: "オープン",
    trackCode: "11",
    gradeCode: "A",
    kyosomeiHondai: " テスト記念 ",
  },
  settings: {
    includeVenue: true,
    includeDistance: true,
    includeAge: true,
    includeClass: true,
    includeConditionKey: true,
    includeTrackCode: true,
    includeGrade: true,
    includeRaceTitle: true,
    includeMonthWindow: false,
    includeRunnerCount: false,
    runnerCount: null,
  },
  limit: 500,
};

it("parenthesizes every IS NOT DISTINCT FROM so R2 SQL does not bind it to AND", () => {
  const sql: string = buildMatchedRacesSql(input);
  expect(sql).toMatch("AND (ra.track_code IS NOT DISTINCT FROM '11')");
  expect(sql).toMatch("AND (ra.grade_code IS NOT DISTINCT FROM 'A')");
  expect(sql).toMatch("ra.grade_code IN ('A', 'F') AND (nullif(");
  expect(sql.split("IS NOT DISTINCT FROM").length - 1).toBe(4);
});

it("builds the matched-race query with the window and cell predicates", () => {
  const sql: string = buildMatchedRacesSql(input);
  expect(sql).toMatch("FROM pc_keiba.jvd_ra ra");
  expect(sql).toMatch("concat(ra.kaisai_nen, ra.kaisai_tsukihi) < '20260920'");
  expect(sql).toMatch("concat(ra.kaisai_nen, ra.kaisai_tsukihi) >= '20160920'");
  expect(sql).toMatch("ra.keibajo_code = '06'");
  expect(sql).toMatch("ra.kyori = '1600'");
  expect(sql).toMatch("ra.kyoso_shubetsu_code = '11'");
  expect(sql).toMatch("ra.kyoso_joken_code = '999'");
  expect(sql).toMatch("WHEN ra.kyoso_joken_code = '999' THEN 'オープン'");
  expect(sql).toMatch("split_part(trim(ra.kyoso_joken_meisho), ' ', 1)");
  expect(sql).toMatch("IS NOT DISTINCT FROM");
  expect(sql).toMatch("ra.track_code IS NOT DISTINCT FROM '11'");
  expect(sql).toMatch("ra.grade_code IS NOT DISTINCT FROM 'A'");
  expect(sql).toMatch("ra.grade_code IN ('A', 'F')");
  expect(sql).toMatch("chr(12288)");
  expect(sql).toMatch("LIMIT 501");
  expect(sql).not.toMatch("regexp_replace");
});

it("honours the include flags, the month window and the runner-count condition", () => {
  const sql: string = buildMatchedRacesSql({
    ...input,
    settings: {
      ...input.settings,
      includeVenue: false,
      includeDistance: false,
      includeAge: false,
      includeClass: false,
      includeConditionKey: false,
      includeTrackCode: false,
      includeGrade: false,
      includeRaceTitle: false,
      includeMonthWindow: true,
      includeRunnerCount: true,
      runnerCount: 16,
    },
  });
  expect(sql).not.toMatch("ra.keibajo_code =");
  expect(sql).not.toMatch("ra.kyori =");
  expect(sql).not.toMatch("IS NOT DISTINCT FROM");
  expect(sql).not.toMatch("ra.grade_code");
  expect(sql).not.toMatch("ra.kyosomei_hondai");
  expect(sql).toMatch("substring(ra.kaisai_tsukihi FROM 1 FOR 2) IN ('08', '09', '10')");
  expect(sql).toMatch("(SELECT count(*) FROM pc_keiba.jvd_se rc");
  expect(sql).toMatch(") = 16");
});

it.each(["B", "C", "L", "", null])(
  "matches named G2/G3/listed/open races without an implicit G1 restriction (%s)",
  (gradeCode) => {
    const sql: string = buildMatchedRacesSql({
      ...input,
      race: { ...input.race, gradeCode },
      settings: { ...input.settings, includeGrade: false },
    });
    expect(sql).toMatch(
      "(nullif(replace(btrim(coalesce(ra.kyosomei_hondai, '')), chr(12288), ''), '') IS NOT DISTINCT FROM 'テスト記念')",
    );
    expect(sql).not.toMatch("ra.grade_code");
    expect(sql).toMatch("FROM pc_keiba.jvd_ra ra");
    expect(sql).toMatch("ra.keibajo_code = '06'");
    expect(sql).toMatch("ra.kyoso_joken_code = '999'");
  },
);

it("retains the explicit G2 grade filter alongside race-title matching", () => {
  const sql: string = buildMatchedRacesSql({
    ...input,
    race: { ...input.race, gradeCode: "B" },
  });
  expect(sql).toMatch("(ra.grade_code IS NOT DISTINCT FROM 'B')");
  expect(sql).toMatch("IS NOT DISTINCT FROM 'テスト記念'");
  expect(sql).not.toMatch("IN ('A', 'F')");
});

it.each(["A", "F", " A "])(
  "retains the G1 title group when explicit grade matching is disabled (%s)",
  (gradeCode) => {
    const sql: string = buildMatchedRacesSql({
      ...input,
      race: { ...input.race, gradeCode },
      settings: { ...input.settings, includeGrade: false },
    });
    expect(sql).toMatch("ra.grade_code IN ('A', 'F')");
    expect(sql).not.toMatch("ra.grade_code IS NOT DISTINCT FROM");
    expect(sql).toMatch("IS NOT DISTINCT FROM 'テスト記念'");
  },
);

it("retains explicit grade matching when title matching is disabled", () => {
  const sql: string = buildMatchedRacesSql({
    ...input,
    settings: { ...input.settings, includeRaceTitle: false },
  });
  expect(sql).toMatch("(ra.grade_code IS NOT DISTINCT FROM 'A')");
  expect(sql).not.toMatch("IN ('A', 'F')");
  expect(sql).not.toMatch("ra.kyosomei_hondai");
});

it("builds the NAR target profile from the tenths and numeric helpers", () => {
  const sql: string = buildTargetProfileSql({ ...input, source: "nar" });
  expect(sql).toMatch("WITH matched AS (");
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("INNER JOIN matched m");
  expect(sql).toMatch("WHERE se.kakutei_chakujun IN ('01', '02', '03')");
  expect(sql).toMatch("replace(btrim(coalesce(se.soha_time, '')), '0', '') <> ''");
  expect(sql).toMatch("try_cast(nullif(btrim(coalesce(se.kohan_3f, '')), '') AS DOUBLE)");
  expect(sql).toMatch("avg(race_time) AS target_race_time");
});

it.each([
  { ...input, namespace: "pc-keiba" },
  { ...input, source: "ban-ei" as "jra" },
  { ...input, raceDate: "20260229" },
  { ...input, years: 0 },
  { ...input, limit: 0 },
  { ...input, limit: 5001 },
])("rejects invalid matched profile input %j", (value) => {
  expect(() => buildMatchedRacesSql(value)).toThrow("Invalid matched profile input");
});

it("parses the target profile row and rejects malformed values", async () => {
  await expect(
    readTargetProfile(input, async () => [
      {
        target_race_time: 834.5,
        target_last3f: "375",
        target_body_weight: null,
        target_carried_weight: 56,
        target_margin: null,
      },
    ]),
  ).resolves.toStrictEqual({
    targetRaceTime: 834.5,
    targetLast3f: 375,
    targetBodyWeight: null,
    targetCarriedWeight: 56,
    targetMargin: null,
  });
  await expect(readTargetProfile(input, async () => [])).rejects.toThrow(
    "Invalid target profile result",
  );
  await expect(readTargetProfile(input, async () => [{ target_race_time: "abc" }])).rejects.toThrow(
    "Invalid target profile value",
  );
});

it("averages decoded fixture rows using each historical venue and excludes sentinels", async () => {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    // DuckDB spells the provider SQL btrim function trim.
    await connection.run("CREATE MACRO btrim(value) AS trim(value)");
    const profile = await readTargetProfile(
      {
        ...input,
        source: "nar",
        years: null,
        settings: {
          ...input.settings,
          includeVenue: false,
          includeDistance: false,
          includeAge: false,
          includeClass: false,
          includeConditionKey: false,
          includeTrackCode: false,
          includeGrade: false,
          includeRaceTitle: false,
        },
      },
      async (sql) => {
        const result = await connection.runAndReadAll(`WITH
        race_fixture AS (
          SELECT '2026' AS kaisai_nen, '0601' AS kaisai_tsukihi, venue AS keibajo_code,
            '01' AS race_bango FROM (VALUES ('83'), ('05')) AS v(venue)
        ),
        runner_fixture AS (
          SELECT '2026' AS kaisai_nen, '0601' AS kaisai_tsukihi, '01' AS race_bango,
            '01' AS kakutei_chakujun, '1234' AS soha_time, *
          FROM (VALUES
            ('83', '3E8', '258', '600', '+006'),
            ('83', '3E8', '24E', '600', '-014'),
            ('83', '3E8', '26C', '600', '000'),
            ('05', '600', '600', '600', '1000'),
            ('83', 'FFF', 'FFF', '000', 'FFF'),
            ('83', '000', '000', '', ''),
            ('83', '3E8oops', '24Eoops', '6x00', '--014'),
            ('05', '3E8', '1e2', 'FFF', '+ 006'),
            ('83', '', '', NULL, NULL)
          ) AS v(keibajo_code, bataiju, futan_juryo, kohan_3f, time_sa)
        ), ${sql
          .replace(/^WITH /u, "")
          .replaceAll("pc_keiba.nvd_ra", "race_fixture")
          .replaceAll("pc_keiba.nvd_se", "runner_fixture")}`);
        return result.getRowObjects();
      },
    );
    expect(profile).toStrictEqual({
      targetRaceTime: 834,
      targetLast3f: 600,
      targetBodyWeight: 900,
      targetCarriedWeight: 4675,
      targetMargin: 255,
    });
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
});

it("adds bare kaisai_nen bounds on every scanned table so R2 SQL prunes year partitions", () => {
  const sql: string = buildTargetProfileSql({
    ...input,
    settings: { ...input.settings, includeRunnerCount: true, runnerCount: 16 },
  });
  expect(sql).toMatch("ra.kaisai_nen >= '2016' AND ra.kaisai_nen <= '2026'");
  expect(sql).toMatch("AND se.kaisai_nen >= '2016' AND se.kaisai_nen <= '2026'");
  expect(sql).toMatch("AND rc.kaisai_nen >= '2016' AND rc.kaisai_nen <= '2026') = 16");
  const unbounded: string = buildMatchedRacesSql({ ...input, years: null });
  expect(unbounded).toMatch("< '20260920' AND ra.kaisai_nen <= '2026' AND ra.keibajo_code");
  expect(unbounded).not.toMatch("kaisai_nen >=");
});

const PROFILE_FIXTURE: string = `WITH
  race_dates AS (
    SELECT * FROM (VALUES
      ('2015', '1231'), ('2016', '0919'), ('2016', '0920'), ('2021', '0601'),
      ('2026', '0919'), ('2026', '0920'), ('2027', '0101')
    ) AS v(kaisai_nen, kaisai_tsukihi)
  ),
  race_rows AS (
    SELECT kaisai_nen, kaisai_tsukihi, '06' AS keibajo_code, '01' AS race_bango FROM race_dates
  ),
  runner_rows AS (
    SELECT r.kaisai_nen, r.kaisai_tsukihi, '01' AS race_bango, '06' AS keibajo_code,
      p.kakutei_chakujun, p.soha_time, '480' AS bataiju, '550' AS futan_juryo,
      '345' AS kohan_3f, '001' AS time_sa
    FROM race_dates r
    CROSS JOIN (VALUES ('01', '1345'), ('02', '1350'), ('03', '1352'), ('04', '1400'))
      AS p(kakutei_chakujun, soha_time)
  ), `;
const YEAR_BOUND: RegExp =
  /(?: AND |\n {4}AND |\n {6}AND )?\b(?:ra|se|rc)\.kaisai_nen [<>]= '\d+'/gu;

it.each([{ years: 10 }, { years: 5 }, { years: null }])(
  "returns the same profile with and without the year bounds (years=$years)",
  async (window) => {
    const instance = await DuckDBInstance.create(":memory:");
    const connection = await instance.connect();
    const run = async (sql: string): Promise<Record<string, unknown>[]> =>
      (
        await connection.runAndReadAll(
          `${PROFILE_FIXTURE}${sql
            .replace(/^WITH /u, "")
            .replaceAll("pc_keiba.jvd_ra", "race_rows")
            .replaceAll("pc_keiba.jvd_se", "runner_rows")}`,
        )
      ).getRowObjects();
    try {
      await connection.run("CREATE MACRO btrim(value) AS trim(value)");
      const sql: string = buildTargetProfileSql({
        ...input,
        years: window.years,
        settings: {
          ...input.settings,
          includeVenue: false,
          includeDistance: false,
          includeAge: false,
          includeClass: false,
          includeConditionKey: false,
          includeTrackCode: false,
          includeGrade: false,
          includeRaceTitle: false,
          includeRunnerCount: true,
          runnerCount: 4,
        },
      });
      const stripped: string = sql.replaceAll(YEAR_BOUND, "");
      expect(stripped).not.toMatch(/\b(?:ra|se|rc)\.kaisai_nen [<>]=/u);
      const bounded: Record<string, unknown>[] = await run(sql);
      expect(bounded).toStrictEqual(await run(stripped));
      expect(bounded[0]?.matched_runners).toBeGreaterThan(0n);
    } finally {
      connection.closeSync();
      instance.closeSync();
    }
  },
);
