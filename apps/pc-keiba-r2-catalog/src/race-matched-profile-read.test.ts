// Runs with bun through Vitest; no provider I/O.
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
  expect(sql).toMatch("substring(ra.kaisai_tsukihi FROM 1 FOR 2) IN ('08', '09', '10')");
  expect(sql).toMatch("(SELECT count(*) FROM pc_keiba.jvd_se rc");
  expect(sql).toMatch(") = 16");
});

it("builds the NAR target profile from the tenths and numeric helpers", () => {
  const sql: string = buildTargetProfileSql({ ...input, source: "nar" });
  expect(sql).toMatch("WITH matched AS (");
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("INNER JOIN matched m");
  expect(sql).toMatch("WHERE se.kakutei_chakujun IN ('01', '02', '03')");
  expect(sql).toMatch("replace(btrim(coalesce(se.soha_time, '')), '0', '') <> ''");
  expect(sql).toMatch("try_cast(nullif(btrim(coalesce(se.kohan_3f, '')), '') AS DOUBLE) AS last3f");
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
