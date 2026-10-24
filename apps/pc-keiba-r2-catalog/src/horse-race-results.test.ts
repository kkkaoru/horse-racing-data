import { DuckDBInstance } from "@duckdb/node-api";
import { expect, it } from "vitest";

import {
  buildCurrentRunnersQuery,
  buildHorseHistoryQuery,
  currentHorseIds,
  horseHistoryRanges,
  mergeHorseRaceResults,
  normaliseHorseRaceResultRow,
  uniqueHorseRaceResults,
  type HorseHistoryRange,
} from "./horse-race-results";
import type { HorseRaceResultRow, HorseRaceResultsFilters, R2SqlCatalogConfig } from "./types";

const config: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "token",
};

const sampleRow: HorseRaceResultRow = {
  babajotaiCodeDirt: "01",
  babajotaiCodeShiba: "01",
  bamei: "Deep",
  banushimei: "Owner",
  barei: "04",
  bataiju: "480",
  blinkerShiyoKubun: "1",
  chokyoshimeiRyakusho: "Trainer",
  corner1: "01",
  corner2: "02",
  corner3: "03",
  corner4: "04",
  currentBarei: "05",
  currentJockey: "Take",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: "550",
  gradeCode: "A",
  hassoJikoku: "1510",
  juryoShubetsuCode: "3",
  kaisaiNen: "2025",
  kaisaiTsukihi: "0715",
  kakuteiChakujun: "01",
  keibajoCode: "05",
  kettoTorokuBango: "2023100001",
  kishumeiRyakusho: "Take",
  kohan3f: "351",
  kyori: "1600",
  kyosoJokenCode: "005",
  kyosoJokenMeisho: "3勝",
  kyosoKigoCode: "A",
  kyosomeiFukudai: null,
  kyosomeiHondai: "テスト",
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: "12",
  raceBango: "11",
  seibetsuCode: "1",
  shussoTosu: "16",
  sohaTime: "1345",
  tanshoNinkijun: "01",
  tanshoOdds: "0250",
  tenkoCode: "1",
  timeSa: "002",
  trackCode: "17",
  umaban: "03",
  wakuban: "2",
  zogenFugo: "+",
  zogenSa: "004",
};

const nowFilters: HorseRaceResultsFilters = {
  date: "20261024",
  keibajoCode: "05",
  raceBango: "11",
  source: "jra",
  sourceScope: "all",
};
const settledRange: HorseHistoryRange = { from: null, settled: true, to: "20261023" };
const recentRange: HorseHistoryRange = { from: "20261023", settled: false, to: "20261024" };

it("reads the current runners from the race's own year partition only", () => {
  const sql = buildCurrentRunnersQuery(config, nowFilters);
  expect(sql).toMatch("FROM pc_keiba.jvd_se\nWHERE kaisai_nen = '2026'");
  expect(sql).toMatch("AND kaisai_tsukihi = '1024'");
  expect(sql).toMatch("AND keibajo_code = '05'");
  expect(sql).toMatch("AND race_bango = '11'");
  expect(sql).toMatch("lpad(btrim(coalesce(umaban, '')), 2, '0') AS current_umaban");
  expect(sql).toMatch("replace(btrim(coalesce(ketto_toroku_bango, '')), '0', '') <> ''");
  expect(sql).toMatch("'不明') AS current_jockey");
  expect(sql).not.toMatch("jvd_ra");
  expect(buildCurrentRunnersQuery(config, { ...nowFilters, source: "nar" })).toMatch(
    "FROM pc_keiba.nvd_se\n",
  );
});

it("builds JRA+NAR settled history over the listed horses with foal-year bounds", () => {
  const sql = buildHorseHistoryQuery({
    env: config,
    filters: nowFilters,
    horseIds: ["2020106002", "2021105001"],
    range: settledRange,
  });
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.jvd_ra ra");
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch("INNER JOIN pc_keiba.nvd_ra ra");
  expect(sql).toMatch("UNION ALL");
  expect(sql).toMatch("WHERE se.ketto_toroku_bango IN ('2020106002', '2021105001')");
  expect(sql).toMatch("AND se.kaisai_nen >= '2022' AND se.kaisai_nen <= '2026'");
  expect(sql).toMatch("AND ra.kaisai_nen >= '2022' AND ra.kaisai_nen <= '2026'");
  expect(sql).toMatch("WHERE concat(kaisai_nen, kaisai_tsukihi) < '20261023'");
  expect(sql).not.toMatch("concat(kaisai_nen, kaisai_tsukihi) >=");
  expect(sql).toMatch("se.blinker_shiyo_kubun");
  expect(sql).not.toMatch("current_");
  expect(sql).not.toMatch("ORDER BY");
  expect(sql).not.toMatch("oversea_");
});

it("bounds the unsettled history to the days between the cutoff and the race", () => {
  const sql = buildHorseHistoryQuery({
    env: config,
    filters: { ...nowFilters, sourceScope: "jra" },
    horseIds: ["2021105001"],
    range: recentRange,
  });
  expect(sql).toMatch("AND se.kaisai_nen >= '2026' AND se.kaisai_nen <= '2026'");
  expect(sql).toMatch(
    "WHERE concat(kaisai_nen, kaisai_tsukihi) < '20261024'\n  AND concat(kaisai_nen, kaisai_tsukihi) >= '20261023'",
  );
  expect(sql).not.toMatch("nvd_");
  expect(sql).not.toMatch("UNION ALL");
});

it("drops the foal-year bound for ban-ei identifiers and escapes quotes", () => {
  const sql = buildHorseHistoryQuery({
    env: config,
    filters: { ...nowFilters, sourceScope: "nar" },
    horseIds: ["2120190323", "20'1"],
    range: settledRange,
  });
  expect(sql).toMatch("IN ('2120190323', '20''1')");
  expect(sql).toMatch("AND se.kaisai_nen <= '2026'\n    AND ra.kaisai_nen <= '2026'");
  expect(sql).not.toMatch("kaisai_nen >=");
  expect(sql).not.toMatch("jvd_");
  const recent = buildHorseHistoryQuery({
    env: config,
    filters: nowFilters,
    horseIds: ["2120190323"],
    range: recentRange,
  });
  expect(recent).toMatch("AND se.kaisai_nen >= '2026' AND se.kaisai_nen <= '2026'");
});

it("splits the history at the first unsettled race day", () => {
  expect(horseHistoryRanges("20261024", "20261023")).toStrictEqual([
    { from: null, settled: true, to: "20261023" },
    { from: "20261023", settled: false, to: "20261024" },
  ]);
  expect(horseHistoryRanges("20261023", "20261023")).toStrictEqual([
    { from: null, settled: true, to: "20261023" },
  ]);
  expect(horseHistoryRanges("20250105", "20261023")).toStrictEqual([
    { from: null, settled: true, to: "20250105" },
  ]);
});

it("lists each registered current horse once in sorted order", () => {
  expect(
    currentHorseIds([
      { ketto_toroku_bango: "2021105001" },
      { ketto_toroku_bango: "2020106002" },
      { ketto_toroku_bango: "2021105001" },
      { ketto_toroku_bango: null },
    ]),
  ).toStrictEqual(["2020106002", "2021105001"]);
});

it("joins past starts to the current entry and orders them like the former query", () => {
  const current = [
    { current_umaban: "10", ketto_toroku_bango: "B", current_jockey: "J10" },
    { current_umaban: "xx", ketto_toroku_bango: "C", current_jockey: "Jx" },
    { current_umaban: "02", ketto_toroku_bango: "A", current_jockey: "J2" },
  ];
  const history = [
    { ketto_toroku_bango: "C", kaisai_nen: "2026", kaisai_tsukihi: "0101", race_bango: "01" },
    { ketto_toroku_bango: "B", kaisai_nen: "2025", kaisai_tsukihi: "0101", race_bango: "01" },
    { ketto_toroku_bango: "A", kaisai_nen: "2025", kaisai_tsukihi: "0101", race_bango: "01" },
    { ketto_toroku_bango: "A", kaisai_nen: "2025", kaisai_tsukihi: "0101", race_bango: "05" },
    { ketto_toroku_bango: "A", kaisai_nen: "2025", kaisai_tsukihi: "0301", race_bango: "01" },
    { ketto_toroku_bango: "A", kaisai_nen: "2026", kaisai_tsukihi: "0101", race_bango: "01" },
    { ketto_toroku_bango: "Z", kaisai_nen: "2026", kaisai_tsukihi: "0101", race_bango: "01" },
  ];
  expect(
    mergeHorseRaceResults(current, history).map(
      (row) =>
        `${String(row.current_jockey)}:${String(row.kaisai_nen)}${String(row.kaisai_tsukihi)}-${String(row.race_bango)}`,
    ),
  ).toStrictEqual([
    "J2:20260101-01",
    "J2:20250301-01",
    "J2:20250101-05",
    "J2:20250101-01",
    "J10:20250101-01",
    "Jx:20260101-01",
  ]);
});

it("rejects unsafe namespace, dates, and codes", () => {
  expect(() =>
    buildCurrentRunnersQuery({ ...config, R2_SQL_NAMESPACE: "pc_keiba;drop" }, nowFilters),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
  expect(() => buildCurrentRunnersQuery(config, { ...nowFilters, date: "2026-07-15" })).toThrow(
    "date must match YYYYMMDD",
  );
  expect(() => buildCurrentRunnersQuery(config, { ...nowFilters, date: "20260231" })).toThrow(
    "date must be a valid calendar date",
  );
  expect(() => buildCurrentRunnersQuery(config, { ...nowFilters, keibajoCode: "5" })).toThrow(
    "keibajoCode must contain two digits",
  );
  expect(() => buildCurrentRunnersQuery(config, { ...nowFilters, raceBango: "1;" })).toThrow(
    "raceBango must contain two digits",
  );
  expect(() =>
    buildHorseHistoryQuery({
      env: config,
      filters: nowFilters,
      horseIds: ["2021105001"],
      range: { from: "2026-10-23", settled: false, to: "20261024" },
    }),
  ).toThrow("date must match YYYYMMDD");
  expect(() =>
    buildHorseHistoryQuery({
      env: { ...config, R2_SQL_NAMESPACE: "x y" },
      filters: nowFilters,
      horseIds: ["2021105001"],
      range: settledRange,
    }),
  ).toThrow("R2_SQL_NAMESPACE must be an unquoted SQL identifier");
});

// Frozen copy of the former single-query builder: the equivalence oracle.
const ORACLE_RACE_COLUMNS: string =
  "ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango, ra.kyosomei_hondai, ra.kyosomei_fukudai, ra.kyosomei_kakkonai, ra.grade_code, ra.kyoso_shubetsu_code, ra.kyoso_kigo_code, ra.juryo_shubetsu_code, ra.kyoso_joken_code, ra.kyoso_joken_meisho, ra.kyori, ra.track_code, ra.hasso_jikoku, ra.shusso_tosu, ra.tenko_code, ra.babajotai_code_shiba, ra.babajotai_code_dirt";
const ORACLE_RUNNER_COLUMNS: string =
  "se.wakuban, se.umaban, se.ketto_toroku_bango, se.bamei, se.seibetsu_code, se.barei, se.futan_juryo, se.kishumei_ryakusho, se.chokyoshimei_ryakusho, se.banushimei, se.bataiju, se.zogen_fugo, se.zogen_sa, se.kakutei_chakujun, se.tansho_odds, se.tansho_ninkijun, se.soha_time, se.time_sa, se.corner_1, se.corner_2, se.corner_3, se.corner_4, se.kohan_3f, se.blinker_shiyo_kubun";
const ORACLE_TABLES: Record<HorseRaceResultsFilters["sourceScope"], string[][]> = {
  all: [
    ["jvd_se", "jvd_ra"],
    ["nvd_se", "nvd_ra"],
  ],
  jra: [["jvd_se", "jvd_ra"]],
  nar: [["nvd_se", "nvd_ra"]],
};
const oracleQuery = (filters: HorseRaceResultsFilters): string => `
WITH current_horses AS (
  SELECT
    lpad(btrim(coalesce(umaban, '')), 2, '0') AS current_umaban,
    ketto_toroku_bango,
    seibetsu_code AS current_seibetsu_code,
    barei AS current_barei,
    coalesce(nullif(btrim(replace(coalesce(kishumei_ryakusho, ''), chr(12288), '')), ''), '不明') AS current_jockey
  FROM pc_keiba.${filters.source === "jra" ? "jvd_se" : "nvd_se"}
  WHERE kaisai_nen = '${filters.date.slice(0, 4)}'
    AND kaisai_tsukihi = '${filters.date.slice(4)}'
    AND keibajo_code = '${filters.keibajoCode}'
    AND race_bango = '${filters.raceBango}'
    AND nullif(btrim(replace(coalesce(ketto_toroku_bango, ''), chr(12288), '')), '') IS NOT NULL
    AND replace(btrim(coalesce(ketto_toroku_bango, '')), '0', '') <> ''
),
history AS (
  ${ORACLE_TABLES[filters.sourceScope]
    .map(
      ([
        se,
        ra,
      ]) => `SELECT ch.current_jockey, ch.current_barei, ch.current_seibetsu_code, ch.current_umaban,
    ${ORACLE_RACE_COLUMNS}, ${ORACLE_RUNNER_COLUMNS}
  FROM pc_keiba.${String(se)} se
  INNER JOIN pc_keiba.${String(ra)} ra
    ON ra.kaisai_nen = se.kaisai_nen AND ra.kaisai_tsukihi = se.kaisai_tsukihi
    AND ra.keibajo_code = se.keibajo_code AND ra.race_bango = se.race_bango
  INNER JOIN current_horses ch ON ch.ketto_toroku_bango = se.ketto_toroku_bango`,
    )
    .join("\n  UNION ALL\n  ")}
)
SELECT * FROM history
WHERE concat(kaisai_nen, kaisai_tsukihi) < '${filters.date}'
ORDER BY try_cast(current_umaban AS INT), kaisai_nen DESC, kaisai_tsukihi DESC, race_bango DESC`;

const FIXTURE_DATES: string[] = [
  "20230601",
  "20240505",
  "20250104",
  "20250105",
  "20261022",
  "20261023",
  "20261024",
  "20261025",
  "20261231",
  "20270101",
  "20270102",
];
const NAR_DATES: string[] = ["20240506", "20261023", "20261024"];
const RUNNER_COLUMNS: string =
  "kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, seibetsu_code, barei, futan_juryo, kishumei_ryakusho, chokyoshimei_ryakusho, banushimei, bataiju, zogen_fugo, zogen_sa, kakutei_chakujun, tansho_odds, tansho_ninkijun, soha_time, time_sa, corner_1, corner_2, corner_3, corner_4, kohan_3f, blinker_shiyo_kubun";
const RACE_COLUMNS: string =
  "kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyosomei_hondai, kyosomei_fukudai, kyosomei_kakkonai, grade_code, kyoso_shubetsu_code, kyoso_kigo_code, juryo_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, kyori, track_code, hasso_jikoku, shusso_tosu, tenko_code, babajotai_code_shiba, babajotai_code_dirt";
const varcharColumns = (columns: string): string =>
  columns
    .split(", ")
    .map((column) => `${column} VARCHAR`)
    .join(", ");
// [keibajo, race, umaban, ketto, jockey] per runner on every fixture date.
const JRA_RUNNERS: string[][] = [
  ["05", "11", "2", "2021105001", "武　豊"],
  ["05", "11", "10", "2020106002", ""],
  ["05", "11", "5", "0000000000", "Zero"],
  ["05", "12", "1", "2120190323", "Banei"],
  ["05", "12", "3", "2019100003", "Other"],
];
const runnerValues = (date: string, runner: string[]): string =>
  `('${date.slice(0, 4)}', '${date.slice(4)}', '${String(runner[0])}', '${String(runner[1])}', '1', '${String(runner[2])}', '${String(runner[3])}', 'Horse${date}', '1', '04', '550', '${String(runner[4])}', 'Trainer', 'Owner', '480', '+', '002', '01', '0250', '01', '1345', '002', '01', '02', '03', '04', '351', '0')`;
const raceValues = (date: string, keibajo: string, race: string): string =>
  `('${date.slice(0, 4)}', '${date.slice(4)}', '${keibajo}', '${race}', 'Race${date}', NULL, NULL, 'A', '12', 'A', '3', '005', '3勝', '1600', '17', '1510', '16', '1', '01', '01')`;

const withFixture = async (
  check: (run: (sql: string) => Promise<Record<string, unknown>[]>) => Promise<void>,
): Promise<void> => {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  const run = async (sql: string): Promise<Record<string, unknown>[]> =>
    (await connection.runAndReadAll(sql)).getRowObjects();
  try {
    // DuckDB spells the provider SQL btrim function trim.
    await connection.run(`CREATE MACRO btrim(value) AS trim(value);
      CREATE SCHEMA pc_keiba;
      CREATE TABLE pc_keiba.jvd_se (${varcharColumns(RUNNER_COLUMNS)});
      CREATE TABLE pc_keiba.nvd_se (${varcharColumns(RUNNER_COLUMNS)});
      CREATE TABLE pc_keiba.jvd_ra (${varcharColumns(RACE_COLUMNS)});
      CREATE TABLE pc_keiba.nvd_ra (${varcharColumns(RACE_COLUMNS)});
      INSERT INTO pc_keiba.jvd_se VALUES ${FIXTURE_DATES.flatMap((date) => JRA_RUNNERS.map((runner) => runnerValues(date, runner))).join(", ")};
      INSERT INTO pc_keiba.jvd_ra VALUES ${FIXTURE_DATES.flatMap((date) => [raceValues(date, "05", "11"), raceValues(date, "05", "12")]).join(", ")};
      INSERT INTO pc_keiba.nvd_se VALUES ${NAR_DATES.map((date) => runnerValues(date, ["42", "03", "7", "2021105001", "Nar"])).join(", ")};
      INSERT INTO pc_keiba.nvd_ra VALUES ${NAR_DATES.map((date) => raceValues(date, "42", "03")).join(", ")};`);
    await check(run);
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
};

it.each([
  { date: "20261024", firstUnsettled: "20261023", raceBango: "11", queries: 3 },
  { date: "20261024", firstUnsettled: "20261023", raceBango: "12", queries: 3 },
  { date: "20250105", firstUnsettled: "20261023", raceBango: "11", queries: 2 },
  { date: "20261025", firstUnsettled: "20261023", raceBango: "11", queries: 3 },
  { date: "20270102", firstUnsettled: "20261231", raceBango: "11", queries: 3 },
  { date: "20270102", firstUnsettled: "20261231", raceBango: "12", queries: 3 },
])(
  "matches the former single query in DuckDB (race $date R$raceBango, cutoff $firstUnsettled)",
  async (scenario) => {
    await withFixture(async (run) => {
      const filters: HorseRaceResultsFilters = {
        ...nowFilters,
        date: scenario.date,
        raceBango: scenario.raceBango,
      };
      const current = await run(buildCurrentRunnersQuery(config, filters));
      const horseIds = currentHorseIds(current);
      const ranges = horseHistoryRanges(filters.date, scenario.firstUnsettled);
      const history = await Promise.all(
        ranges.map((range) =>
          run(buildHorseHistoryQuery({ env: config, filters, horseIds, range })),
        ),
      );
      const actual = uniqueHorseRaceResults(
        mergeHorseRaceResults(current, history.flat()).map(normaliseHorseRaceResultRow),
      );
      const expected = uniqueHorseRaceResults(
        (await run(oracleQuery(filters))).map(normaliseHorseRaceResultRow),
      );
      expect(actual.length).toBeGreaterThan(3);
      expect(actual).toStrictEqual(expected);
      expect(ranges.length + 1).toBe(scenario.queries);
    });
  },
);

it.each([
  { source: "jra", sourceScope: "jra", keibajoCode: "05", raceBango: "11" },
  { source: "jra", sourceScope: "nar", keibajoCode: "05", raceBango: "11" },
  { source: "nar", sourceScope: "all", keibajoCode: "42", raceBango: "03" },
  { source: "nar", sourceScope: "nar", keibajoCode: "42", raceBango: "03" },
] satisfies Partial<HorseRaceResultsFilters>[])(
  "matches the former single query for source $source and scope $sourceScope",
  async (scope) => {
    await withFixture(async (run) => {
      const filters: HorseRaceResultsFilters = { ...nowFilters, ...scope };
      const current = await run(buildCurrentRunnersQuery(config, filters));
      const horseIds = currentHorseIds(current);
      const history = await Promise.all(
        horseHistoryRanges(filters.date, "20261023").map((range) =>
          run(buildHorseHistoryQuery({ env: config, filters, horseIds, range })),
        ),
      );
      expect(
        uniqueHorseRaceResults(
          mergeHorseRaceResults(current, history.flat()).map(normaliseHorseRaceResultRow),
        ),
      ).toStrictEqual(
        uniqueHorseRaceResults((await run(oracleQuery(filters))).map(normaliseHorseRaceResultRow)),
      );
    });
  },
);

it("maps camelCase HorseRaceResult fields from snake_case R2 rows", () => {
  expect(
    normaliseHorseRaceResultRow({
      babajotai_code_dirt: "01",
      babajotai_code_shiba: "01",
      bamei: "Deep",
      banushimei: "Owner",
      barei: 4,
      bataiju: 480n,
      blinker_shiyo_kubun: true,
      chokyoshimei_ryakusho: "Trainer",
      corner_1: "01",
      corner_2: "02",
      corner_3: "03",
      corner_4: "04",
      current_barei: "05",
      current_jockey: "Take",
      current_seibetsu_code: "1",
      current_umaban: "01",
      futan_juryo: "550",
      grade_code: "A",
      hasso_jikoku: "1510",
      juryo_shubetsu_code: "3",
      kaisai_nen: "2025",
      kaisai_tsukihi: "0715",
      kakutei_chakujun: "01",
      keibajo_code: "05",
      ketto_toroku_bango: "2023100001",
      kishumei_ryakusho: "Take",
      kohan_3f: "351",
      kyori: "1600",
      kyoso_joken_code: "005",
      kyoso_joken_meisho: "3勝",
      kyoso_kigo_code: "A",
      kyosomei_fukudai: null,
      kyosomei_hondai: "テスト",
      kyosomei_kakkonai: { note: "x" },
      kyoso_shubetsu_code: "12",
      race_bango: 11,
      seibetsu_code: "1",
      shusso_tosu: "16",
      soha_time: "1345",
      tansho_ninkijun: "01",
      tansho_odds: "0250",
      tenko_code: "1",
      time_sa: "002",
      track_code: "17",
      umaban: "03",
      wakuban: "2",
      zogen_fugo: "+",
      zogen_sa: "004",
    }),
  ).toStrictEqual({
    babajotaiCodeDirt: "01",
    babajotaiCodeShiba: "01",
    bamei: "Deep",
    banushimei: "Owner",
    barei: "4",
    bataiju: "480",
    blinkerShiyoKubun: "true",
    chokyoshimeiRyakusho: "Trainer",
    corner1: "01",
    corner2: "02",
    corner3: "03",
    corner4: "04",
    currentBarei: "05",
    currentJockey: "Take",
    currentSeibetsuCode: "1",
    currentUmaban: "01",
    futanJuryo: "550",
    gradeCode: "A",
    hassoJikoku: "1510",
    juryoShubetsuCode: "3",
    kaisaiNen: "2025",
    kaisaiTsukihi: "0715",
    kakuteiChakujun: "01",
    keibajoCode: "05",
    kettoTorokuBango: "2023100001",
    kishumeiRyakusho: "Take",
    kohan3f: "351",
    kyori: "1600",
    kyosoJokenCode: "005",
    kyosoJokenMeisho: "3勝",
    kyosoKigoCode: "A",
    kyosomeiFukudai: null,
    kyosomeiHondai: "テスト",
    kyosomeiKakkonai: '{"note":"x"}',
    kyosoShubetsuCode: "12",
    raceBango: "11",
    seibetsuCode: "1",
    shussoTosu: "16",
    sohaTime: "1345",
    tanshoNinkijun: "01",
    tanshoOdds: "0250",
    tenkoCode: "1",
    timeSa: "002",
    trackCode: "17",
    umaban: "03",
    wakuban: "2",
    zogenFugo: "+",
    zogenSa: "004",
  });
});

it("rejects rows missing race identity fields", () => {
  expect(() =>
    normaliseHorseRaceResultRow({
      kaisai_nen: "",
      kaisai_tsukihi: "0715",
      keibajo_code: "05",
      race_bango: "01",
    }),
  ).toThrow("R2 SQL row is missing kaisai_nen");
  expect(() =>
    normaliseHorseRaceResultRow({
      kaisai_nen: "2025",
      kaisai_tsukihi: null,
      keibajo_code: "05",
      race_bango: "01",
    }),
  ).toThrow("R2 SQL row is missing kaisai_tsukihi");
  expect(() =>
    normaliseHorseRaceResultRow({
      kaisai_nen: "2025",
      kaisai_tsukihi: "0715",
      keibajo_code: "",
      race_bango: "01",
    }),
  ).toThrow("R2 SQL row is missing keibajo_code");
  expect(() =>
    normaliseHorseRaceResultRow({
      kaisai_nen: "2025",
      kaisai_tsukihi: "0715",
      keibajo_code: "05",
      race_bango: null,
    }),
  ).toThrow("R2 SQL row is missing race_bango");
});

it("keeps the first row for a duplicated horse-race key", () => {
  expect(
    uniqueHorseRaceResults([
      sampleRow,
      { ...sampleRow, bamei: "Duplicate" },
      { ...sampleRow, currentUmaban: "02", kaisaiNen: "2024" },
    ]),
  ).toStrictEqual([
    {
      babajotaiCodeDirt: "01",
      babajotaiCodeShiba: "01",
      bamei: "Deep",
      banushimei: "Owner",
      barei: "04",
      bataiju: "480",
      blinkerShiyoKubun: "1",
      chokyoshimeiRyakusho: "Trainer",
      corner1: "01",
      corner2: "02",
      corner3: "03",
      corner4: "04",
      currentBarei: "05",
      currentJockey: "Take",
      currentSeibetsuCode: "1",
      currentUmaban: "01",
      futanJuryo: "550",
      gradeCode: "A",
      hassoJikoku: "1510",
      juryoShubetsuCode: "3",
      kaisaiNen: "2025",
      kaisaiTsukihi: "0715",
      kakuteiChakujun: "01",
      keibajoCode: "05",
      kettoTorokuBango: "2023100001",
      kishumeiRyakusho: "Take",
      kohan3f: "351",
      kyori: "1600",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3勝",
      kyosoKigoCode: "A",
      kyosomeiFukudai: null,
      kyosomeiHondai: "テスト",
      kyosomeiKakkonai: null,
      kyosoShubetsuCode: "12",
      raceBango: "11",
      seibetsuCode: "1",
      shussoTosu: "16",
      sohaTime: "1345",
      tanshoNinkijun: "01",
      tanshoOdds: "0250",
      tenkoCode: "1",
      timeSa: "002",
      trackCode: "17",
      umaban: "03",
      wakuban: "2",
      zogenFugo: "+",
      zogenSa: "004",
    },
    {
      babajotaiCodeDirt: "01",
      babajotaiCodeShiba: "01",
      bamei: "Deep",
      banushimei: "Owner",
      barei: "04",
      bataiju: "480",
      blinkerShiyoKubun: "1",
      chokyoshimeiRyakusho: "Trainer",
      corner1: "01",
      corner2: "02",
      corner3: "03",
      corner4: "04",
      currentBarei: "05",
      currentJockey: "Take",
      currentSeibetsuCode: "1",
      currentUmaban: "02",
      futanJuryo: "550",
      gradeCode: "A",
      hassoJikoku: "1510",
      juryoShubetsuCode: "3",
      kaisaiNen: "2024",
      kaisaiTsukihi: "0715",
      kakuteiChakujun: "01",
      keibajoCode: "05",
      kettoTorokuBango: "2023100001",
      kishumeiRyakusho: "Take",
      kohan3f: "351",
      kyori: "1600",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3勝",
      kyosoKigoCode: "A",
      kyosomeiFukudai: null,
      kyosomeiHondai: "テスト",
      kyosomeiKakkonai: null,
      kyosoShubetsuCode: "12",
      raceBango: "11",
      seibetsuCode: "1",
      shussoTosu: "16",
      sohaTime: "1345",
      tanshoNinkijun: "01",
      tanshoOdds: "0250",
      tenkoCode: "1",
      timeSa: "002",
      trackCode: "17",
      umaban: "03",
      wakuban: "2",
      zogenFugo: "+",
      zogenSa: "004",
    },
  ]);
});
