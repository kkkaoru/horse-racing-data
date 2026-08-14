// Run with bun (bunx vitest)

import { getTableName, isTable } from "drizzle-orm";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

type ExecuteFn = (query: unknown) => Promise<{ rows: unknown[] }>;
type WithDbQueryCacheFn = <T>(keyParts: readonly unknown[], loader: () => Promise<T>) => Promise<T>;
type GetDbFn = () => { execute: ExecuteFn };

const { executeMock, withDbQueryCacheMock } = vi.hoisted(() => ({
  executeMock: vi.fn<ExecuteFn>(),
  withDbQueryCacheMock: vi.fn<WithDbQueryCacheFn>(),
}));

vi.mock("./client", () => ({
  getDb: vi.fn<GetDbFn>(() => ({
    execute: executeMock,
  })),
}));

vi.mock("./query-cache", () => ({
  withDbQueryCache: withDbQueryCacheMock,
}));

import type { FinishPositionBucketFilter } from "../lib/finish-prediction-dimensions";
import type { RaceDetail, Runner } from "../lib/race-types";
import type { RunningStyleBucketFilter } from "../lib/running-style-prediction-dimensions";
import {
  getFinishPositionBucketEvaluation,
  getFinishPositionLambdarankPredictions,
  getHorseDetailData,
  getHorseList,
  getHorseRaceResults,
  getRaceAbilityTests,
  getRaceRunners,
  getRaceTimeStats,
  getRaceTrainings,
  getRunningStyleBucketEvaluation,
  getSimilarRaceStats,
  getTimeScoreRows,
  searchFavoriteHorses,
} from "./queries";

interface DrizzleSqlLike {
  queryChunks?: unknown[];
}

const isDrizzleSqlLike = (value: unknown): value is DrizzleSqlLike =>
  typeof value === "object" && value !== null && "queryChunks" in value;

const isStringFragment = (value: unknown): value is { value: unknown[] } =>
  typeof value === "object" &&
  value !== null &&
  "value" in value &&
  Array.isArray((value as { value: unknown }).value);

const stringifyValueArray = (values: unknown[]): string =>
  values
    .map((entry) => {
      if (typeof entry === "string") {
        return entry;
      }
      if (typeof entry === "number" || typeof entry === "boolean") {
        return String(entry);
      }
      return "";
    })
    .join("");

const stringifyChunk = (chunk: unknown): string => {
  if (chunk === null || chunk === undefined) {
    return "";
  }
  if (typeof chunk === "string") {
    return `'${chunk}'`;
  }
  if (typeof chunk === "number" || typeof chunk === "boolean") {
    return String(chunk);
  }
  if (isStringFragment(chunk)) {
    return stringifyValueArray(chunk.value);
  }
  if (isDrizzleSqlLike(chunk)) {
    return stringifyQuery(chunk);
  }
  return "";
};

const stringifyQuery = (value: unknown): string => {
  if (!isDrizzleSqlLike(value)) {
    return "";
  }
  const chunks = value.queryChunks ?? [];
  return chunks.map((chunk) => stringifyChunk(chunk)).join("");
};

const collectTableNames = (value: unknown): string[] => {
  if (isTable(value)) {
    return [getTableName(value)];
  }
  if (!isDrizzleSqlLike(value)) {
    return [];
  }
  return (value.queryChunks ?? []).flatMap((chunk) => collectTableNames(chunk));
};

const ALL_FLAGS_ON_FILTER: RunningStyleBucketFilter = {
  category: "jra",
  conditionKey: null,
  enabled: {
    condition: true,
    distance: true,
    grade: true,
    keibajo: true,
    kyosoJoken: true,
    kyosoShubetsu: true,
    raceName: true,
    track: true,
  },
  gradeCode: "G3",
  keibajoCode: "05",
  kyori: 2400,
  kyosoJokenCode: "999",
  kyosoShubetsuCode: "11",
  period: "all",
  raceName: "東京新聞杯",
  source: "jra",
  trackCode: "10",
};

const OOS_ONLY_FILTER: RunningStyleBucketFilter = {
  category: "jra",
  conditionKey: null,
  enabled: {
    condition: true,
    distance: true,
    grade: true,
    keibajo: true,
    kyosoJoken: true,
    kyosoShubetsu: true,
    raceName: true,
    track: true,
  },
  gradeCode: "G3",
  keibajoCode: "05",
  kyori: 2400,
  kyosoJokenCode: "999",
  kyosoShubetsuCode: "11",
  period: "oos-only",
  raceName: "東京新聞杯",
  source: "jra",
  trackCode: "10",
};

const KEIBAJO_ONLY_FILTER: RunningStyleBucketFilter = {
  category: "jra",
  conditionKey: null,
  enabled: {
    condition: false,
    distance: false,
    grade: false,
    keibajo: true,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  },
  gradeCode: null,
  keibajoCode: "05",
  kyori: 2400,
  kyosoJokenCode: null,
  kyosoShubetsuCode: "11",
  period: "all",
  raceName: null,
  source: "jra",
  trackCode: null,
};

const ALL_FLAGS_OFF_FILTER: RunningStyleBucketFilter = {
  category: "jra",
  conditionKey: null,
  enabled: {
    condition: false,
    distance: false,
    grade: false,
    keibajo: false,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  },
  gradeCode: null,
  keibajoCode: "05",
  kyori: 2400,
  kyosoJokenCode: null,
  kyosoShubetsuCode: "11",
  period: "all",
  raceName: null,
  source: "jra",
  trackCode: null,
};

const NAR_FILTER: RunningStyleBucketFilter = {
  category: "nar",
  conditionKey: "A1",
  enabled: {
    condition: true,
    distance: false,
    grade: false,
    keibajo: true,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  },
  gradeCode: null,
  keibajoCode: "44",
  kyori: 1800,
  kyosoJokenCode: null,
  kyosoShubetsuCode: "11",
  period: "all",
  raceName: null,
  source: "nar",
  trackCode: null,
};

const BAN_EI_FILTER = {
  category: "ban-ei",
  conditionKey: null,
  enabled: {
    condition: false,
    distance: false,
    grade: false,
    keibajo: false,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  },
  gradeCode: null,
  keibajoCode: "83",
  kyori: 200,
  kyosoJokenCode: null,
  kyosoShubetsuCode: "11",
  period: "all",
  raceName: null,
  source: "nar",
  trackCode: null,
} satisfies Omit<RunningStyleBucketFilter, "category"> & { category: string };

const PERFECT_AGGREGATE_ROW = {
  cm_nn: "10",
  cm_no: "0",
  cm_ns: "0",
  cm_nsh: "0",
  cm_on: "0",
  cm_oo: "10",
  cm_os: "0",
  cm_osh: "0",
  cm_shn: "0",
  cm_sho: "0",
  cm_shs: "0",
  cm_shsh: "10",
  cm_sn: "0",
  cm_so: "0",
  cm_ss: "10",
  cm_ssh: "0",
  log_loss_nige_count: "10",
  log_loss_nige_sum: "5",
  log_loss_oikomi_count: "10",
  log_loss_oikomi_sum: "8",
  log_loss_sashi_count: "10",
  log_loss_sashi_sum: "6",
  log_loss_senkou_count: "10",
  log_loss_senkou_sum: "4",
  prediction_count: "40",
  race_count: "5",
  top2_hit_count: "38",
  corner1_pair_score_sum: "18",
  corner1_pair_score_count: "20",
  corner3_pair_score_sum: "17",
  corner3_pair_score_count: "20",
  corner4_pair_score_sum: "16",
  corner4_pair_score_count: "20",
  finish_pair_score_sum: "15",
  finish_pair_score_count: "20",
};

beforeEach(() => {
  executeMock.mockReset();
  withDbQueryCacheMock.mockReset();
  withDbQueryCacheMock.mockImplementation(
    async (_keyParts: unknown, loader: () => Promise<unknown>) => loader(),
  );
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("getRaceRunners joins JRA overseas identities by the complete race-entry key", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getRaceRunners("jra", "2026", "08", "16", "A8", "04");

  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(collectTableNames(queryArg)).toStrictEqual([
    "jvd_se",
    "jvd_um",
    "oversea_runner_identity",
  ]);
  expect(queryText).toMatch(/identity\.race_source = '\s*jra\s*'/u);
  expect(queryText).toMatch(/identity\.kaisai_nen = se\.kaisai_nen/u);
  expect(queryText).toMatch(/identity\.kaisai_tsukihi = se\.kaisai_tsukihi/u);
  expect(queryText).toMatch(/identity\.keibajo_code = se\.keibajo_code/u);
  expect(queryText).toMatch(/identity\.race_bango = se\.race_bango/u);
  expect(queryText).toMatch(/identity\.umaban = se\.umaban/u);
  expect(queryText).toMatch(/identity\.horse_name_full/u);
  expect(queryText).toMatch(/identity\.source_horse_id/u);
});

it("getHorseRaceResults excludes empty and all-zero identities for a JRA current race", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getHorseRaceResults("jra", "2026", "08", "16", "A8", "04");

  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(withDbQueryCacheMock.mock.calls[0]?.[0].slice(0, 2)).toStrictEqual([
    "getHorseRaceResults",
    "v2",
  ]);
  expect(collectTableNames(queryArg)[0]).toBe("jvd_se");
  expect(queryText).toMatch(/btrim\(ketto_toroku_bango\) not in \('\s*'\)/u);
  expect(queryText).toMatch(/btrim\(ketto_toroku_bango\) !~ '\s*\^0\+\$\s*'/u);
});

it("getHorseRaceResults joins source-mapped overseas histories without treating placeholders as JV identities", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getHorseRaceResults("jra", "2026", "08", "16", "A8", "04");

  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(collectTableNames(queryArg)).toStrictEqual([
    "jvd_se",
    "jvd_se",
    "jvd_ra",
    "nvd_se",
    "nvd_ra",
    "oversea_runner_source_id",
    "jvd_se",
    "oversea_horse_race_history",
    "oversea_runner_identity",
  ]);
  expect(queryText).toMatch(/past\.source = mapping\.source/u);
  expect(queryText).toMatch(/past\.source_horse_id = mapping\.source_horse_id/u);
  expect(queryText).toMatch(/mapping\.race_source = '\s*jra\s*'/u);
  expect(queryText).toMatch(/mapping\.source = '\s*netkeiba\s*'/u);
  expect(queryText).toMatch(/mapping\.umaban as "currentUmaban"/u);
  expect(queryText).toMatch(/mapping\.source_horse_id as "kettoTorokuBango"/u);
  expect(queryText).toMatch(/btrim\(current_se\.ketto_toroku_bango\) ~ '\s*\^0\+\$\s*'/u);
  expect(queryText).toMatch(/past\.race_date < '\s*20260816\s*'::date/u);
});

it("getHorseRaceResults uses NAR runners for the current identity set", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getHorseRaceResults("nar", "2026", "08", "16", "A8", "04");

  const queryArg = executeMock.mock.calls[0]?.[0];
  expect(collectTableNames(queryArg)[0]).toBe("nvd_se");
});

it("getHorseList excludes all-zero identities from both recent JRA and NAR candidates", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getHorseList({
    date: "",
    dateFrom: "",
    dateTo: "",
    distanceMax: "",
    distanceMin: "",
    jockeyName: "",
    keibajoCode: "",
    last3fMax: "",
    last3fMin: "",
    oddsMax: "",
    oddsMin: "",
    order: "latest",
    popularityMax: "",
    popularityMin: "",
    q: "",
    raceNumber: "",
    raceTimeMax: "",
    raceTimeMin: "",
    rank: "",
    source: "all",
    surface: "",
    trainerName: "",
    turn: "",
  });

  const queryText = stringifyQuery(executeMock.mock.calls[0]?.[0]);
  expect(
    queryText.match(/btrim\(coalesce\(ketto_toroku_bango, ''\)\) !~ '\^0\+\$'/gu),
  ).toHaveLength(2);
});

it("getHorseList excludes all-zero identities from both filtered JRA and NAR aggregates", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getHorseList({
    date: "",
    dateFrom: "",
    dateTo: "",
    distanceMax: "",
    distanceMin: "",
    jockeyName: "",
    keibajoCode: "",
    last3fMax: "",
    last3fMin: "",
    oddsMax: "",
    oddsMin: "",
    order: "name",
    popularityMax: "",
    popularityMin: "",
    q: "2023100001",
    raceNumber: "",
    raceTimeMax: "",
    raceTimeMin: "",
    rank: "",
    source: "all",
    surface: "",
    trainerName: "",
    turn: "",
  });

  const queryText = stringifyQuery(executeMock.mock.calls[0]?.[0]);
  expect(
    queryText.match(/btrim\(coalesce\(ketto_toroku_bango, ''\)\) !~ '\^0\+\$'/gu),
  ).toHaveLength(2);
});

it("searchFavoriteHorses excludes all-zero identities from recent and fallback searches", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await searchFavoriteHorses("2023100001");

  const recentQueryText = stringifyQuery(executeMock.mock.calls[0]?.[0]);
  const fallbackQueryText = stringifyQuery(executeMock.mock.calls[1]?.[0]);
  expect(
    recentQueryText.match(/btrim\(coalesce\(ketto_toroku_bango, ''\)\) !~ '\^0\+\$'/gu),
  ).toHaveLength(2);
  expect(
    fallbackQueryText.match(/btrim\(coalesce\(ketto_toroku_bango, ''\)\) !~ '\^0\+\$'/gu),
  ).toHaveLength(2);
});

it("getRaceAbilityTests excludes all-zero current NAR identities", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getRaceAbilityTests("nar", "2026", "08", "16", "35", "04");

  const queryText = stringifyQuery(executeMock.mock.calls[0]?.[0]);
  expect(queryText).toMatch(/btrim\(coalesce\(ketto_toroku_bango, ''\)\) !~ '\^0\+\$'/u);
});

it("getHorseDetailData rejects a direct all-zero placeholder without querying", async () => {
  const result = await getHorseDetailData(" 0000000000 ", {
    date: "",
    dateFrom: "",
    dateTo: "",
    distanceMax: "",
    distanceMin: "",
    jockeyName: "",
    keibajoCode: "",
    last3fMax: "",
    last3fMin: "",
    oddsMax: "",
    oddsMin: "",
    order: "latest",
    popularityMax: "",
    popularityMin: "",
    q: "",
    raceNumber: "",
    raceTimeMax: "",
    raceTimeMin: "",
    rank: "all",
    source: "all",
    surface: "all",
    trainerName: "",
    turn: "all",
  });

  expect(result).toBe(null);
  expect(withDbQueryCacheMock.mock.calls.length).toBe(0);
  expect(executeMock.mock.calls.length).toBe(0);
});

it("getHorseDetailData queries a real registered horse identity", async () => {
  executeMock.mockResolvedValue({
    rows: [{ horseName: "登録馬", popularity: "1", rank: "01", winOdds: "20" }],
  });

  const result = await getHorseDetailData("2020100001", {
    date: "",
    dateFrom: "",
    dateTo: "",
    distanceMax: "",
    distanceMin: "",
    jockeyName: "",
    keibajoCode: "",
    last3fMax: "",
    last3fMin: "",
    oddsMax: "",
    oddsMin: "",
    order: "latest",
    popularityMax: "",
    popularityMin: "",
    q: "",
    raceNumber: "",
    raceTimeMax: "",
    raceTimeMin: "",
    rank: "all",
    source: "all",
    surface: "all",
    trainerName: "",
    turn: "all",
  });

  expect(result?.summary.name).toBe("登録馬");
  expect(withDbQueryCacheMock.mock.calls.length).toBe(1);
  expect(executeMock.mock.calls.length).toBe(1);
});

it("getSimilarRaceStats counts placeholder entries separately without counting an empty left join", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getSimilarRaceStats(PERCLASS_703_RACE, {
    classConditionName: null,
    includeAge: false,
    includeBloodlineAncestors: false,
    includeClass: false,
    includeDistance: false,
    includeFrame: false,
    includeMonthWindow: false,
    includeNarOnly: false,
    includeRaceNumber: false,
    includeRaceSubtitle: false,
    includeRaceTitle: false,
    includeRunnerCount: false,
    includeSex: false,
    includeSurface: false,
    includeTurn: false,
    includeVenue: false,
    includeWeight: false,
    runnerCount: null,
    sourceScope: "all",
    years: null,
  });

  const queryText = stringifyQuery(executeMock.mock.calls[0]?.[0]);
  expect(queryText).toMatch(/when ranked_grouped_entries\.name is null then null/u);
  expect(queryText).toMatch(
    /then 'horse:' \|\| btrim\(ranked_grouped_entries\.ketto_toroku_bango\)/u,
  );
  expect(queryText).toMatch(
    /else concat_ws\(\s*':'\s*,\s*'entry'\s*,\s*ranked_grouped_entries\.race_source\s*,\s*ranked_grouped_entries\.kaisai_nen/u,
  );
  expect(queryText).not.toMatch(/count\(distinct ranked_grouped_entries\.ketto_toroku_bango\)/u);
});

it("getTimeScoreRows resolves mapped overseas histories without sharing placeholder identities", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getTimeScoreRows(PERCLASS_703_RACE, {
    classConditionName: null,
    includeAge: false,
    includeBloodlineAncestors: false,
    includeClass: false,
    includeDistance: false,
    includeFrame: false,
    includeMonthWindow: false,
    includeNarOnly: false,
    includeRaceNumber: false,
    includeRaceSubtitle: false,
    includeRaceTitle: false,
    includeRunnerCount: false,
    includeSex: false,
    includeSurface: false,
    includeTurn: false,
    includeVenue: false,
    includeWeight: false,
    runnerCount: null,
    sourceScope: "all",
    years: null,
  });

  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(withDbQueryCacheMock.mock.calls[0]?.[0].slice(0, 2)).toStrictEqual([
    "getTimeScoreRows",
    "v2",
  ]);
  expect(collectTableNames(queryArg)).toStrictEqual([
    "jvd_se",
    "oversea_runner_source_id",
    "jvd_ra",
    "jvd_se",
    "jvd_se",
    "jvd_se",
    "jvd_ra",
    "nvd_se",
    "nvd_ra",
    "oversea_horse_race_history",
  ]);
  expect(queryText).toMatch(/when btrim\(se\.ketto_toroku_bango\) ~ '\^0\+\$'/u);
  expect(queryText).toMatch(/then mapping\.source_horse_id/u);
  expect(queryText).toMatch(/mapping\.source = '\s*netkeiba\s*'/u);
  expect(queryText).toMatch(/or mapping\.source_horse_id is not null/u);
  expect(queryText).toMatch(/past\.source = '\s*netkeiba\s*'/u);
  expect(queryText).toMatch(/past\.source_horse_id in \(select history_horse_id/u);
  expect(queryText).toMatch(/filter \(where history\.keibajo_code is not null\)/u);
});

it("getRaceTimeStats keeps placeholder runners but blocks cross-race horse history joins", async () => {
  executeMock.mockResolvedValue({ rows: [] });

  await getRaceTimeStats(PERCLASS_703_RACE, {
    classConditionName: null,
    includeAge: false,
    includeBloodlineAncestors: false,
    includeClass: false,
    includeDistance: false,
    includeFrame: false,
    includeMonthWindow: false,
    includeNarOnly: false,
    includeRaceNumber: false,
    includeRaceSubtitle: false,
    includeRaceTitle: false,
    includeRunnerCount: false,
    includeSex: false,
    includeSurface: false,
    includeTurn: false,
    includeVenue: false,
    includeWeight: false,
    runnerCount: null,
    sourceScope: "all",
    years: null,
  });

  const queryText = stringifyQuery(executeMock.mock.calls[0]?.[0]);
  expect(queryText).toMatch(
    /on btrim\(coalesce\(current_entries\.ketto_toroku_bango, ''\)\) <> ''/u,
  );
  expect(queryText).toMatch(
    /btrim\(coalesce\(current_entries\.ketto_toroku_bango, ''\)\) !~ '\^0\+\$'/u,
  );
});

it("getRunningStyleBucketEvaluation emits SQL with all dimension predicates when all flags are on", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/b\.keibajo_code = /u);
  expect(queryText).toMatch(/b\.kyori = /u);
  expect(queryText).toMatch(/b\.kyoso_shubetsu_code = /u);
  expect(queryText).toMatch(/b\.kyoso_joken_code = /u);
  expect(queryText).toMatch(/b\.condition_key = /u);
  expect(queryText).toMatch(/b\.track_code = /u);
  expect(queryText).toMatch(/b\.grade_code = /u);
  expect(queryText).toMatch(/regexp_replace\(b\.race_name, /u);
});

it("getRunningStyleBucketEvaluation omits dimension predicates when only keibajo flag is on", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: KEIBAJO_ONLY_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/b\.keibajo_code = /u);
  expect(queryText).not.toMatch(/b\.kyori = /u);
  expect(queryText).not.toMatch(/b\.kyoso_shubetsu_code = /u);
  expect(queryText).not.toMatch(/b\.kyoso_joken_code = /u);
  expect(queryText).not.toMatch(/b\.condition_key = /u);
  expect(queryText).not.toMatch(/b\.track_code = /u);
  expect(queryText).not.toMatch(/b\.grade_code = /u);
  expect(queryText).not.toMatch(/regexp_replace\(b\.race_name, /u);
});

it("getRunningStyleBucketEvaluation omits all dimension predicates when every flag is off", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_OFF_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).not.toMatch(/b\.keibajo_code = /u);
  expect(queryText).not.toMatch(/b\.kyori = /u);
  expect(queryText).not.toMatch(/b\.kyoso_shubetsu_code = /u);
  expect(queryText).not.toMatch(/b\.kyoso_joken_code = /u);
  expect(queryText).not.toMatch(/b\.condition_key = /u);
  expect(queryText).not.toMatch(/b\.track_code = /u);
  expect(queryText).not.toMatch(/b\.grade_code = /u);
  expect(queryText).not.toMatch(/regexp_replace\(b\.race_name, /u);
});

it("getRunningStyleBucketEvaluation normalises trailing U+3000 padding on race_name via regexp_replace", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(
    /regexp_replace\(b\.race_name, '\^\[\[:space:\]　\]\+\|\[\[:space:\]　\]\+\$', '', 'g'\) = /u,
  );
});

it("getRunningStyleBucketEvaluation returns null when SQL returns zero rows", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  const result = await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_ON_FILTER });
  expect(result).toBe(null);
});

it("getRunningStyleBucketEvaluation guards against feature_version drift using a latest_versions subquery", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/max\(running_style_feature_version\)/u);
  expect(queryText).toMatch(/latest_versions/u);
  expect(queryText).toMatch(/group by model_version/u);
});

it("getRunningStyleBucketEvaluation aggregates all 16 confusion matrix cells", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/sum\(cm_actual_nige_pred_nige_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_nige_pred_senkou_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_nige_pred_sashi_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_nige_pred_oikomi_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_senkou_pred_nige_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_senkou_pred_senkou_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_senkou_pred_sashi_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_senkou_pred_oikomi_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_sashi_pred_nige_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_sashi_pred_senkou_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_sashi_pred_sashi_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_sashi_pred_oikomi_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_oikomi_pred_nige_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_oikomi_pred_senkou_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_oikomi_pred_sashi_count\)/u);
  expect(queryText).toMatch(/sum\(cm_actual_oikomi_pred_oikomi_count\)/u);
});

it("getRunningStyleBucketEvaluation aggregates the 8 per-class log loss columns, top2 hits, and order-pair scores", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/sum\(log_loss_nige_sum\)/u);
  expect(queryText).toMatch(/sum\(log_loss_nige_count\)/u);
  expect(queryText).toMatch(/sum\(log_loss_senkou_sum\)/u);
  expect(queryText).toMatch(/sum\(log_loss_senkou_count\)/u);
  expect(queryText).toMatch(/sum\(log_loss_sashi_sum\)/u);
  expect(queryText).toMatch(/sum\(log_loss_sashi_count\)/u);
  expect(queryText).toMatch(/sum\(log_loss_oikomi_sum\)/u);
  expect(queryText).toMatch(/sum\(log_loss_oikomi_count\)/u);
  expect(queryText).toMatch(/sum\(top2_hit_count\)/u);
  expect(queryText).toMatch(/sum\(corner1_pair_score_sum\)/u);
  expect(queryText).toMatch(/sum\(corner1_pair_score_count\)/u);
  expect(queryText).toMatch(/sum\(corner3_pair_score_sum\)/u);
  expect(queryText).toMatch(/sum\(corner3_pair_score_count\)/u);
  expect(queryText).toMatch(/sum\(corner4_pair_score_sum\)/u);
  expect(queryText).toMatch(/sum\(corner4_pair_score_count\)/u);
  expect(queryText).toMatch(/sum\(finish_pair_score_sum\)/u);
  expect(queryText).toMatch(/sum\(finish_pair_score_count\)/u);
});

it("getRunningStyleBucketEvaluation skips SQL and returns null for ban-ei category", async () => {
  const result = await getRunningStyleBucketEvaluation({ filter: BAN_EI_FILTER });
  expect(result).toBe(null);
  expect(executeMock).not.toHaveBeenCalled();
});

it("getRunningStyleBucketEvaluation translates aggregate row into RunningStyleBucketMetrics on happy path", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  const result = await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_ON_FILTER });
  expect(result?.accuracy).toBe(1);
  expect(result?.predictionCount).toBe(40);
  expect(result?.raceCount).toBe(5);
  expect(result?.top2Accuracy).toBe(0.95);
  expect(result?.corner1PairScore).toStrictEqual({ pairCount: 20, score: 0.9 });
  expect(result?.corner3PairScore).toStrictEqual({ pairCount: 20, score: 0.85 });
  expect(result?.corner4PairScore).toStrictEqual({ pairCount: 20, score: 0.8 });
  expect(result?.finishPairScore).toStrictEqual({ pairCount: 20, score: 0.75 });
  expect(result?.perClass.nige.accuracy).toBe(1);
});

it("getRunningStyleBucketEvaluation emits nar source filter and condition predicate for NAR races", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: NAR_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/'nar'/u);
  expect(queryText).toMatch(/b\.condition_key = /u);
  expect(queryText).toMatch(/b\.keibajo_code = /u);
});

it("getRunningStyleBucketEvaluation omits the evaluation_window_from predicate when period is all", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).not.toMatch(/b\.evaluation_window_from/u);
});

it("getRunningStyleBucketEvaluation injects evaluation_window_from OOS bounds when period is oos-only", async () => {
  executeMock.mockResolvedValue({ rows: [PERFECT_AGGREGATE_ROW] });
  await getRunningStyleBucketEvaluation({ filter: OOS_ONLY_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/b\.evaluation_window_from </u);
  expect(queryText).toMatch(/b\.evaluation_window_from >=/u);
  expect(queryText).toMatch(/'20160101'/u);
  expect(queryText).toMatch(/'20260101'/u);
});

const FINISH_ALL_FLAGS_ON_FILTER: FinishPositionBucketFilter = {
  category: "jra",
  conditionKey: null,
  enabled: {
    condition: false,
    distance: true,
    grade: true,
    keibajo: true,
    kyosoJoken: true,
    kyosoShubetsu: true,
    raceName: true,
    track: true,
  },
  gradeCode: "G3",
  keibajoCode: "05",
  kyori: 2400,
  kyosoJokenCode: "999",
  kyosoShubetsuCode: "11",
  modelVersion: "jra-cb-v7-lineage-wf-21y",
  period: "all",
  raceName: "東京新聞杯",
  source: "jra",
  trackCode: "10",
};

const FINISH_KEIBAJO_ONLY_FILTER: FinishPositionBucketFilter = {
  category: "jra",
  conditionKey: null,
  enabled: {
    condition: false,
    distance: false,
    grade: false,
    keibajo: true,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  },
  gradeCode: null,
  keibajoCode: "05",
  kyori: 2400,
  kyosoJokenCode: null,
  kyosoShubetsuCode: "11",
  modelVersion: "jra-cb-v7-lineage-wf-21y",
  period: "all",
  raceName: null,
  source: "jra",
  trackCode: null,
};

const FINISH_OOS_ONLY_FILTER: FinishPositionBucketFilter = {
  category: "nar",
  conditionKey: null,
  enabled: {
    condition: false,
    distance: false,
    grade: false,
    keibajo: true,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  },
  gradeCode: null,
  keibajoCode: "44",
  kyori: 1800,
  kyosoJokenCode: null,
  kyosoShubetsuCode: "11",
  modelVersion: "nar-xgb-v7-lineage-wf-21y",
  period: "oos-only",
  raceName: null,
  source: "nar",
  trackCode: null,
};

const FINISH_AGGREGATE_ROW = {
  ndcg_at_3_race_count: "100",
  ndcg_at_3_sum: "63",
  pair_score_pair_count: "5000",
  pair_score_sum: "3500",
  place1_hit_sum: "52",
  place2_hit_sum: "28",
  place3_hit_sum: "20",
  prediction_count: "1500",
  race_count: "100",
  top1_hit_sum: "52",
  top3_box_hit_sum: "12",
  top3_exact_hit_sum: "3",
  top3_place_relation_sum: "57",
  top3_winner_capture_sum: "71",
  top5_winner_capture_sum: "86",
};

it("getFinishPositionBucketEvaluation pins the explicit model_version predicate without a latest_versions CTE", async () => {
  executeMock.mockResolvedValue({ rows: [FINISH_AGGREGATE_ROW] });
  await getFinishPositionBucketEvaluation({ filter: FINISH_ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/b\.model_version = /u);
  expect(queryText).toMatch(/'jra-cb-v7-lineage-wf-21y'/u);
  expect(queryText).not.toMatch(/latest_versions/u);
});

it("getFinishPositionBucketEvaluation emits all eight dimension predicates when every flag is on", async () => {
  executeMock.mockResolvedValue({ rows: [FINISH_AGGREGATE_ROW] });
  await getFinishPositionBucketEvaluation({ filter: FINISH_ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/b\.keibajo_code = /u);
  expect(queryText).toMatch(/b\.kyori = /u);
  expect(queryText).toMatch(/b\.kyoso_shubetsu_code = /u);
  expect(queryText).toMatch(/b\.kyoso_joken_code = /u);
  expect(queryText).toMatch(/b\.track_code = /u);
  expect(queryText).toMatch(/b\.grade_code = /u);
  expect(queryText).toMatch(/regexp_replace\(b\.race_name, /u);
});

it("getFinishPositionBucketEvaluation omits dimension predicates when only keibajo flag is on", async () => {
  executeMock.mockResolvedValue({ rows: [FINISH_AGGREGATE_ROW] });
  await getFinishPositionBucketEvaluation({ filter: FINISH_KEIBAJO_ONLY_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/b\.keibajo_code = /u);
  expect(queryText).not.toMatch(/b\.kyori = /u);
  expect(queryText).not.toMatch(/b\.kyoso_shubetsu_code = /u);
  expect(queryText).not.toMatch(/b\.track_code = /u);
  expect(queryText).not.toMatch(/b\.grade_code = /u);
  expect(queryText).not.toMatch(/regexp_replace\(b\.race_name, /u);
});

it("getFinishPositionBucketEvaluation aggregates all fifteen ranking metric columns", async () => {
  executeMock.mockResolvedValue({ rows: [FINISH_AGGREGATE_ROW] });
  await getFinishPositionBucketEvaluation({ filter: FINISH_ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/sum\(top1_hit_sum\)/u);
  expect(queryText).toMatch(/sum\(place1_hit_sum\)/u);
  expect(queryText).toMatch(/sum\(place2_hit_sum\)/u);
  expect(queryText).toMatch(/sum\(place3_hit_sum\)/u);
  expect(queryText).toMatch(/sum\(top3_box_hit_sum\)/u);
  expect(queryText).toMatch(/sum\(top3_exact_hit_sum\)/u);
  expect(queryText).toMatch(/sum\(top3_winner_capture_sum\)/u);
  expect(queryText).toMatch(/sum\(top5_winner_capture_sum\)/u);
  expect(queryText).toMatch(/sum\(top3_place_relation_sum\)/u);
  expect(queryText).toMatch(/sum\(pair_score_sum\)/u);
  expect(queryText).toMatch(/sum\(pair_score_pair_count\)/u);
  expect(queryText).toMatch(/sum\(ndcg_at_3_sum\)/u);
  expect(queryText).toMatch(/sum\(ndcg_at_3_race_count\)/u);
});

it("getFinishPositionBucketEvaluation omits the evaluation_window_from predicate when period is all", async () => {
  executeMock.mockResolvedValue({ rows: [FINISH_AGGREGATE_ROW] });
  await getFinishPositionBucketEvaluation({ filter: FINISH_ALL_FLAGS_ON_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).not.toMatch(/b\.evaluation_window_from/u);
});

it("getFinishPositionBucketEvaluation injects evaluation_window_from OOS bounds when period is oos-only", async () => {
  executeMock.mockResolvedValue({ rows: [FINISH_AGGREGATE_ROW] });
  await getFinishPositionBucketEvaluation({ filter: FINISH_OOS_ONLY_FILTER });
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/b\.evaluation_window_from </u);
  expect(queryText).toMatch(/b\.evaluation_window_from >=/u);
  expect(queryText).toMatch(/'20240101'/u);
  expect(queryText).toMatch(/'20260101'/u);
});

it("getFinishPositionBucketEvaluation returns null when SQL returns zero rows", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  const result = await getFinishPositionBucketEvaluation({ filter: FINISH_ALL_FLAGS_ON_FILTER });
  expect(result).toBe(null);
});

it("getFinishPositionBucketEvaluation derives accuracies and averages from the aggregate row", async () => {
  executeMock.mockResolvedValue({ rows: [FINISH_AGGREGATE_ROW] });
  const result = await getFinishPositionBucketEvaluation({ filter: FINISH_ALL_FLAGS_ON_FILTER });
  expect(result?.raceCount).toBe(100);
  expect(result?.predictionCount).toBe(1500);
  expect(result?.top1Accuracy).toBe(0.52);
  expect(result?.place2Accuracy).toBe(0.28);
  expect(result?.pairScoreAvg).toBe(0.7);
  expect(result?.ndcgAt3Avg).toBe(0.63);
  expect(result?.smallSampleWarning).toBe(false);
});

it("getFinishPositionBucketEvaluation flags a small sample when race_count is below thirty", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        ndcg_at_3_race_count: "10",
        ndcg_at_3_sum: "6",
        pair_score_pair_count: "0",
        pair_score_sum: "0",
        place1_hit_sum: "5",
        place2_hit_sum: "2",
        place3_hit_sum: "1",
        prediction_count: "120",
        race_count: "10",
        top1_hit_sum: "5",
        top3_box_hit_sum: "1",
        top3_exact_hit_sum: "0",
        top3_place_relation_sum: "4",
        top3_winner_capture_sum: "6",
        top5_winner_capture_sum: "8",
      },
    ],
  });
  const result = await getFinishPositionBucketEvaluation({ filter: FINISH_KEIBAJO_ONLY_FILTER });
  expect(result?.smallSampleWarning).toBe(true);
  expect(result?.pairScoreAvg).toBe(0);
});

const PERCLASS_703_RACE: RaceDetail = {
  babajotaiCodeDirt: "0",
  babajotaiCodeShiba: "0",
  gradeCode: null,
  hassoJikoku: "1430",
  jockeyNames: [],
  kaisaiKai: "2",
  kaisaiNen: "2026",
  kaisaiNichime: "5",
  kaisaiTsukihi: "0608",
  keibajoCode: "05",
  kyori: "1600",
  kyosoJokenCode: "703",
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: "11",
  juryoShubetsuCode: "1",
  raceBango: "11",
  shussoTosu: "16",
  source: "jra",
  tenkoCode: "1",
  torokuTosu: "16",
  trackCode: "10",
};

const PERCLASS_703_RUNNERS: Runner[] = [
  {
    bamei: "Alpha",
    banushimei: null,
    barei: "4",
    bataiju: "480",
    chokyoshimeiRyakusho: null,
    corner1: null,
    corner2: null,
    corner3: null,
    corner4: null,
    damSireName: null,
    futanJuryo: "560",
    kakuteiChakujun: null,
    kettoTorokuBango: "2020100001",
    kishumeiRyakusho: null,
    kohan3f: null,
    moshokuCode: null,
    seibetsuCode: "1",
    sireName: null,
    sireSireName: null,
    sohaTime: null,
    tanshoNinkijun: null,
    tanshoOdds: null,
    timeSa: null,
    umaban: "1",
    wakuban: "1",
    zogenFugo: null,
    zogenSa: null,
  },
  {
    bamei: "Bravo",
    banushimei: null,
    barei: "4",
    bataiju: "490",
    chokyoshimeiRyakusho: null,
    corner1: null,
    corner2: null,
    corner3: null,
    corner4: null,
    damSireName: null,
    futanJuryo: "560",
    kakuteiChakujun: null,
    kettoTorokuBango: "2020100002",
    kishumeiRyakusho: null,
    kohan3f: null,
    moshokuCode: null,
    seibetsuCode: "1",
    sireName: null,
    sireSireName: null,
    sohaTime: null,
    tanshoNinkijun: null,
    tanshoOdds: null,
    timeSa: null,
    umaban: "2",
    wakuban: "2",
    zogenFugo: null,
    zogenSa: null,
  },
];

const NAR_CELL_RACE: RaceDetail = {
  ...PERCLASS_703_RACE,
  gradeCode: "E",
  kaisaiTsukihi: "0702",
  keibajoCode: "54",
  kyori: "1400",
  kyosoJokenCode: null,
  raceBango: "03",
  source: "nar",
  trackCode: "20",
};

it("getFinishPositionLambdarankPredictions ignores stale subclass active rows", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "jra-cb-v9-sim-2013",
        predicted_rank: 1,
        predicted_score: "0.91",
        shusso_tosu: 2,
        umaban: 1,
      },
    ],
  });
  await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/from finish_position_active_models/u);
  expect(queryText).toMatch(/where category = /u);
  expect(queryText).toMatch(/'jra'/u);
  expect(queryText).toMatch(/and subclass is null/u);
  expect(queryText).not.toMatch(/subclass = /u);
  expect(queryText).not.toMatch(/order by \(subclass is null\) asc/u);
});

it("getFinishPositionLambdarankPredictions does not prioritize reverted NAR a957 cell rows", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(NAR_CELL_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/allowed_model_versions\(model_version\) as/u);
  expect(queryText).not.toMatch(/nar-xgb-cell-a957d8b4-v1/u);
  expect(queryText).toMatch(/'nar'/u);
});

it("getFinishPositionLambdarankPredictions gates priority 0 to false for a JRA race matching no cell-routing rule", async () => {
  const unroutedJraRace: RaceDetail = {
    ...PERCLASS_703_RACE,
    keibajoCode: "05",
    kyosoJokenCode: "010",
  };
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(unroutedJraRace, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/select p0\.model_version, 0 as priority/u);
  expect(queryText).toMatch(/from race_finish_position_model_predictions p0/u);
  expect(queryText).toMatch(/where false\s+and p0\.source = 'jra'/u);
});

it("getFinishPositionLambdarankPredictions always attempts the NAR transformer blend as priority 0", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(NAR_CELL_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/where p0\.model_version = 'iter40-nar-settransformer-blend-v1'/u);
  expect(queryText).toMatch(/'iter40-nar-settransformer-blend-v1'/u);
});

it("getFinishPositionLambdarankPredictions emits priority 0 cell-routing branch for a routed JRA race", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(
    /where p0\.model_version = 'jra-cb-v9-sim-2013-clean-jockey-pedigree269'/u,
  );
  expect(queryText).toMatch(/'jra-cb-v9-sim-2013-clean-jockey-pedigree269'/u);
  expect(queryText).toMatch(/'jra-cb-v10-prior-corner274-2013'/u);
});

it("getFinishPositionLambdarankPredictions returns rows selected via the priority 0 cell-routing branch", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
        predicted_rank: 1,
        predicted_score: "0.93",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
        predicted_rank: 2,
        predicted_score: "0.51",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result.length).toBe(2);
  expect(result[0]?.modelVersion).toBe("jra-cb-v9-sim-2013-clean-jockey-pedigree269");
  expect(result[0]?.predictedFinishNorm).toBe(0);
  expect(result[1]?.predictedFinishNorm).toBe(1);
});

it("getFinishPositionLambdarankPredictions emits priority 2 active fallback guarded by exists clause", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/select active\.model_version, 2 as priority/u);
  expect(queryText).toMatch(
    /where exists \(\s*select 1\s*from race_finish_position_model_predictions p2\s*where p2\.model_version = active\.model_version/u,
  );
});

it("getFinishPositionLambdarankPredictions bounds priority 3 fallback to leak-free versions", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/allowed_prediction_model_versions as/u);
  expect(queryText).toMatch(/select p3\.model_version, 3 as priority/u);
  expect(queryText).toMatch(/from race_finish_position_model_predictions p3/u);
  expect(queryText).toMatch(
    /p3\.model_version in \(\s*select model_version from allowed_prediction_model_versions\s*\)/u,
  );
  expect(queryText).toMatch(/from finish_position_active_models stale/u);
  expect(queryText).toMatch(/stale\.subclass is not null/u);
  expect(queryText).toMatch(/stale\.model_version = p3\.model_version/u);
  expect(queryText).toMatch(/'jra-cb-v9-sim-2013-clean'/u);
  expect(queryText).toMatch(/'iter12-nar-xgb-hpo-v8-clean188'/u);
  expect(queryText).toMatch(/'banei-cb-v9-sim-2011'/u);
  expect(queryText).toMatch(/'banei-cb-v8-window2011-wf-15y'/u);
  expect(queryText).toMatch(/'jra-cb-stage1-marketfree235-2013'/u);
  expect(queryText).toMatch(/'iter12-nar-xgb-hpo-v8-stage1-marketfree-184'/u);
  expect(queryText).toMatch(/group by p3\.model_version/u);
  expect(queryText).toMatch(/order by priority, recency desc nulls last/u);
});

it("getFinishPositionLambdarankPredictions excludes off-label cell-routing variants from priority 3, returning nothing when that off-label row is the only candidate", async () => {
  const unroutedJraRace: RaceDetail = {
    ...PERCLASS_703_RACE,
    keibajoCode: "05",
    kyosoJokenCode: "010",
  };
  executeMock.mockResolvedValue({ rows: [] });
  const result = await getFinishPositionLambdarankPredictions(
    unroutedJraRace,
    PERCLASS_703_RUNNERS,
  );
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(
    /p3\.model_version not in \(\s*select model_version from cell_routing_off_label_variant_model_versions\s*\)/u,
  );
  const cteBody =
    queryText.match(
      /cell_routing_off_label_variant_model_versions\(model_version\) as \(\s*values([\s\S]*?)\),\s*active as \(/u,
    )?.[1] ?? "";
  expect(cteBody).toMatch(/'jra-cb-v9-sim-2013-clean-jockey-pedigree269'/u);
  expect(cteBody).toMatch(/'jra-cb-v10-prior-corner274-2013'/u);
  expect(cteBody).toMatch(/'banei-cb-v8-window2011-wf-15y'/u);
  expect(cteBody).not.toMatch(/'jra-cb-v9-sim-2013-clean'\)/u);
  expect(cteBody).not.toMatch(/'banei-cb-v9-sim-2011'/u);
  expect(result.length).toBe(0);
});

it("getFinishPositionLambdarankPredictions still lets priority 3 select the plain JRA default champion (not excluded, unlike a routed variant)", async () => {
  const unroutedJraRace: RaceDetail = {
    ...PERCLASS_703_RACE,
    keibajoCode: "05",
    kyosoJokenCode: "010",
  };
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "jra-cb-v9-sim-2013-clean",
        predicted_rank: 1,
        predicted_score: "0.8",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "jra-cb-v9-sim-2013-clean",
        predicted_rank: 2,
        predicted_score: "0.3",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    unroutedJraRace,
    PERCLASS_703_RUNNERS,
  );
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  const cteBody =
    queryText.match(
      /cell_routing_off_label_variant_model_versions\(model_version\) as \(\s*values([\s\S]*?)\),\s*active as \(/u,
    )?.[1] ?? "";
  expect(cteBody).not.toMatch(/'jra-cb-v9-sim-2013-clean'\)/u);
  expect(result.length).toBe(2);
  expect(result[0]?.modelVersion).toBe("jra-cb-v9-sim-2013-clean");
  expect(result[1]?.modelVersion).toBe("jra-cb-v9-sim-2013-clean");
});

it("getFinishPositionLambdarankPredictions applies leak-free guard to final selected rows", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(
    /where p\.source = 'jra'\s+and p\.model_version in \(\s*select model_version from allowed_prediction_model_versions\s*\)/u,
  );
});

it("getFinishPositionLambdarankPredictions references prediction_generated_at on race_finish_position_model_predictions", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/max\(p\.prediction_generated_at\) as recency/u);
  expect(queryText).toMatch(/max\(p3\.prediction_generated_at\) as recency/u);
  expect(queryText).not.toMatch(/p\.predicted_at/u);
  expect(queryText).not.toMatch(/p3\.predicted_at/u);
});

it("getFinishPositionLambdarankPredictions returns predictions from priority 2 fallback model_version", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter30-nar-cb-ensemble-A-v8",
        predicted_rank: 1,
        predicted_score: "0.88",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter30-nar-cb-ensemble-A-v8",
        predicted_rank: 2,
        predicted_score: "0.42",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result.length).toBe(2);
  expect(result[0]?.modelVersion).toBe("iter30-nar-cb-ensemble-A-v8");
  expect(result[0]?.predictedFinishNorm).toBe(0);
  expect(result[1]?.predictedFinishNorm).toBe(1);
});

it("getFinishPositionLambdarankPredictions translates execute rows into prediction features", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "0.91",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: "0.55",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result.length).toBe(2);
  expect(result[0]?.horseNumber).toBe("1");
  expect(result[0]?.modelVersion).toBe("iter23-jra-cb-ensemble-703-v8");
  expect(result[0]?.predictedFinishNorm).toBe(0);
  expect(result[0]?.showProbability).toBe(null);
  expect(result[0]?.winProbability).toBe(null);
  expect(result[1]?.horseNumber).toBe("2");
  expect(result[1]?.predictedFinishNorm).toBe(1);
});

it("getFinishPositionLambdarankPredictions computes a low confidenceTier from a tight within-race predicted_score spread", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "1.0",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: "1.2",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.confidenceTier).toBe("low");
  expect(result[1]?.confidenceTier).toBe("low");
});

it("getFinishPositionLambdarankPredictions computes a mid confidenceTier from a moderate within-race predicted_score spread", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "0.0",
        shusso_tosu: 3,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: "1.4",
        shusso_tosu: 3,
        umaban: 2,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 3,
        predicted_score: "2.8",
        shusso_tosu: 3,
        umaban: 3,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.confidenceTier).toBe("mid");
  expect(result[1]?.confidenceTier).toBe("mid");
  expect(result[2]?.confidenceTier).toBe("mid");
});

it("getFinishPositionLambdarankPredictions computes a high confidenceTier from a wide within-race predicted_score spread", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "1.0",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: "5.0",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.confidenceTier).toBe("high");
  expect(result[1]?.confidenceTier).toBe("high");
});

it("getFinishPositionLambdarankPredictions exposes the raw predictedScoreStddev for a tight within-race spread", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "1.0",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: "1.05",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.predictedScoreStddev).toBe(0.03535533905932741);
  expect(result[1]?.predictedScoreStddev).toBe(0.03535533905932741);
});

it("getFinishPositionLambdarankPredictions exposes the raw predictedScoreStddev for a wider within-race spread that still resolves to a low confidenceTier", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "1.0",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: "2.0",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.confidenceTier).toBe("low");
  expect(result[0]?.predictedScoreStddev).toBe(Math.SQRT1_2);
  expect(result[1]?.predictedScoreStddev).toBe(Math.SQRT1_2);
});

it("getFinishPositionLambdarankPredictions returns a null predictedScoreStddev when stddev is not computable", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "1.0",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: null,
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.predictedScoreStddev).toBe(null);
  expect(result[1]?.predictedScoreStddev).toBe(null);
});

it("getFinishPositionLambdarankPredictions returns a null confidenceTier when fewer than 2 rows have a valid predicted_score", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "1.0",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: null,
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.confidenceTier).toBe(null);
  expect(result[1]?.confidenceTier).toBe(null);
});

it("getFinishPositionLambdarankPredictions returns a null confidenceTier when every predicted_score is null", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: null,
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: null,
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.confidenceTier).toBe(null);
  expect(result[1]?.confidenceTier).toBe(null);
});

it("getFinishPositionLambdarankPredictions treats a non-numeric predicted_score as invalid when counting valid scores for confidenceTier", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 1,
        predicted_score: "1.0",
        shusso_tosu: 2,
        umaban: 1,
      },
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
        predicted_rank: 2,
        predicted_score: "not-a-number",
        shusso_tosu: 2,
        umaban: 2,
      },
    ],
  });
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result[0]?.confidenceTier).toBe(null);
  expect(result[1]?.confidenceTier).toBe(null);
});

it("getFinishPositionLambdarankPredictions returns empty array when execute throws", async () => {
  executeMock.mockRejectedValue(new Error("db down"));
  const result = await getFinishPositionLambdarankPredictions(
    PERCLASS_703_RACE,
    PERCLASS_703_RUNNERS,
  );
  expect(result.length).toBe(0);
});

it("getFinishPositionLambdarankPredictions short-circuits without SQL when only one runner is present", async () => {
  const singleRunner: Runner[] = [
    {
      bamei: "Solo",
      banushimei: null,
      barei: "4",
      bataiju: "480",
      chokyoshimeiRyakusho: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      damSireName: null,
      futanJuryo: "560",
      kakuteiChakujun: null,
      kettoTorokuBango: "2020100003",
      kishumeiRyakusho: null,
      kohan3f: null,
      moshokuCode: null,
      seibetsuCode: "1",
      sireName: null,
      sireSireName: null,
      sohaTime: null,
      tanshoNinkijun: null,
      tanshoOdds: null,
      timeSa: null,
      umaban: "1",
      wakuban: "1",
      zogenFugo: null,
      zogenSa: null,
    },
  ];
  const result = await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, singleRunner);
  expect(result.length).toBe(0);
  expect(executeMock).not.toHaveBeenCalled();
});

it("getFinishPositionLambdarankPredictions uses source='overseas' and category='overseas' for an overseas venue", async () => {
  const overseasRace: RaceDetail = {
    ...PERCLASS_703_RACE,
    keibajoCode: "A6",
    source: "jra",
  };
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(overseasRace, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  // The active-model CTE must look up category='overseas', not 'jra'.
  expect(queryText).toMatch(/where category = 'overseas'/u);
  // Every source predicate must use 'overseas', not 'jra'.
  expect(queryText).toMatch(/p0\.source = 'overseas'/u);
  expect(queryText).toMatch(/p\.source = 'overseas'/u);
  expect(queryText).toMatch(/p2\.source = 'overseas'/u);
  expect(queryText).toMatch(/p3\.source = 'overseas'/u);
  // Priority 0 must be gated to false (no cell-routing for overseas).
  expect(queryText).toMatch(/where false\s+and p0\.source = 'overseas'/u);
});

it("race-runners-nar-includes-sire-name", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        bamei: "テスト馬",
        banushimei: null,
        barei: "4",
        bataiju: "480",
        chokyoshimeiRyakusho: null,
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
        damSireName: "母父馬",
        futanJuryo: "560",
        kakuteiChakujun: null,
        kettoTorokuBango: "2020100001",
        kishumeiRyakusho: null,
        kohan3f: null,
        moshokuCode: null,
        seibetsuCode: "1",
        sireName: "父馬",
        sireSireName: "父父馬",
        sohaTime: null,
        tanshoNinkijun: null,
        tanshoOdds: null,
        timeSa: null,
        umaban: "1",
        wakuban: "1",
        zogenFugo: null,
        zogenSa: null,
      },
    ],
  });
  const runners = await getRaceRunners("nar", "2026", "06", "05", "44", "01");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/primary_um\.ketto_joho_01b/u);
  expect(queryText).toMatch(/secondary_um\.ketto_joho_01b/u);
  expect(queryText).toMatch(/primary_um\s*\n\s*on primary_um\.ketto_toroku_bango/u);
  expect(queryText).toMatch(/secondary_um\s*\n\s*on secondary_um\.ketto_toroku_bango/u);
  expect(runners[0]?.sireName).toBe("父馬");
});

it("race-runners-nar-falls-back-to-nvd-um-when-nvd-nu-missing", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        bamei: "テスト馬",
        banushimei: null,
        barei: "4",
        bataiju: "480",
        chokyoshimeiRyakusho: null,
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
        damSireName: "母父馬",
        futanJuryo: "560",
        kakuteiChakujun: null,
        kettoTorokuBango: "2020100002",
        kishumeiRyakusho: null,
        kohan3f: null,
        moshokuCode: null,
        seibetsuCode: "1",
        sireName: "フォールバック父",
        sireSireName: null,
        sohaTime: null,
        tanshoNinkijun: null,
        tanshoOdds: null,
        timeSa: null,
        umaban: "1",
        wakuban: "1",
        zogenFugo: null,
        zogenSa: null,
      },
    ],
  });
  const runners = await getRaceRunners("nar", "2026", "06", "05", "44", "01");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/coalesce\(\s*nullif\(regexp_replace\(primary_um\.ketto_joho_01b/u);
  expect(queryText).toMatch(/nullif\(regexp_replace\(secondary_um\.ketto_joho_01b/u);
  expect(runners[0]?.sireName).toBe("フォールバック父");
});

it("race-runners-jra-includes-sire-name", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        bamei: "JRA馬",
        banushimei: null,
        barei: "4",
        bataiju: "480",
        chokyoshimeiRyakusho: null,
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
        damSireName: "JRA母父",
        futanJuryo: "560",
        kakuteiChakujun: null,
        kettoTorokuBango: "2020100003",
        kishumeiRyakusho: null,
        kohan3f: null,
        moshokuCode: null,
        seibetsuCode: "1",
        sireName: "JRA父馬",
        sireSireName: "JRA父父馬",
        sohaTime: null,
        tanshoNinkijun: null,
        tanshoOdds: null,
        timeSa: null,
        umaban: "1",
        wakuban: "1",
        zogenFugo: null,
        zogenSa: null,
      },
    ],
  });
  const runners = await getRaceRunners("jra", "2026", "06", "05", "05", "11");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/um\.ketto_joho_01b/u);
  expect(queryText).toMatch(/um\.ketto_joho_03b/u);
  expect(queryText).toMatch(/um\.ketto_joho_05b/u);
  expect(queryText).toMatch(/left join\s+um\s*\n\s*on um\.ketto_toroku_bango/u);
  expect(runners[0]?.sireName).toBe("JRA父馬");
  expect(runners[0]?.damSireName).toBe("JRA母父");
});

it("race-runners-trims-whitespace-from-sire-name", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        bamei: "全角空白馬",
        banushimei: null,
        barei: "4",
        bataiju: "480",
        chokyoshimeiRyakusho: null,
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
        damSireName: null,
        futanJuryo: "560",
        kakuteiChakujun: null,
        kettoTorokuBango: "2020100004",
        kishumeiRyakusho: null,
        kohan3f: null,
        moshokuCode: null,
        seibetsuCode: "1",
        sireName: "正規化父",
        sireSireName: null,
        sohaTime: null,
        tanshoNinkijun: null,
        tanshoOdds: null,
        timeSa: null,
        umaban: "1",
        wakuban: "1",
        zogenFugo: null,
        zogenSa: null,
      },
    ],
  });
  const runners = await getRaceRunners("jra", "2026", "06", "05", "05", "11");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(
    /regexp_replace\(um\.ketto_joho_01b, '\^\[\[:space:\]　\]\+\|\[\[:space:\]　\]\+\$', '', 'g'\)/u,
  );
  expect(runners[0]?.sireName).toBe("正規化父");
});

it("race-runners-null-when-both-bloodline-tables-empty", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        bamei: "血統不明馬",
        banushimei: null,
        barei: "4",
        bataiju: "480",
        chokyoshimeiRyakusho: null,
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
        damSireName: null,
        futanJuryo: "560",
        kakuteiChakujun: null,
        kettoTorokuBango: "2020100005",
        kishumeiRyakusho: null,
        kohan3f: null,
        moshokuCode: null,
        seibetsuCode: "1",
        sireName: null,
        sireSireName: null,
        sohaTime: null,
        tanshoNinkijun: null,
        tanshoOdds: null,
        timeSa: null,
        umaban: "1",
        wakuban: "1",
        zogenFugo: null,
        zogenSa: null,
      },
    ],
  });
  const runners = await getRaceRunners("nar", "2026", "06", "05", "44", "01");
  expect(runners[0]?.sireName).toBe(null);
  expect(runners[0]?.sireSireName).toBe(null);
  expect(runners[0]?.damSireName).toBe(null);
});

// Regression guards for the 2026-06-28 NAR bataiju mismatch incident.
// A prior `left join lateral (... latest_weight)` (introduced commit cacf868c)
// silently substituted the horse's most-recent past-race bataiju for today's
// blank value. The fix (commit e679cb55) removed the lateral so today's null
// passes through and the viewer renders "-". Do NOT add any past-race
// fallback (lateral subquery or coalesce to a different race row) for ANY
// today-value column in this query — see the incident note in queries.ts.
it("race-runners-nar-sql-has-no-past-race-lateral-join", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getRaceRunners("nar", "2026", "06", "28", "44", "01");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(/left join lateral/iu.test(queryText)).toBe(false);
});

it("race-runners-nar-sql-does-not-coalesce-bataiju-to-past-row", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getRaceRunners("nar", "2026", "06", "28", "44", "01");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(/coalesce\([^)]*se\.bataiju/iu.test(queryText)).toBe(false);
});

it("race-runners-nar-sql-references-se-bataiju-directly-without-fallback", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getRaceRunners("nar", "2026", "06", "28", "44", "01");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(/se\.bataiju,/u.test(queryText)).toBe(true);
});

it("race-runners-nar-passes-null-bataiju-through-when-today-row-blank", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        bamei: "未計量馬",
        banushimei: null,
        barei: "4",
        bataiju: null,
        chokyoshimeiRyakusho: null,
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
        damSireName: null,
        futanJuryo: "560",
        kakuteiChakujun: null,
        kettoTorokuBango: "2020100099",
        kishumeiRyakusho: null,
        kohan3f: null,
        moshokuCode: null,
        seibetsuCode: "1",
        sireName: null,
        sireSireName: null,
        sohaTime: null,
        tanshoNinkijun: null,
        tanshoOdds: null,
        timeSa: null,
        umaban: "1",
        wakuban: "1",
        zogenFugo: null,
        zogenSa: null,
      },
    ],
  });
  const runners = await getRaceRunners("nar", "2026", "06", "28", "44", "01");
  expect(runners[0]?.bataiju).toBe(null);
});

it("race-runners-jra-sql-has-no-past-race-lateral-join", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getRaceRunners("jra", "2026", "06", "28", "05", "11");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(/left join lateral/iu.test(queryText)).toBe(false);
});

it("race-runners-jra-sql-does-not-coalesce-bataiju-to-past-row", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getRaceRunners("jra", "2026", "06", "28", "05", "11");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(/coalesce\([^)]*se\.bataiju/iu.test(queryText)).toBe(false);
});

it("get-race-trainings-non-jra-source-returns-empty-without-querying-db", async () => {
  const trainings = await getRaceTrainings("nar", "2026", "07", "18", "44", "04");
  expect(trainings).toStrictEqual([]);
  expect(executeMock.mock.calls.length).toBe(0);
});

it("get-race-trainings-sql-left-joins-runners-with-no-workout-rows", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getRaceTrainings("jra", "2026", "07", "18", "02", "04");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  // jvd_hc/jvd_wc only cover Miho/Ritto training centers, so most entrants at a
  // Hokkaido summer-circuit meet (e.g. Hakodate) have zero matching rows. Without
  // this fallback branch the INNER JOINs above silently drop those runners instead
  // of showing one placeholder row per entrant.
  expect(/no_workout_runners as \(/u.test(queryText)).toBe(true);
  expect(/from all_workouts/u.test(queryText)).toBe(true);
});

it("get-race-trainings-partitions workout rows by runner number for real and placeholder IDs", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getRaceTrainings("jra", "2026", "07", "18", "02", "04");
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/partition by umaban, "trainingType"/u);
  expect(queryText).not.toMatch(/partition by ketto_toroku_bango, "trainingType"/u);
});

it("get-race-trainings-returns-one-row-per-entrant-including-those-without-official-workouts", async () => {
  executeMock.mockResolvedValue({
    rows: [
      { chokyoNengappi: "20260715", trainingType: "ウッド", umaban: "16" },
      { chokyoNengappi: "", trainingType: "-", umaban: "01" },
      { chokyoNengappi: "", trainingType: "-", umaban: "02" },
    ],
  });
  const trainings = await getRaceTrainings("jra", "2026", "07", "18", "02", "04");
  expect(trainings).toStrictEqual([
    { chokyoNengappi: "20260715", trainingType: "ウッド", umaban: "16" },
    { chokyoNengappi: "", trainingType: "-", umaban: "01" },
    { chokyoNengappi: "", trainingType: "-", umaban: "02" },
  ]);
});
