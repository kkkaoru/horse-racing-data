// Run with bun (bunx vitest)

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
  getRaceRunners,
  getRunningStyleBucketEvaluation,
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

it("getRunningStyleBucketEvaluation aggregates the 8 per-class log loss columns plus top2 hits", async () => {
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

it("getFinishPositionLambdarankPredictions emits subclass-aware active CTE referencing kyosoJokenCode", async () => {
  executeMock.mockResolvedValue({
    rows: [
      {
        model_version: "iter23-jra-cb-ensemble-703-v8",
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
  expect(queryText).toMatch(/and \(subclass = /u);
  expect(queryText).toMatch(/'703'/u);
  expect(queryText).toMatch(/or subclass is null\)/u);
  expect(queryText).toMatch(/order by \(subclass is null\) asc/u);
});

it("getFinishPositionLambdarankPredictions emits priority 1 active fallback guarded by exists clause", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/select active\.model_version, 1 as priority/u);
  expect(queryText).toMatch(
    /where exists \(\s*select 1\s*from race_finish_position_model_predictions p2\s*where p2\.model_version = active\.model_version/u,
  );
});

it("getFinishPositionLambdarankPredictions emits priority 2 fallback over any race prediction", async () => {
  executeMock.mockResolvedValue({ rows: [] });
  await getFinishPositionLambdarankPredictions(PERCLASS_703_RACE, PERCLASS_703_RUNNERS);
  const queryArg = executeMock.mock.calls[0]?.[0];
  const queryText = stringifyQuery(queryArg);
  expect(queryText).toMatch(/select p3\.model_version, 2 as priority/u);
  expect(queryText).toMatch(/from race_finish_position_model_predictions p3/u);
  expect(queryText).toMatch(/group by p3\.model_version/u);
  expect(queryText).toMatch(/order by priority, recency desc nulls last/u);
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
