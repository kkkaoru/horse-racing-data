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

import type { RunningStyleBucketFilter } from "../lib/running-style-prediction-dimensions";
import { getRunningStyleBucketEvaluation } from "./queries";

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
