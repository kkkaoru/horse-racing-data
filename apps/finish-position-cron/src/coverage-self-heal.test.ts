// Run with bun. Tests for the per-race coverage self-healing cron.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env, PredictCategory } from "./types";

interface ClaimResult {
  proceed: boolean;
  state?: string;
}

interface RaceSourceRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  race_start_at_jst: string;
}

interface CompletionCallParams {
  category: string;
  env: unknown;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
}

const {
  claimFocusedFullRaceMock,
  enqueuePredictMock,
  isFocusedFullPredictionCompleteMock,
  isOldDateRunYmdMock,
} = vi.hoisted(() => {
  const claimFocusedFullRace = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const enqueuePredict = vi.fn(async (): Promise<PredictCategory[]> => ["jra"]);
  const isFocusedFullPredictionComplete = vi.fn(
    async (_params: CompletionCallParams): Promise<boolean> => false,
  );
  const isOldDateRunYmd = vi.fn((): boolean => false);
  return {
    claimFocusedFullRaceMock: claimFocusedFullRace,
    enqueuePredictMock: enqueuePredict,
    isFocusedFullPredictionCompleteMock: isFocusedFullPredictionComplete,
    isOldDateRunYmdMock: isOldDateRunYmd,
  };
});

vi.mock("./do-state", () => ({ claimFocusedFullRace: claimFocusedFullRaceMock }));
vi.mock("./queue-producer", () => ({ enqueuePredict: enqueuePredictMock }));
vi.mock("./focused-full-completion", () => ({
  isFocusedFullPredictionComplete: isFocusedFullPredictionCompleteMock,
}));
vi.mock("./old-date-guard", () => ({ isOldDateRunYmd: isOldDateRunYmdMock }));

import {
  COVERAGE_SELF_HEAL_CRON,
  runCoverageSelfHeal,
  shouldRunCoverageSelfHealCron,
} from "./coverage-self-heal";

const realtimeAllMock = vi.fn(async () => ({ results: [] as RaceSourceRow[] }));
const realtimeBindMock = vi.fn(() => ({ all: realtimeAllMock }));
const realtimePrepareMock = vi.fn(() => ({ bind: realtimeBindMock }));

const cronFirstMock = vi.fn(async (): Promise<{ count: number } | null> => null);
const cronRunMock = vi.fn(async () => ({ success: true }));
const cronBindMock = vi.fn(() => ({ first: cronFirstMock, run: cronRunMock }));
const cronPrepareMock = vi.fn(() => ({ bind: cronBindMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: { prepare: cronPrepareMock } as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: { prepare: realtimePrepareMock } as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

const NOW = new Date("2026-07-12T06:00:00.000Z");

beforeEach(() => {
  claimFocusedFullRaceMock.mockClear();
  enqueuePredictMock.mockClear();
  isFocusedFullPredictionCompleteMock.mockClear();
  isOldDateRunYmdMock.mockClear();
  realtimeAllMock.mockClear();
  realtimeBindMock.mockClear();
  realtimePrepareMock.mockClear();
  cronFirstMock.mockClear();
  cronRunMock.mockClear();
  cronBindMock.mockClear();
  cronPrepareMock.mockClear();
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  enqueuePredictMock.mockResolvedValue(["jra"]);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  isOldDateRunYmdMock.mockReturnValue(false);
  realtimeAllMock.mockResolvedValue({ results: [] });
  cronFirstMock.mockResolvedValue(null);
});

test("COVERAGE_SELF_HEAL_CRON is the every-15-min race-hours schedule offset by 7 minutes", () => {
  expect(COVERAGE_SELF_HEAL_CRON).toBe("7,22,37,52 1-11 * * *");
});

test("shouldRunCoverageSelfHealCron matches the configured cron", () => {
  expect(shouldRunCoverageSelfHealCron("7,22,37,52 1-11 * * *")).toBe(true);
});

test("shouldRunCoverageSelfHealCron rejects a different cron", () => {
  expect(shouldRunCoverageSelfHealCron("*/10 1-11 * * *")).toBe(false);
});

test("shouldRunCoverageSelfHealCron rejects an empty string", () => {
  expect(shouldRunCoverageSelfHealCron("")).toBe(false);
});

test("runCoverageSelfHeal skips the scan and warns when runYmd is unexpectedly old", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(summary).toStrictEqual({
    alreadyComplete: 0,
    alreadyInFlight: 0,
    candidates: 0,
    enqueued: 0,
    errors: 0,
    escalated: 0,
    scanned: 0,
  });
  expect(realtimePrepareMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] runYmd=20260712 unexpectedly old -- skipping scan",
  );
  warnSpy.mockRestore();
});

test("runCoverageSelfHeal queries realtime_race_sources for the JST calendar day and returns an empty summary with no races", async () => {
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(realtimeBindMock).toHaveBeenCalledWith("2026", "0712");
  expect(isFocusedFullPredictionCompleteMock).not.toHaveBeenCalled();
  expect(summary).toStrictEqual({
    alreadyComplete: 0,
    alreadyInFlight: 0,
    candidates: 0,
    enqueued: 0,
    errors: 0,
    escalated: 0,
    scanned: 0,
  });
});

test("runCoverageSelfHeal excludes races still inside the grace window, in the future, and with an unparseable post time", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_bango: "01",
        race_start_at_jst: "2026-07-12T14:46:00+09:00",
        source: "jra",
      },
      {
        keibajo_code: "05",
        race_bango: "02",
        race_start_at_jst: "2026-07-12T20:00:00+09:00",
        source: "jra",
      },
      { keibajo_code: "05", race_bango: "03", race_start_at_jst: "not-a-date", source: "jra" },
    ],
  });
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(isFocusedFullPredictionCompleteMock).not.toHaveBeenCalled();
  expect(summary.scanned).toBe(3);
  expect(summary.candidates).toBe(0);
});

test("runCoverageSelfHeal resolves category from source/keibajo_code and zero-pads codes, including the exact grace-window boundary", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "5",
        race_bango: "1",
        race_start_at_jst: "2026-07-12T10:00:00+09:00",
        source: "jra",
      },
      {
        keibajo_code: "83",
        race_bango: "3",
        race_start_at_jst: "2026-07-12T14:45:00+09:00",
        source: "nar",
      },
      {
        keibajo_code: "44",
        race_bango: "7",
        race_start_at_jst: "2026-07-12T12:00:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledTimes(3);
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "05",
    raceBango: "01",
    runYmd: "20260712",
  });
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith({
    category: "ban-ei",
    env: expect.anything(),
    keibajoCode: "83",
    raceBango: "03",
    runYmd: "20260712",
  });
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith({
    category: "nar",
    env: expect.anything(),
    keibajoCode: "44",
    raceBango: "07",
    runYmd: "20260712",
  });
  expect(summary).toStrictEqual({
    alreadyComplete: 3,
    alreadyInFlight: 0,
    candidates: 3,
    enqueued: 0,
    errors: 0,
    escalated: 0,
    scanned: 3,
  });
  expect(logSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] tick runYmd=20260712 scanned=3 candidates=3 enqueued=0 escalated=0 alreadyInFlight=0 alreadyComplete=3 errors=0",
  );
  logSpy.mockRestore();
});

test("runCoverageSelfHeal enqueues a fresh skipDedup focused-full message and records the event when there is no existing claim", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_bango: "11",
        race_start_at_jst: "2026-07-12T10:00:00+09:00",
        source: "jra",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue(null);
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260712",
    staleAfterMs: 900000,
  });
  expect(enqueuePredictMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    env: expect.anything(),
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-07-12",
    runYmd: "20260712",
    skipDedup: true,
  });
  expect(cronBindMock).toHaveBeenNthCalledWith(1, "20260712", "jra", "05", "11");
  expect(cronBindMock).toHaveBeenNthCalledWith(
    2,
    "20260712",
    "jra",
    "05",
    "11",
    "2026-07-12T10:00:00+09:00",
    0,
    1,
    0,
  );
  expect(cronRunMock).toHaveBeenCalledTimes(1);
  expect(warnSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] enqueued gap-fill category=jra runYmd=20260712 keibajo=05 race=11 priorEnqueueCount=0",
  );
  expect(summary).toStrictEqual({
    alreadyComplete: 0,
    alreadyInFlight: 0,
    candidates: 1,
    enqueued: 1,
    errors: 0,
    escalated: 0,
    scanned: 1,
  });
  warnSpy.mockRestore();
});

test("runCoverageSelfHeal passes the D1 prior-enqueue count through to the re-enqueue and its event row", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_bango: "11",
        race_start_at_jst: "2026-07-12T10:00:00+09:00",
        source: "jra",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue({ count: 1 });
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).toHaveBeenCalledTimes(1);
  expect(cronBindMock).toHaveBeenNthCalledWith(
    2,
    "20260712",
    "jra",
    "05",
    "11",
    "2026-07-12T10:00:00+09:00",
    1,
    1,
    0,
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] enqueued gap-fill category=jra runYmd=20260712 keibajo=05 race=11 priorEnqueueCount=1",
  );
  warnSpy.mockRestore();
});

test("runCoverageSelfHeal skips enqueueing and reports in-flight when the DO claim is still fresh", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_bango: "11",
        race_start_at_jst: "2026-07-12T10:00:00+09:00",
        source: "jra",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue({ count: 0 });
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: false, state: "started" });
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).not.toHaveBeenCalled();
  expect(cronRunMock).not.toHaveBeenCalled();
  expect(summary).toStrictEqual({
    alreadyComplete: 0,
    alreadyInFlight: 1,
    candidates: 1,
    enqueued: 0,
    errors: 0,
    escalated: 0,
    scanned: 1,
  });
});

test("runCoverageSelfHeal escalates instead of re-enqueueing once the per-race cap is reached", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_bango: "11",
        race_start_at_jst: "2026-07-12T10:00:00+09:00",
        source: "jra",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue({ count: 2 });
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(enqueuePredictMock).not.toHaveBeenCalled();
  expect(cronBindMock).toHaveBeenNthCalledWith(
    2,
    "20260712",
    "jra",
    "05",
    "11",
    "2026-07-12T10:00:00+09:00",
    2,
    0,
    1,
  );
  expect(cronRunMock).toHaveBeenCalledTimes(1);
  expect(errorSpy).toHaveBeenCalledWith(
    "SELF_HEAL_ESCALATE category=jra runYmd=20260712 keibajo=05 race=11 priorEnqueueCount=2",
  );
  expect(summary).toStrictEqual({
    alreadyComplete: 0,
    alreadyInFlight: 0,
    candidates: 1,
    enqueued: 0,
    errors: 0,
    escalated: 1,
    scanned: 1,
  });
  errorSpy.mockRestore();
});

test("runCoverageSelfHeal logs and isolates a per-race failure without throwing or blocking other candidates", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_bango: "11",
        race_start_at_jst: "2026-07-12T10:00:00+09:00",
        source: "jra",
      },
      {
        keibajo_code: "44",
        race_bango: "07",
        race_start_at_jst: "2026-07-12T11:00:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockImplementation(
    async ({ keibajoCode }: CompletionCallParams) => {
      if (keibajoCode === "05") throw new Error("boom");
      return true;
    },
  );
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(errorSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] failed to heal category=jra runYmd=20260712 keibajo=05 race=11:",
    "Error: boom",
  );
  expect(summary).toStrictEqual({
    alreadyComplete: 1,
    alreadyInFlight: 0,
    candidates: 2,
    enqueued: 0,
    errors: 1,
    escalated: 0,
    scanned: 2,
  });
  errorSpy.mockRestore();
});
