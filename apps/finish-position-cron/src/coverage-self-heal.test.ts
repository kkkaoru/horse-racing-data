// Run with bun. Tests for the per-race coverage self-healing + pre-race readiness cron.

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

interface EnqueuePredictCallParams {
  category: string;
  daysAhead: number;
  env: unknown;
  keibajoCode: string;
  mode: string;
  raceBango: string;
  runDate: string;
  runYmd: string;
  skipDedup: boolean;
}

const {
  claimFocusedFullRaceMock,
  enqueuePredictMock,
  isFocusedFullPredictionCompleteMock,
  isOldDateRunYmdMock,
} = vi.hoisted(() => {
  const claimFocusedFullRace = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const enqueuePredict = vi.fn(
    async (_params: EnqueuePredictCallParams): Promise<PredictCategory[]> => ["jra"],
  );
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
  MAX_PRE_RACE_ENQUEUES_PER_RACE,
  PRE_RACE_ENQUEUE_CAP_PER_TICK,
  PRE_RACE_ESCALATED_RETRY_MINUTES,
  PRE_RACE_LEAD_MINUTES,
  buildPostRaceGapCandidates,
  buildPreRaceGapCandidates,
  isEscalatedRetryDue,
  isPastGraceWindow,
  isWithinPreRaceLeadWindow,
  runCoverageSelfHeal,
  shouldRunCoverageSelfHealCron,
} from "./coverage-self-heal";

const realtimeAllMock = vi.fn(async () => ({ results: [] as RaceSourceRow[] }));
const realtimeBindMock = vi.fn(() => ({ all: realtimeAllMock }));
const realtimePrepareMock = vi.fn(() => ({ bind: realtimeBindMock }));

const cronFirstMock = vi.fn(async (): Promise<{ count: number } | null> => null);
const lastEnqueuedAtMock = vi.fn(
  async (): Promise<{ last_enqueued_at: string | null } | null> => null,
);
const cronRunMock = vi.fn(async () => ({ success: true }));
const cronBindMock = vi.fn((..._args: unknown[]) => ({ first: cronFirstMock, run: cronRunMock }));
const cronPrepareMock = vi.fn((sql: string) => ({
  bind: (...args: unknown[]) => {
    cronBindMock(...args);
    return {
      first: sql.includes("max(recorded_at)") ? lastEnqueuedAtMock : cronFirstMock,
      run: cronRunMock,
    };
  },
}));

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

// 2026-07-12 15:00 JST
const NOW = new Date("2026-07-12T06:00:00.000Z");

const EMPTY_SUMMARY = {
  alreadyComplete: 0,
  alreadyInFlight: 0,
  capped: 0,
  candidates: 0,
  enqueued: 0,
  errors: 0,
  escalated: 0,
  postRaceCandidates: 0,
  preRaceCandidates: 0,
  preRaceEnqueued: 0,
  scanned: 0,
};

beforeEach(() => {
  claimFocusedFullRaceMock.mockClear();
  enqueuePredictMock.mockClear();
  isFocusedFullPredictionCompleteMock.mockClear();
  isOldDateRunYmdMock.mockClear();
  realtimeAllMock.mockClear();
  realtimeBindMock.mockClear();
  realtimePrepareMock.mockClear();
  cronFirstMock.mockClear();
  lastEnqueuedAtMock.mockClear();
  cronRunMock.mockClear();
  cronBindMock.mockClear();
  cronPrepareMock.mockClear();
  lastEnqueuedAtMock.mockResolvedValue(null);
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

test("pre-race readiness constants mirror weight lead and keep separate budgets", () => {
  expect(PRE_RACE_LEAD_MINUTES).toBe(180);
  expect(PRE_RACE_ENQUEUE_CAP_PER_TICK).toBe(16);
  expect(MAX_PRE_RACE_ENQUEUES_PER_RACE).toBe(2);
  expect(PRE_RACE_ESCALATED_RETRY_MINUTES).toBe(30);
});

test("isEscalatedRetryDue is true when last enqueue is missing or unparseable", () => {
  expect(isEscalatedRetryDue(null, NOW, PRE_RACE_ESCALATED_RETRY_MINUTES)).toBe(true);
  expect(isEscalatedRetryDue(undefined, NOW, PRE_RACE_ESCALATED_RETRY_MINUTES)).toBe(true);
  expect(isEscalatedRetryDue("", NOW, PRE_RACE_ESCALATED_RETRY_MINUTES)).toBe(true);
  expect(isEscalatedRetryDue("not-a-date", NOW, PRE_RACE_ESCALATED_RETRY_MINUTES)).toBe(true);
});

test("isEscalatedRetryDue respects the backoff interval against D1 UTC timestamps", () => {
  expect(isEscalatedRetryDue("2026-07-12 05:45:00", NOW, PRE_RACE_ESCALATED_RETRY_MINUTES)).toBe(
    false,
  );
  expect(isEscalatedRetryDue("2026-07-12 05:30:00", NOW, PRE_RACE_ESCALATED_RETRY_MINUTES)).toBe(
    true,
  );
  expect(isEscalatedRetryDue("2026-07-12 05:29:59", NOW, PRE_RACE_ESCALATED_RETRY_MINUTES)).toBe(
    true,
  );
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

test("isPastGraceWindow is true only after the 15-minute grace after post", () => {
  expect(isPastGraceWindow("2026-07-12T14:45:00+09:00", NOW)).toBe(true);
  expect(isPastGraceWindow("2026-07-12T14:46:00+09:00", NOW)).toBe(false);
  expect(isPastGraceWindow("2026-07-12T15:00:00+09:00", NOW)).toBe(false);
  expect(isPastGraceWindow("not-a-date", NOW)).toBe(false);
});

test("isWithinPreRaceLeadWindow covers (now, now+180m] and rejects past/unparseable", () => {
  // 1 minute ahead
  expect(isWithinPreRaceLeadWindow("2026-07-12T15:01:00+09:00", NOW)).toBe(true);
  // exactly 180 minutes ahead (inclusive boundary)
  expect(isWithinPreRaceLeadWindow("2026-07-12T18:00:00+09:00", NOW)).toBe(true);
  // 181 minutes ahead (outside)
  expect(isWithinPreRaceLeadWindow("2026-07-12T18:01:00+09:00", NOW)).toBe(false);
  // already at/ past post
  expect(isWithinPreRaceLeadWindow("2026-07-12T15:00:00+09:00", NOW)).toBe(false);
  expect(isWithinPreRaceLeadWindow("2026-07-12T14:00:00+09:00", NOW)).toBe(false);
  expect(isWithinPreRaceLeadWindow("not-a-date", NOW)).toBe(false);
});

test("buildPreRaceGapCandidates sorts by race_start ascending and tags phase", () => {
  const rows: RaceSourceRow[] = [
    {
      keibajo_code: "44",
      race_bango: "10",
      race_start_at_jst: "2026-07-12T17:00:00+09:00",
      source: "nar",
    },
    {
      keibajo_code: "30",
      race_bango: "1",
      race_start_at_jst: "2026-07-12T15:30:00+09:00",
      source: "nar",
    },
    {
      keibajo_code: "05",
      race_bango: "11",
      race_start_at_jst: "2026-07-12T10:00:00+09:00",
      source: "jra",
    },
  ];
  const candidates = buildPreRaceGapCandidates(rows, NOW);
  expect(candidates).toStrictEqual([
    {
      category: "nar",
      keibajoCode: "30",
      phase: "pre-race",
      raceBango: "01",
      raceStartAtJst: "2026-07-12T15:30:00+09:00",
    },
    {
      category: "nar",
      keibajoCode: "44",
      phase: "pre-race",
      raceBango: "10",
      raceStartAtJst: "2026-07-12T17:00:00+09:00",
    },
  ]);
});

test("buildPostRaceGapCandidates sorts by race_start ascending and tags phase", () => {
  const rows: RaceSourceRow[] = [
    {
      keibajo_code: "44",
      race_bango: "7",
      race_start_at_jst: "2026-07-12T12:00:00+09:00",
      source: "nar",
    },
    {
      keibajo_code: "5",
      race_bango: "1",
      race_start_at_jst: "2026-07-12T10:00:00+09:00",
      source: "jra",
    },
  ];
  const candidates = buildPostRaceGapCandidates(rows, NOW);
  expect(candidates.map((c) => `${c.keibajoCode}:${c.raceBango}:${c.phase}`)).toStrictEqual([
    "05:01:post-race",
    "44:07:post-race",
  ]);
});

test("buildPreRaceGapCandidates breaks ties by keibajo then race_bango when post times match", () => {
  const sameStart = "2026-07-12T16:00:00+09:00";
  const rows: RaceSourceRow[] = [
    { keibajo_code: "44", race_bango: "08", race_start_at_jst: sameStart, source: "nar" },
    { keibajo_code: "30", race_bango: "02", race_start_at_jst: sameStart, source: "nar" },
    { keibajo_code: "44", race_bango: "06", race_start_at_jst: sameStart, source: "nar" },
    { keibajo_code: "30", race_bango: "01", race_start_at_jst: sameStart, source: "nar" },
  ];
  const candidates = buildPreRaceGapCandidates(rows, NOW);
  expect(candidates.map((c) => `${c.keibajoCode}:${c.raceBango}`)).toStrictEqual([
    "30:01",
    "30:02",
    "44:06",
    "44:08",
  ]);
});

test("runCoverageSelfHeal skips the scan and warns when runYmd is unexpectedly old", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(summary).toStrictEqual(EMPTY_SUMMARY);
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
  expect(summary).toStrictEqual({ ...EMPTY_SUMMARY, scanned: 0 });
});

test("runCoverageSelfHeal excludes races still inside the grace window, beyond the pre-race lead, and with an unparseable post time", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "05",
        race_bango: "01",
        // 14 min past post -- inside grace, not pre-race
        race_start_at_jst: "2026-07-12T14:46:00+09:00",
        source: "jra",
      },
      {
        keibajo_code: "05",
        race_bango: "02",
        // 181 min ahead -- beyond pre-race lead
        race_start_at_jst: "2026-07-12T18:01:00+09:00",
        source: "jra",
      },
      { keibajo_code: "05", race_bango: "03", race_start_at_jst: "not-a-date", source: "jra" },
    ],
  });
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(isFocusedFullPredictionCompleteMock).not.toHaveBeenCalled();
  expect(summary.scanned).toBe(3);
  expect(summary.candidates).toBe(0);
  expect(summary.preRaceCandidates).toBe(0);
  expect(summary.postRaceCandidates).toBe(0);
});

test("runCoverageSelfHeal resolves category from source/keibajo_code and zero-pads codes for post-race candidates", async () => {
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
    ...EMPTY_SUMMARY,
    alreadyComplete: 3,
    candidates: 3,
    postRaceCandidates: 3,
    scanned: 3,
  });
  expect(logSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] tick runYmd=20260712 scanned=3 candidates=3 preRaceCandidates=0 postRaceCandidates=3 enqueued=0 preRaceEnqueued=0 escalated=0 alreadyInFlight=0 alreadyComplete=3 capped=0 errors=0",
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
  // prior-count bind includes post-utc cutoff (10:00 JST = 01:00 UTC)
  expect(cronBindMock).toHaveBeenNthCalledWith(
    1,
    "20260712",
    "jra",
    "05",
    "11",
    "2026-07-12 01:00:00",
  );
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
    ...EMPTY_SUMMARY,
    candidates: 1,
    enqueued: 1,
    postRaceCandidates: 1,
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
    ...EMPTY_SUMMARY,
    alreadyInFlight: 1,
    candidates: 1,
    postRaceCandidates: 1,
    scanned: 1,
  });
});

test("runCoverageSelfHeal escalates instead of re-enqueueing once the per-race post-race cap is reached", async () => {
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
    ...EMPTY_SUMMARY,
    candidates: 1,
    escalated: 1,
    postRaceCandidates: 1,
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
    ...EMPTY_SUMMARY,
    alreadyComplete: 1,
    candidates: 2,
    errors: 1,
    postRaceCandidates: 2,
    scanned: 2,
  });
  errorSpy.mockRestore();
});

test("runCoverageSelfHeal enqueues pre-race incomplete races as mode=full skipDedup with PRE_RACE_READY telemetry", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "44",
        race_bango: "06",
        race_start_at_jst: "2026-07-12T16:00:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue(null);
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).toHaveBeenCalledWith({
    category: "nar",
    daysAhead: 2,
    env: expect.anything(),
    keibajoCode: "44",
    mode: "full",
    raceBango: "06",
    runDate: "2026-07-12",
    runYmd: "20260712",
    skipDedup: true,
  });
  // 16:00 JST = 07:00 UTC
  expect(cronBindMock).toHaveBeenNthCalledWith(
    1,
    "20260712",
    "nar",
    "44",
    "06",
    "2026-07-12 07:00:00",
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] PRE_RACE_READY enqueued gap-fill pre_race=1 category=nar runYmd=20260712 keibajo=44 race=06 priorEnqueueCount=0",
  );
  expect(summary).toStrictEqual({
    ...EMPTY_SUMMARY,
    candidates: 1,
    enqueued: 1,
    preRaceCandidates: 1,
    preRaceEnqueued: 1,
    scanned: 1,
  });
  warnSpy.mockRestore();
});

test("runCoverageSelfHeal skips complete pre-race candidates without enqueueing", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "30",
        race_bango: "01",
        race_start_at_jst: "2026-07-12T15:30:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(summary).toStrictEqual({
    ...EMPTY_SUMMARY,
    alreadyComplete: 1,
    candidates: 1,
    preRaceCandidates: 1,
    scanned: 1,
  });
});

test("runCoverageSelfHeal processes pre-race candidates earliest-post first and respects the per-tick enqueue cap", async () => {
  // 18 incomplete pre-race candidates within the lead window; only first 16 enqueue.
  // Fixed JST wall times (NOW = 15:00 JST): 15:30, 15:31, ... — all inside 180m lead.
  const results: RaceSourceRow[] = Array.from(
    { length: PRE_RACE_ENQUEUE_CAP_PER_TICK + 2 },
    (_, i) => {
      const raceNum = i + 1;
      const minute = 30 + i;
      const hh = minute >= 60 ? "16" : "15";
      const mm = String(minute % 60).padStart(2, "0");
      return {
        keibajo_code: "44",
        race_bango: String(raceNum),
        race_start_at_jst: `2026-07-12T${hh}:${mm}:00+09:00`,
        source: "nar",
      };
    },
  );
  // Reverse so selection order must come from sort, not input order.
  realtimeAllMock.mockResolvedValue({
    results: [...results].reverse(),
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue(null);
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).toHaveBeenCalledTimes(PRE_RACE_ENQUEUE_CAP_PER_TICK);
  // Earliest race_bango first among enqueued
  const enqueuedRaceBangos = enqueuePredictMock.mock.calls.map((call) => call[0].raceBango);
  expect(enqueuedRaceBangos).toStrictEqual(
    Array.from({ length: PRE_RACE_ENQUEUE_CAP_PER_TICK }, (_, i) => String(i + 1).padStart(2, "0")),
  );
  expect(summary.enqueued).toBe(PRE_RACE_ENQUEUE_CAP_PER_TICK);
  expect(summary.preRaceEnqueued).toBe(PRE_RACE_ENQUEUE_CAP_PER_TICK);
  expect(summary.capped).toBe(2);
  expect(summary.preRaceCandidates).toBe(PRE_RACE_ENQUEUE_CAP_PER_TICK + 2);
  expect(warnSpy.mock.calls.some((c) => String(c[0]).includes("PRE_RACE_READY capped"))).toBe(true);
  warnSpy.mockRestore();
});

test("runCoverageSelfHeal keeps post-race heal independent of pre-race prior counts", async () => {
  // One post-race incomplete race. Pre-race prior-count SQL is different
  // (recorded_at < post); post-race uses recorded_at >= post. Returning count=0
  // for post-race prior means pre-race history does not block post-race enqueue.
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "44",
        race_bango: "01",
        race_start_at_jst: "2026-07-12T10:00:00+09:00",
        source: "nar",
      },
      {
        keibajo_code: "44",
        race_bango: "08",
        race_start_at_jst: "2026-07-12T16:30:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue({ count: 0 });
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).toHaveBeenCalledTimes(2);
  const modes = enqueuePredictMock.mock.calls.map((call) => {
    const body = call[0];
    return `${body.keibajoCode}:${body.raceBango}:mode=${body.mode}:skipDedup=${body.skipDedup}`;
  });
  expect(modes).toContain("44:01:mode=full:skipDedup=true");
  expect(modes).toContain("44:08:mode=full:skipDedup=true");
  // Post-race count SQL uses >= cutoff; pre-race uses < cutoff
  const countBinds = cronBindMock.mock.calls.filter((c) => c.length === 5);
  expect(countBinds).toEqual(
    expect.arrayContaining([
      ["20260712", "nar", "44", "01", "2026-07-12 01:00:00"],
      ["20260712", "nar", "44", "08", "2026-07-12 07:30:00"],
    ]),
  );
  expect(summary.enqueued).toBe(2);
  expect(summary.preRaceEnqueued).toBe(1);
  expect(summary.postRaceCandidates).toBe(1);
  expect(summary.preRaceCandidates).toBe(1);
  expect(
    warnSpy.mock.calls.some((c) => String(c[0]).includes("PRE_RACE_READY enqueued gap-fill")),
  ).toBe(true);
  expect(
    warnSpy.mock.calls.some(
      (c) => String(c[0]).includes("enqueued gap-fill") && !String(c[0]).includes("PRE_RACE_READY"),
    ),
  ).toBe(true);
  warnSpy.mockRestore();
});

test("runCoverageSelfHeal escalates pre-race with PRE_RACE_READY_ESCALATE once pre-race budget is exhausted", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "44",
        race_bango: "10",
        race_start_at_jst: "2026-07-12T17:00:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue({ count: MAX_PRE_RACE_ENQUEUES_PER_RACE });
  lastEnqueuedAtMock.mockResolvedValue({ last_enqueued_at: "2026-07-12 05:45:00" });
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "PRE_RACE_READY_ESCALATE pre_race=1 category=nar runYmd=20260712 keibajo=44 race=10 priorEnqueueCount=2",
  );
  expect(summary).toStrictEqual({
    ...EMPTY_SUMMARY,
    candidates: 1,
    escalated: 1,
    preRaceCandidates: 1,
    scanned: 1,
  });
  errorSpy.mockRestore();
});

test("runCoverageSelfHeal keeps retrying an incomplete pre-race race after escalate once backoff elapses", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "83",
        race_bango: "06",
        race_start_at_jst: "2026-07-12T16:30:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue({ count: MAX_PRE_RACE_ENQUEUES_PER_RACE });
  lastEnqueuedAtMock.mockResolvedValue({ last_enqueued_at: "2026-07-12 05:25:00" });
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).toHaveBeenCalledTimes(1);
  expect(enqueuePredictMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "ban-ei",
      keibajoCode: "83",
      mode: "full",
      raceBango: "06",
      skipDedup: true,
    }),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] PRE_RACE_READY_ESCALATED_RETRY pre_race=1 category=ban-ei runYmd=20260712 keibajo=83 race=06 priorEnqueueCount=2",
  );
  expect(cronBindMock).toHaveBeenCalledWith(
    "20260712",
    "ban-ei",
    "83",
    "06",
    "2026-07-12T16:30:00+09:00",
    2,
    1,
    1,
  );
  expect(summary).toStrictEqual({
    ...EMPTY_SUMMARY,
    candidates: 1,
    enqueued: 1,
    preRaceCandidates: 1,
    preRaceEnqueued: 1,
    scanned: 1,
  });
  warnSpy.mockRestore();
});

test("runCoverageSelfHeal does not enqueue an escalated pre-race retry while the DO claim is in flight", async () => {
  realtimeAllMock.mockResolvedValue({
    results: [
      {
        keibajo_code: "83",
        race_bango: "07",
        race_start_at_jst: "2026-07-12T16:45:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  cronFirstMock.mockResolvedValue({ count: MAX_PRE_RACE_ENQUEUES_PER_RACE });
  lastEnqueuedAtMock.mockResolvedValue({ last_enqueued_at: "2026-07-12 05:20:00" });
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: false, state: "started" });
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).not.toHaveBeenCalled();
  expect(summary).toStrictEqual({
    ...EMPTY_SUMMARY,
    alreadyInFlight: 1,
    candidates: 1,
    preRaceCandidates: 1,
    scanned: 1,
  });
});

test("runCoverageSelfHeal does not let a pre-race failure block post-race heal", async () => {
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
        race_bango: "06",
        race_start_at_jst: "2026-07-12T16:00:00+09:00",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockImplementation(
    async ({ keibajoCode }: CompletionCallParams) => {
      if (keibajoCode === "44") throw new Error("pre-race boom");
      return false;
    },
  );
  cronFirstMock.mockResolvedValue(null);
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const summary = await runCoverageSelfHeal({ env: makeEnv(), now: NOW });
  expect(enqueuePredictMock).toHaveBeenCalledTimes(1);
  expect(enqueuePredictMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "jra",
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      skipDedup: true,
    }),
  );
  expect(errorSpy).toHaveBeenCalledWith(
    "[coverage-self-heal] PRE_RACE_READY failed to heal pre_race=1 category=nar runYmd=20260712 keibajo=44 race=06:",
    "Error: pre-race boom",
  );
  expect(summary.enqueued).toBe(1);
  expect(summary.errors).toBe(1);
  expect(summary.postRaceCandidates).toBe(1);
  expect(summary.preRaceCandidates).toBe(1);
  errorSpy.mockRestore();
  warnSpy.mockRestore();
});
