// Run with bun. Tests for the per-race finish-position rescore coordinator.

import { beforeEach, expect, test, vi } from "vitest";

const { claimRescoreRaceMock } = vi.hoisted(() => ({ claimRescoreRaceMock: vi.fn() }));

vi.mock("./do-state", () => ({ claimRescoreRace: claimRescoreRaceMock }));

import {
  DEFAULT_RESCORE_LEAD_MINUTES,
  formatRunDateJst,
  formatRunYmdJst,
  isCoordinatorEnabled,
  isWithinRescoreWindow,
  planRescoreForCategory,
  runRaceCoordinatorTick,
  selectRacesWithinWindow,
  triggerWeightRebuildIfNeeded,
} from "./race-coordinator";
import type { Env } from "./types";

interface RaceSourceRow {
  keibajo_code: string;
  race_bango: string;
  race_start_at_jst: string;
}

const sendMock = vi.fn(async () => undefined);
const bindMock = vi.fn();
const prepareMock = vi.fn(() => ({ bind: bindMock }));

const makeEnv = (overrides: Partial<Env> = {}): Env => ({
  COORDINATOR_ENABLED: "1",
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: { prepare: prepareMock } as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
  ...overrides,
});

const stubD1Rows = (rows: RaceSourceRow[]): void => {
  bindMock.mockReturnValue({ all: vi.fn(async () => ({ results: rows })) });
};

beforeEach(() => {
  claimRescoreRaceMock.mockClear();
  sendMock.mockClear();
  bindMock.mockClear();
  prepareMock.mockClear();
  claimRescoreRaceMock.mockResolvedValue({ proceed: true });
});

test("DEFAULT_RESCORE_LEAD_MINUTES is 25", () => {
  expect(DEFAULT_RESCORE_LEAD_MINUTES).toBe(25);
});

test("formatRunDateJst converts a UTC instant to the JST calendar date", () => {
  expect(formatRunDateJst(new Date("2026-06-19T01:00:00.000Z"))).toBe("2026-06-19");
});

test("formatRunDateJst rolls to the next JST day for late-UTC instants", () => {
  expect(formatRunDateJst(new Date("2026-06-19T16:00:00.000Z"))).toBe("2026-06-20");
});

test("formatRunYmdJst returns the 8-digit JST date", () => {
  expect(formatRunYmdJst(new Date("2026-06-19T01:00:00.000Z"))).toBe("20260619");
});

test("isWithinRescoreWindow returns true when post time is inside the lead window", () => {
  const now = new Date("2026-06-19T05:00:00.000Z");
  expect(isWithinRescoreWindow("2026-06-19T14:20:00+09:00", now, 25)).toBe(true);
});

test("isWithinRescoreWindow returns false when post time is already past", () => {
  const now = new Date("2026-06-19T05:00:00.000Z");
  expect(isWithinRescoreWindow("2026-06-19T13:00:00+09:00", now, 25)).toBe(false);
});

test("isWithinRescoreWindow returns false when post time is beyond the lead window", () => {
  const now = new Date("2026-06-19T05:00:00.000Z");
  expect(isWithinRescoreWindow("2026-06-19T15:00:00+09:00", now, 25)).toBe(false);
});

test("isWithinRescoreWindow includes the exact now boundary", () => {
  const now = new Date("2026-06-19T05:00:00.000Z");
  expect(isWithinRescoreWindow("2026-06-19T14:00:00+09:00", now, 25)).toBe(true);
});

test("isWithinRescoreWindow includes the exact window-end boundary", () => {
  const now = new Date("2026-06-19T05:00:00.000Z");
  expect(isWithinRescoreWindow("2026-06-19T14:25:00+09:00", now, 25)).toBe(true);
});

test("isWithinRescoreWindow returns false for an unparseable post time", () => {
  const now = new Date("2026-06-19T05:00:00.000Z");
  expect(isWithinRescoreWindow("not-a-date", now, 25)).toBe(false);
});

test("selectRacesWithinWindow keeps only in-window races and zero-pads the keys", () => {
  const now = new Date("2026-06-19T05:00:00.000Z");
  const targets = selectRacesWithinWindow(
    [
      { keibajo_code: "5", race_bango: "3", race_start_at_jst: "2026-06-19T14:10:00+09:00" },
      { keibajo_code: "5", race_bango: "4", race_start_at_jst: "2026-06-19T13:00:00+09:00" },
    ],
    now,
    25,
  );
  expect(targets).toStrictEqual([
    { keibajoCode: "05", raceBango: "03", raceStartAtJst: "2026-06-19T14:10:00+09:00" },
  ]);
});

test("selectRacesWithinWindow returns an empty array when no race is in window", () => {
  const now = new Date("2026-06-19T05:00:00.000Z");
  const targets = selectRacesWithinWindow(
    [{ keibajo_code: "05", race_bango: "01", race_start_at_jst: "2026-06-19T20:00:00+09:00" }],
    now,
    25,
  );
  expect(targets).toStrictEqual([]);
});

test("planRescoreForCategory queries D1 with the jra source and split date", async () => {
  stubD1Rows([]);
  await planRescoreForCategory({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(prepareMock).toHaveBeenCalledTimes(1);
  expect(bindMock).toHaveBeenCalledWith("jra", "2026", "0619");
});

test("planRescoreForCategory maps the ban-ei category to the nar source", async () => {
  stubD1Rows([]);
  await planRescoreForCategory({
    category: "ban-ei",
    date: "2026-06-19",
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0619");
});

test("planRescoreForCategory enqueues a per-race rescore message for an in-window race", async () => {
  stubD1Rows([
    { keibajo_code: "05", race_bango: "11", race_start_at_jst: "2026-06-19T14:10:00+09:00" },
  ]);
  const summary = await planRescoreForCategory({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(sendMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "11",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
  expect(summary).toStrictEqual({
    alreadyClaimed: 0,
    category: "jra",
    date: "2026-06-19",
    enqueued: 1,
    scanned: 1,
    withinWindow: 1,
  });
});

test("planRescoreForCategory claims each in-window race in the DO", async () => {
  stubD1Rows([
    { keibajo_code: "05", race_bango: "11", race_start_at_jst: "2026-06-19T14:10:00+09:00" },
  ]);
  await planRescoreForCategory({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(claimRescoreRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
});

test("planRescoreForCategory does not enqueue when the DO claim is rejected", async () => {
  claimRescoreRaceMock.mockResolvedValue({ proceed: false, state: "enqueued" });
  stubD1Rows([
    { keibajo_code: "05", race_bango: "11", race_start_at_jst: "2026-06-19T14:10:00+09:00" },
  ]);
  const summary = await planRescoreForCategory({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(sendMock).not.toHaveBeenCalled();
  expect(summary.enqueued).toBe(0);
  expect(summary.alreadyClaimed).toBe(1);
  expect(summary.withinWindow).toBe(1);
});

test("planRescoreForCategory skips out-of-window races without claiming or enqueueing", async () => {
  stubD1Rows([
    { keibajo_code: "05", race_bango: "01", race_start_at_jst: "2026-06-19T20:00:00+09:00" },
  ]);
  const summary = await planRescoreForCategory({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(summary).toStrictEqual({
    alreadyClaimed: 0,
    category: "jra",
    date: "2026-06-19",
    enqueued: 0,
    scanned: 1,
    withinWindow: 0,
  });
});

test("runRaceCoordinatorTick plans all three categories for the JST date", async () => {
  stubD1Rows([]);
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
  });
  expect(summaries.map((s) => s.category)).toStrictEqual(["jra", "nar", "ban-ei"]);
  expect(prepareMock).toHaveBeenCalledTimes(3);
});

test("runRaceCoordinatorTick uses the JST calendar date for each summary", async () => {
  stubD1Rows([]);
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T16:00:00.000Z"),
  });
  expect(summaries.map((summary) => summary.date)).toStrictEqual([
    "2026-06-20",
    "2026-06-20",
    "2026-06-20",
  ]);
});

test("isCoordinatorEnabled returns true when COORDINATOR_ENABLED is 1", () => {
  expect(isCoordinatorEnabled(makeEnv({ COORDINATOR_ENABLED: "1" }))).toBe(true);
});

test("isCoordinatorEnabled returns false when COORDINATOR_ENABLED is unset", () => {
  expect(isCoordinatorEnabled(makeEnv({ COORDINATOR_ENABLED: undefined }))).toBe(false);
});

test("isCoordinatorEnabled returns false when COORDINATOR_ENABLED is 0", () => {
  expect(isCoordinatorEnabled(makeEnv({ COORDINATOR_ENABLED: "0" }))).toBe(false);
});

test("runRaceCoordinatorTick is a shadow no-op when the coordinator is disabled", async () => {
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv({ COORDINATOR_ENABLED: undefined }),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
  });
  expect(prepareMock).not.toHaveBeenCalled();
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(summaries).toStrictEqual([
    {
      alreadyClaimed: 0,
      category: "jra",
      date: "2026-06-19",
      enqueued: 0,
      scanned: 0,
      withinWindow: 0,
    },
    {
      alreadyClaimed: 0,
      category: "nar",
      date: "2026-06-19",
      enqueued: 0,
      scanned: 0,
      withinWindow: 0,
    },
    {
      alreadyClaimed: 0,
      category: "ban-ei",
      date: "2026-06-19",
      enqueued: 0,
      scanned: 0,
      withinWindow: 0,
    },
  ]);
});

test("triggerWeightRebuildIfNeeded claims a synthetic WR race keyed by the JST half-hour slot in the DO", async () => {
  await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-05",
    env: makeEnv(),
    now: new Date("2026-06-05T01:00:00.000Z"),
    runYmd: "20260605",
  });
  expect(claimRescoreRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "WR",
    raceBango: "1000",
    runYmd: "20260605",
  });
});

test("triggerWeightRebuildIfNeeded uses different dedup keys for different JST half-hour slots", async () => {
  await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-05",
    env: makeEnv(),
    now: new Date("2026-06-05T01:00:00.000Z"),
    runYmd: "20260605",
  });
  await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-05",
    env: makeEnv(),
    now: new Date("2026-06-05T01:30:00.000Z"),
    runYmd: "20260605",
  });
  expect(claimRescoreRaceMock).toHaveBeenNthCalledWith(1, {
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "WR",
    raceBango: "1000",
    runYmd: "20260605",
  });
  expect(claimRescoreRaceMock).toHaveBeenNthCalledWith(2, {
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "WR",
    raceBango: "1030",
    runYmd: "20260605",
  });
});

test("triggerWeightRebuildIfNeeded uses the same dedup key within a single half-hour slot", async () => {
  await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-05",
    env: makeEnv(),
    now: new Date("2026-06-05T01:00:00.000Z"),
    runYmd: "20260605",
  });
  await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-05",
    env: makeEnv(),
    now: new Date("2026-06-05T01:17:00.000Z"),
    runYmd: "20260605",
  });
  expect(claimRescoreRaceMock).toHaveBeenNthCalledWith(1, {
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "WR",
    raceBango: "1000",
    runYmd: "20260605",
  });
  expect(claimRescoreRaceMock).toHaveBeenNthCalledWith(2, {
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "WR",
    raceBango: "1000",
    runYmd: "20260605",
  });
});

test("triggerWeightRebuildIfNeeded floors to the lower slot just before the half-hour boundary", async () => {
  await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-05",
    env: makeEnv(),
    now: new Date("2026-06-05T01:29:00.000Z"),
    runYmd: "20260605",
  });
  expect(claimRescoreRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "WR",
    raceBango: "1000",
    runYmd: "20260605",
  });
});

test("triggerWeightRebuildIfNeeded uses the upper slot at the half-hour boundary", async () => {
  await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-05",
    env: makeEnv(),
    now: new Date("2026-06-05T01:30:00.000Z"),
    runYmd: "20260605",
  });
  expect(claimRescoreRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "WR",
    raceBango: "1030",
    runYmd: "20260605",
  });
});

test("triggerWeightRebuildIfNeeded sends a rescore-mode skipDedup message when claim proceeds", async () => {
  claimRescoreRaceMock.mockResolvedValue({ proceed: true });
  const enqueued = await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    mode: "rescore",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
    skipDedup: true,
  });
  expect(enqueued).toBe(true);
});

test("triggerWeightRebuildIfNeeded does not send when claim is rejected", async () => {
  claimRescoreRaceMock.mockResolvedValue({ proceed: false, state: "enqueued" });
  const enqueued = await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(sendMock).not.toHaveBeenCalled();
  expect(enqueued).toBe(false);
});

test("runRaceCoordinatorTick triggers weight rebuild for categories with enqueued races", async () => {
  stubD1Rows([
    { keibajo_code: "05", race_bango: "11", race_start_at_jst: "2026-06-19T14:10:00+09:00" },
  ]);
  await runRaceCoordinatorTick({
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
  });
  expect(claimRescoreRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
    keibajoCode: "WR",
    raceBango: "1400",
    runYmd: "20260619",
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    mode: "rescore",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
    skipDedup: true,
  });
});

test("runRaceCoordinatorTick does not trigger weight rebuild when no races are enqueued", async () => {
  stubD1Rows([]);
  await runRaceCoordinatorTick({
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
  });
  expect(sendMock).not.toHaveBeenCalled();
});

test("runRaceCoordinatorTick does not trigger weight rebuild when the coordinator is disabled", async () => {
  await runRaceCoordinatorTick({
    env: makeEnv({ COORDINATOR_ENABLED: undefined }),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
  });
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
});
