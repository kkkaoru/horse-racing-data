// Run with bun. Tests for the per-race finish-position rescore coordinator.

import { beforeEach, expect, test, vi } from "vitest";

const { claimRescoreRaceMock } = vi.hoisted(() => ({ claimRescoreRaceMock: vi.fn() }));

vi.mock("./do-state", () => ({ claimRescoreRace: claimRescoreRaceMock }));

import {
  DEFAULT_RESCORE_LEAD_MINUTES,
  KOCHI_KEIBAJO_CODE,
  fetchCardMaxRaceBango,
  formatRunDateJst,
  formatRunYmdJst,
  isCoordinatorEnabled,
  isWithinCategoryTimeBox,
  isWithinRescoreWindow,
  planRescoreForCategory,
  resolveCardMaxRaceBangoForKochi,
  resolveRescoreCategories,
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

const stubD1FirstRow = (maxRaceBango: number | null): void => {
  bindMock.mockReturnValue({ first: vi.fn(async () => ({ max_race_bango: maxRaceBango })) });
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

test("isWithinCategoryTimeBox always returns true for jra regardless of hour", () => {
  expect(isWithinCategoryTimeBox("jra", new Date("2026-06-19T00:00:00.000Z"))).toBe(true);
});

test("isWithinCategoryTimeBox returns true for jra at a JST hour outside 14-21", () => {
  expect(isWithinCategoryTimeBox("jra", new Date("2026-06-19T02:00:00.000Z"))).toBe(true);
});

test("isWithinCategoryTimeBox returns false for nar just before the JST 10:00 window start", () => {
  expect(isWithinCategoryTimeBox("nar", new Date("2026-06-19T00:59:00.000Z"))).toBe(false);
});

test("isWithinCategoryTimeBox returns true for nar at the exact JST 10:00 window start", () => {
  expect(isWithinCategoryTimeBox("nar", new Date("2026-06-19T01:00:00.000Z"))).toBe(true);
});

test("isWithinCategoryTimeBox returns true for nar in the middle of the JST window", () => {
  expect(isWithinCategoryTimeBox("nar", new Date("2026-06-19T09:00:00.000Z"))).toBe(true);
});

test("isWithinCategoryTimeBox returns true for nar just before the JST 21:00 window end", () => {
  expect(isWithinCategoryTimeBox("nar", new Date("2026-06-19T11:59:00.000Z"))).toBe(true);
});

test("isWithinCategoryTimeBox returns false for nar at the exact JST 21:00 window end", () => {
  expect(isWithinCategoryTimeBox("nar", new Date("2026-06-19T12:00:00.000Z"))).toBe(false);
});

test("isWithinCategoryTimeBox returns false for ban-ei just before the JST 14:00 window start", () => {
  expect(isWithinCategoryTimeBox("ban-ei", new Date("2026-06-19T04:59:00.000Z"))).toBe(false);
});

test("isWithinCategoryTimeBox returns true for ban-ei at the exact JST 14:00 window start", () => {
  expect(isWithinCategoryTimeBox("ban-ei", new Date("2026-06-19T05:00:00.000Z"))).toBe(true);
});

test("isWithinCategoryTimeBox returns false for ban-ei at the exact JST 21:00 window end", () => {
  expect(isWithinCategoryTimeBox("ban-ei", new Date("2026-06-19T12:00:00.000Z"))).toBe(false);
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
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0619", "83");
});

test("planRescoreForCategory excludes the ban-ei keibajo code for the normal nar category", async () => {
  stubD1Rows([]);
  await planRescoreForCategory({
    category: "nar",
    date: "2026-06-19",
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(prepareMock).toHaveBeenCalledWith(expect.stringContaining("source in (?)"));
  expect(prepareMock).toHaveBeenCalledWith(expect.stringContaining("keibajo_code not in (?)"));
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0619", "83");
});

test("planRescoreForCategory includes only the ban-ei keibajo code while using the nar source", async () => {
  stubD1Rows([]);
  await planRescoreForCategory({
    category: "ban-ei",
    date: "2026-06-19",
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(prepareMock).toHaveBeenCalledWith(expect.stringContaining("source in (?)"));
  expect(prepareMock).toHaveBeenCalledWith(expect.stringContaining("keibajo_code in (?)"));
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0619", "83");
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

test("runRaceCoordinatorTick plans only the default JRA-only category for the JST date", async () => {
  stubD1Rows([]);
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
  });
  expect(summaries.map((s) => s.category)).toStrictEqual(["jra"]);
  expect(prepareMock).toHaveBeenCalledTimes(1);
});

test("runRaceCoordinatorTick plans every category listed in RESCORE_CATEGORIES", async () => {
  stubD1Rows([]);
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv({ RESCORE_CATEGORIES: "jra,nar,ban-ei" }),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
  });
  expect(summaries.map((s) => s.category)).toStrictEqual(["jra", "nar", "ban-ei"]);
  expect(prepareMock).toHaveBeenCalledTimes(3);
});

test("runRaceCoordinatorTick at JST 09:00 shadows nar and ban-ei while jra still plans", async () => {
  stubD1Rows([]);
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv({ RESCORE_CATEGORIES: "jra,nar,ban-ei" }),
    leadMinutes: 25,
    now: new Date("2026-06-19T00:00:00.000Z"), // JST 09:00 -- before nar 10:00 and ban-ei 14:00
  });
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
  expect(prepareMock).toHaveBeenCalledTimes(1);
});

test("runRaceCoordinatorTick at JST 11:00 plans jra and nar while ban-ei still shadows", async () => {
  stubD1Rows([]);
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv({ RESCORE_CATEGORIES: "jra,nar,ban-ei" }),
    leadMinutes: 25,
    now: new Date("2026-06-19T02:00:00.000Z"), // JST 11:00 -- inside nar [10,21), outside ban-ei [14,21)
  });
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
  expect(prepareMock).toHaveBeenCalledTimes(2);
});

test("runRaceCoordinatorTick at JST 18:00 plans jra, nar, and ban-ei", async () => {
  stubD1Rows([]);
  await runRaceCoordinatorTick({
    env: makeEnv({ RESCORE_CATEGORIES: "jra,nar,ban-ei" }),
    leadMinutes: 25,
    now: new Date("2026-06-19T09:00:00.000Z"), // JST 18:00 -- inside both nar and ban-ei boxes
  });
  expect(prepareMock).toHaveBeenCalledTimes(3);
});

test("runRaceCoordinatorTick uses the JST calendar date for each summary", async () => {
  stubD1Rows([]);
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv(),
    leadMinutes: 25,
    now: new Date("2026-06-19T16:00:00.000Z"),
  });
  expect(summaries.map((summary) => summary.date)).toStrictEqual(["2026-06-20"]);
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
  ]);
});

test("runRaceCoordinatorTick shadow no-op reflects RESCORE_CATEGORIES scope", async () => {
  const summaries = await runRaceCoordinatorTick({
    env: makeEnv({ COORDINATOR_ENABLED: undefined, RESCORE_CATEGORIES: "nar,ban-ei" }),
    leadMinutes: 25,
    now: new Date("2026-06-19T05:00:00.000Z"),
  });
  expect(summaries).toStrictEqual([
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

test("resolveRescoreCategories defaults to JRA-only when RESCORE_CATEGORIES is unset", () => {
  expect(resolveRescoreCategories(makeEnv({ RESCORE_CATEGORIES: undefined }))).toStrictEqual([
    "jra",
  ]);
});

test("resolveRescoreCategories defaults to JRA-only when RESCORE_CATEGORIES is empty", () => {
  expect(resolveRescoreCategories(makeEnv({ RESCORE_CATEGORIES: "" }))).toStrictEqual(["jra"]);
});

test("resolveRescoreCategories parses a single category", () => {
  expect(resolveRescoreCategories(makeEnv({ RESCORE_CATEGORIES: "jra" }))).toStrictEqual(["jra"]);
});

test("resolveRescoreCategories parses multiple comma-separated categories", () => {
  expect(resolveRescoreCategories(makeEnv({ RESCORE_CATEGORIES: "jra,nar" }))).toStrictEqual([
    "jra",
    "nar",
  ]);
});

test("resolveRescoreCategories trims whitespace around each token", () => {
  expect(resolveRescoreCategories(makeEnv({ RESCORE_CATEGORIES: " nar , ban-ei " }))).toStrictEqual(
    ["nar", "ban-ei"],
  );
});

test("resolveRescoreCategories drops unrecognized tokens", () => {
  expect(
    resolveRescoreCategories(makeEnv({ RESCORE_CATEGORIES: "nar,not-a-category,ban-ei" })),
  ).toStrictEqual(["nar", "ban-ei"]);
});

test("resolveRescoreCategories falls back to JRA-only when no token is recognized", () => {
  expect(resolveRescoreCategories(makeEnv({ RESCORE_CATEGORIES: "not-a-category" }))).toStrictEqual(
    ["jra"],
  );
});

test("triggerWeightRebuildIfNeeded claims a synthetic WR race keyed by the JST half-hour slot in the DO", async () => {
  stubD1Rows([]);
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
  stubD1Rows([]);
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
  stubD1Rows([]);
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
  stubD1Rows([]);
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
  stubD1Rows([]);
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

test("triggerWeightRebuildIfNeeded enqueues one per-race rescore message per race registered for the day", async () => {
  claimRescoreRaceMock.mockResolvedValue({ proceed: true });
  stubD1Rows([
    { keibajo_code: "5", race_bango: "1", race_start_at_jst: "2026-06-19T10:10:00+09:00" },
    { keibajo_code: "5", race_bango: "2", race_start_at_jst: "2026-06-19T10:40:00+09:00" },
    { keibajo_code: "2", race_bango: "11", race_start_at_jst: "2026-06-19T15:30:00+09:00" },
  ]);
  const enqueued = await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(enqueued).toBe(3);
  expect(sendMock).toHaveBeenCalledTimes(3);
  expect(sendMock).toHaveBeenNthCalledWith(1, {
    category: "jra",
    daysAhead: 0,
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "01",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
  expect(sendMock).toHaveBeenNthCalledWith(2, {
    category: "jra",
    daysAhead: 0,
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "02",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
  expect(sendMock).toHaveBeenNthCalledWith(3, {
    category: "jra",
    daysAhead: 0,
    keibajoCode: "02",
    mode: "rescore",
    raceBango: "11",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
});

test("triggerWeightRebuildIfNeeded enqueues nothing on a day with zero registered races for the category", async () => {
  claimRescoreRaceMock.mockResolvedValue({ proceed: true });
  stubD1Rows([]);
  const enqueued = await triggerWeightRebuildIfNeeded({
    category: "ban-ei",
    date: "2026-06-19",
    env: makeEnv(),
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(enqueued).toBe(0);
  expect(sendMock).not.toHaveBeenCalled();
});

test("triggerWeightRebuildIfNeeded does not send when claim is rejected", async () => {
  claimRescoreRaceMock.mockResolvedValue({ proceed: false, state: "enqueued" });
  stubD1Rows([
    { keibajo_code: "05", race_bango: "11", race_start_at_jst: "2026-06-19T14:10:00+09:00" },
  ]);
  const enqueued = await triggerWeightRebuildIfNeeded({
    category: "jra",
    date: "2026-06-19",
    env: makeEnv(),
    now: new Date("2026-06-19T05:00:00.000Z"),
    runYmd: "20260619",
  });
  expect(sendMock).not.toHaveBeenCalled();
  expect(enqueued).toBe(0);
});

test("runRaceCoordinatorTick triggers a weight-rebuild per-race fan-out for categories with enqueued races", async () => {
  // The single stubbed race both falls inside the near-post window (drives
  // planRescoreForCategory's own enqueue) and is the only row
  // listRacesForCategory returns for the weight-rebuild fan-out -- so this
  // one race is dispatched twice, once by each independent mechanism, both
  // producing the identical per-race rescore message shape.
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
  expect(sendMock).toHaveBeenCalledTimes(2);
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

test("KOCHI_KEIBAJO_CODE is 54", () => {
  expect(KOCHI_KEIBAJO_CODE).toBe("54");
});

test("fetchCardMaxRaceBango queries realtime_race_sources with source/keibajo/date binds", async () => {
  stubD1FirstRow(10);
  const result = await fetchCardMaxRaceBango({
    env: makeEnv(),
    keibajoCode: "54",
    runYmd: "20260712",
    source: "nar",
  });
  expect(result).toBe(10);
  expect(bindMock).toHaveBeenCalledWith("nar", "54", "2026", "0712");
});

test("fetchCardMaxRaceBango returns null when discovery has no rows for the card", async () => {
  stubD1FirstRow(null);
  const result = await fetchCardMaxRaceBango({
    env: makeEnv(),
    keibajoCode: "54",
    runYmd: "20260712",
    source: "nar",
  });
  expect(result).toBe(null);
});

test("fetchCardMaxRaceBango returns null when the D1 row itself is undefined", async () => {
  bindMock.mockReturnValue({ first: vi.fn(async () => undefined) });
  const result = await fetchCardMaxRaceBango({
    env: makeEnv(),
    keibajoCode: "54",
    runYmd: "20260712",
    source: "nar",
  });
  expect(result).toBe(null);
});

test("resolveCardMaxRaceBangoForKochi returns undefined without querying D1 for a non-Kochi venue", async () => {
  const result = await resolveCardMaxRaceBangoForKochi({
    env: makeEnv(),
    keibajoCode: "10",
    runYmd: "20260712",
  });
  expect(result).toBe(undefined);
  expect(prepareMock).not.toHaveBeenCalled();
});

test("resolveCardMaxRaceBangoForKochi returns undefined when keibajoCode is absent", async () => {
  const result = await resolveCardMaxRaceBangoForKochi({
    env: makeEnv(),
    keibajoCode: undefined,
    runYmd: "20260712",
  });
  expect(result).toBe(undefined);
  expect(prepareMock).not.toHaveBeenCalled();
});

test("resolveCardMaxRaceBangoForKochi returns the fetched value for keibajoCode 54", async () => {
  stubD1FirstRow(10);
  const result = await resolveCardMaxRaceBangoForKochi({
    env: makeEnv(),
    keibajoCode: "54",
    runYmd: "20260712",
  });
  expect(result).toBe(10);
});

test("resolveCardMaxRaceBangoForKochi returns undefined when discovery has no rows yet", async () => {
  stubD1FirstRow(null);
  const result = await resolveCardMaxRaceBangoForKochi({
    env: makeEnv(),
    keibajoCode: "54",
    runYmd: "20260712",
  });
  expect(result).toBe(undefined);
});

test("resolveCardMaxRaceBangoForKochi fails closed to undefined when the D1 query throws", async () => {
  bindMock.mockReturnValue({
    first: vi.fn(async () => {
      throw new Error("D1 unavailable");
    }),
  });
  const result = await resolveCardMaxRaceBangoForKochi({
    env: makeEnv(),
    keibajoCode: "54",
    runYmd: "20260712",
  });
  expect(result).toBe(undefined);
});
