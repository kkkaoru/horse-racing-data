// Run with bun. Tests for the running-style Queue kick module.

import { afterEach, beforeEach, expect, test, vi } from "vitest";
import {
  kickRunningStylePlan,
  runRunningStyleKickMorningGap,
  runRunningStyleKickTomorrowPrewarm,
  RUNNING_STYLE_KICK_CRON_MORNING_GAP,
  RUNNING_STYLE_KICK_CRON_TOMORROW_PREWARM,
  shouldRunRunningStyleKickMorningGapCron,
  shouldRunRunningStyleKickTomorrowPrewarmCron,
} from "./running-style-kick";
import type { RunningStylePlanJobMessage } from "./types";

const sendMock = vi.fn(async (_message: RunningStylePlanJobMessage): Promise<void> => undefined);

const makeEnv = () => ({ RUNNING_STYLE_PLAN_JOBS: { send: sendMock } });

beforeEach(() => {
  sendMock.mockClear();
  sendMock.mockResolvedValue(undefined);
});

afterEach(() => {
  vi.restoreAllMocks();
});

test("shouldRunRunningStyleKickMorningGapCron matches the morning-gap cron", () => {
  expect(shouldRunRunningStyleKickMorningGapCron(RUNNING_STYLE_KICK_CRON_MORNING_GAP)).toBe(true);
});

test("shouldRunRunningStyleKickMorningGapCron rejects an unrelated cron string", () => {
  expect(shouldRunRunningStyleKickMorningGapCron("*/10 1-11 * * *")).toBe(false);
});

test("shouldRunRunningStyleKickTomorrowPrewarmCron matches the tomorrow-prewarm cron", () => {
  expect(
    shouldRunRunningStyleKickTomorrowPrewarmCron(RUNNING_STYLE_KICK_CRON_TOMORROW_PREWARM),
  ).toBe(true);
});

test("shouldRunRunningStyleKickTomorrowPrewarmCron rejects an unrelated cron string", () => {
  expect(shouldRunRunningStyleKickTomorrowPrewarmCron("*/10 1-11 * * *")).toBe(false);
});

test("kickRunningStylePlan enqueues the dated plan job through the direct binding", async () => {
  await kickRunningStylePlan({ date: "20260719", env: makeEnv() });
  expect(sendMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith({
    date: "20260719",
    type: "plan-running-style-predictions",
  });
});

test("kickRunningStylePlan logs the queued date", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(kickRunningStylePlan({ date: "20260719", env: makeEnv() })).resolves.toBeUndefined();
  expect(logSpy).toHaveBeenCalledWith("[running-style-kick] queued date=20260719");
});

test("kickRunningStylePlan logs the failed date and rethrows a Queue failure", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  sendMock.mockRejectedValueOnce(new Error("queue unavailable"));
  await expect(kickRunningStylePlan({ date: "20260719", env: makeEnv() })).rejects.toThrow(
    "queue unavailable",
  );
  expect(errorSpy).toHaveBeenCalledWith(
    "[running-style-kick] failed date=20260719: Error: queue unavailable",
  );
});

test("kickRunningStylePlan fails closed and logs the date when the binding is absent", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await expect(kickRunningStylePlan({ date: "20260719", env: {} })).rejects.toThrow(
    "RUNNING_STYLE_PLAN_JOBS binding is unavailable",
  );
  expect(errorSpy).toHaveBeenCalledWith(
    "[running-style-kick] failed date=20260719: Error: RUNNING_STYLE_PLAN_JOBS binding is unavailable",
  );
});

test("kickRunningStylePlan logs an unconditional start line before the enqueue", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await kickRunningStylePlan({ date: "20260719", env: makeEnv() });
  expect(logSpy).toHaveBeenCalledWith("[running-style-kick] start date=20260719");
});

test("runRunningStyleKickMorningGap kicks TODAY's JST date derived from now", async () => {
  await runRunningStyleKickMorningGap({
    env: makeEnv(),
    now: new Date("2026-07-18T16:00:00.000Z"),
  });
  expect(sendMock).toHaveBeenCalledWith({
    date: "20260719",
    type: "plan-running-style-predictions",
  });
});

test("runRunningStyleKickTomorrowPrewarm kicks TOMORROW's JST date derived from now", async () => {
  await runRunningStyleKickTomorrowPrewarm({
    env: makeEnv(),
    now: new Date("2026-07-18T13:00:00.000Z"),
  });
  expect(sendMock).toHaveBeenCalledWith({
    date: "20260719",
    type: "plan-running-style-predictions",
  });
});

test("runRunningStyleKickTomorrowPrewarm rolls over a month boundary", async () => {
  await runRunningStyleKickTomorrowPrewarm({
    env: makeEnv(),
    now: new Date("2026-01-31T01:00:00.000Z"),
  });
  expect(sendMock).toHaveBeenCalledWith({
    date: "20260201",
    type: "plan-running-style-predictions",
  });
});

test("runRunningStyleKickTomorrowPrewarm rolls over a year boundary", async () => {
  await runRunningStyleKickTomorrowPrewarm({
    env: makeEnv(),
    now: new Date("2026-12-31T01:00:00.000Z"),
  });
  expect(sendMock).toHaveBeenCalledWith({
    date: "20270101",
    type: "plan-running-style-predictions",
  });
});
