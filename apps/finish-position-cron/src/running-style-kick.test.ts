// Run with bun. Tests for the running-style HTTP kick module.

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
import type { Env } from "./types";

const makeEnv = (): Env =>
  ({
    REALTIME_ADMIN_TOKEN: "admin-secret",
  }) as Env;

const fetchMock = vi.fn(
  async (_url: string, _init: RequestInit): Promise<Response> =>
    new Response(null, { status: 200 }),
);

beforeEach(() => {
  fetchMock.mockClear();
  fetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
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

test("kickRunningStylePlan POSTs to sync-realtime-data's /api/jobs with the plan job body", async () => {
  await kickRunningStylePlan({ date: "20260719", env: makeEnv() });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
  expect(url).toBe("https://sync-realtime-data.kkk4oru.com/api/jobs");
  expect(init.method).toBe("POST");
  expect(init.body).toBe('{"date":"20260719","type":"plan-running-style-predictions"}');
});

test("kickRunningStylePlan authorizes with the REALTIME_ADMIN_TOKEN bearer header", async () => {
  await kickRunningStylePlan({ date: "20260719", env: makeEnv() });
  const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
  const headers = init.headers as Record<string, string>;
  expect(headers.authorization).toBe("Bearer admin-secret");
  expect(headers["content-type"]).toBe("application/json");
});

test("kickRunningStylePlan falls back to an empty bearer token when REALTIME_ADMIN_TOKEN is unset", async () => {
  await kickRunningStylePlan({ date: "20260719", env: {} as Env });
  const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
  const headers = init.headers as Record<string, string>;
  expect(headers.authorization).toBe("Bearer ");
});

test("kickRunningStylePlan logs ok on a 2xx response and does not throw", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  fetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  await expect(kickRunningStylePlan({ date: "20260719", env: makeEnv() })).resolves.toBeUndefined();
  expect(logSpy).toHaveBeenCalledWith("[running-style-kick] ok date=20260719 status=200");
  logSpy.mockRestore();
});

test("kickRunningStylePlan logs a non-2xx response as an error and does not throw", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  fetchMock.mockResolvedValue(new Response(null, { status: 403 }));
  await expect(kickRunningStylePlan({ date: "20260719", env: makeEnv() })).resolves.toBeUndefined();
  expect(errorSpy).toHaveBeenCalledWith("[running-style-kick] non-2xx date=20260719 status=403");
  errorSpy.mockRestore();
});

test("kickRunningStylePlan swallows a network failure instead of throwing", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  fetchMock.mockRejectedValue(new Error("network unreachable"));
  await expect(kickRunningStylePlan({ date: "20260719", env: makeEnv() })).resolves.toBeUndefined();
  expect(errorSpy).toHaveBeenCalledWith(
    "[running-style-kick] failed date=20260719: Error: network unreachable",
  );
  errorSpy.mockRestore();
});

test("kickRunningStylePlan logs an unconditional start line before the fetch", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await kickRunningStylePlan({ date: "20260719", env: makeEnv() });
  expect(logSpy).toHaveBeenCalledWith("[running-style-kick] start date=20260719");
  logSpy.mockRestore();
});

test("runRunningStyleKickMorningGap kicks TODAY's JST date derived from now", async () => {
  await runRunningStyleKickMorningGap({
    env: makeEnv(),
    now: new Date("2026-07-18T16:00:00.000Z"),
  });
  const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
  expect(init.body).toBe('{"date":"20260719","type":"plan-running-style-predictions"}');
});

test("runRunningStyleKickTomorrowPrewarm kicks TOMORROW's JST date derived from now", async () => {
  await runRunningStyleKickTomorrowPrewarm({
    env: makeEnv(),
    now: new Date("2026-07-18T13:00:00.000Z"),
  });
  const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
  expect(init.body).toBe('{"date":"20260719","type":"plan-running-style-predictions"}');
});

test("runRunningStyleKickTomorrowPrewarm rolls over a month boundary", async () => {
  await runRunningStyleKickTomorrowPrewarm({
    env: makeEnv(),
    now: new Date("2026-01-31T01:00:00.000Z"),
  });
  const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
  expect(init.body).toBe('{"date":"20260201","type":"plan-running-style-predictions"}');
});

test("runRunningStyleKickTomorrowPrewarm rolls over a year boundary", async () => {
  await runRunningStyleKickTomorrowPrewarm({
    env: makeEnv(),
    now: new Date("2026-12-31T01:00:00.000Z"),
  });
  const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
  expect(init.body).toBe('{"date":"20270101","type":"plan-running-style-predictions"}');
});
