// Run with bun. Tests for the day-base prewarm cron dispatch: distinct
// per-category GET /prewarm-day-base calls against the Container DO, with
// per-category failure isolation.

import { beforeEach, expect, test, vi } from "vitest";
import type { RaceEntry } from "./cron-decision";

const { enumerateTodaysRacesMock } = vi.hoisted(() => ({
  enumerateTodaysRacesMock: vi.fn(async (): Promise<RaceEntry[]> => []),
}));

vi.mock("./cron-decision", () => ({
  enumerateTodaysRaces: enumerateTodaysRacesMock,
}));

import { prewarmCategory, runDayBasePrewarm } from "./day-base-prewarm";
import type { Env } from "./types";

const containerDoFetchMock = vi.fn((_request: Request) =>
  Promise.resolve(new Response("", { status: 200 })),
);
const containerDoGetMock = vi.fn(() => ({ fetch: containerDoFetchMock }));
const containerDoIdFromNameMock = vi.fn((name: string) => ({ name }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {
    get: containerDoGetMock,
    idFromName: containerDoIdFromNameMock,
  } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: {} as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

const resultLineBody = (fields: Record<string, string>): string =>
  `${JSON.stringify({ type: "result", ...fields })}\n`;

beforeEach(() => {
  enumerateTodaysRacesMock.mockClear();
  containerDoFetchMock.mockClear();
  containerDoGetMock.mockClear();
  containerDoIdFromNameMock.mockClear();
  enumerateTodaysRacesMock.mockResolvedValue([]);
  containerDoFetchMock.mockImplementation(() => Promise.resolve(new Response("", { status: 200 })));
});

test("runDayBasePrewarm skips dispatch and logs when no races are scheduled today", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([]);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await runDayBasePrewarm({ daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(containerDoIdFromNameMock).not.toHaveBeenCalled();
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] no races scheduled runYmd=20260628 -- skipping dispatch",
  );
  logSpy.mockRestore();
});

test("runDayBasePrewarm dedupes categories from a mixed-category race list and dispatches each once", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "jra", keibajoCode: "05", raceBango: "01" },
    { category: "jra", keibajoCode: "05", raceBango: "02" },
    { category: "nar", keibajoCode: "44", raceBango: "01" },
    { category: "ban-ei", keibajoCode: "83", raceBango: "01" },
  ]);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await runDayBasePrewarm({ daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(containerDoIdFromNameMock).toHaveBeenCalledTimes(3);
  expect(containerDoIdFromNameMock).toHaveBeenCalledWith("predict-jra");
  expect(containerDoIdFromNameMock).toHaveBeenCalledWith("predict-nar");
  expect(containerDoIdFromNameMock).toHaveBeenCalledWith("predict-ban-ei");
  expect(containerDoFetchMock).toHaveBeenCalledTimes(3);
  logSpy.mockRestore();
});

test("runDayBasePrewarm logs and does not throw when enumerateTodaysRaces rejects", async () => {
  enumerateTodaysRacesMock.mockRejectedValue(new Error("D1 query failed"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(
    runDayBasePrewarm({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" }),
  ).resolves.toBeUndefined();
  expect(containerDoIdFromNameMock).not.toHaveBeenCalled();
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] enumerate failed runYmd=20260712: Error: D1 query failed",
  );
  errorSpy.mockRestore();
  logSpy.mockRestore();
});

test("runDayBasePrewarm logs a start line before enumerating, so an uncaught throw still leaves evidence", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([]);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await runDayBasePrewarm({ daysAhead: 2, env: makeEnv(), runYmd: "20260712" });
  expect(logSpy).toHaveBeenCalledWith("[day-base-prewarm] start runYmd=20260712");
  logSpy.mockRestore();
});

test("runDayBasePrewarm continues warming other categories when one category's DO fetch rejects", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "jra", keibajoCode: "05", raceBango: "01" },
    { category: "nar", keibajoCode: "44", raceBango: "01" },
  ]);
  containerDoFetchMock.mockImplementation((request: Request) => {
    if (request.url.includes("category=jra")) return Promise.reject(new Error("boom"));
    return Promise.resolve(
      new Response(
        resultLineBody({
          category: "nar",
          parquetKey: "nar/20260628/day-base.parquet",
          runDate: "20260628",
          status: "success",
        }),
        { status: 200 },
      ),
    );
  });
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(
    runDayBasePrewarm({ daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBeUndefined();
  expect(containerDoFetchMock).toHaveBeenCalledTimes(2);
  const urls = containerDoFetchMock.mock.calls.map((call) => (call as unknown as [Request])[0].url);
  expect(urls).toStrictEqual([
    "http://do/prewarm-day-base?category=jra&daysAhead=2&runDate=20260628",
    "http://do/prewarm-day-base?category=nar&daysAhead=2&runDate=20260628",
  ]);
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=jra runYmd=20260628: Error: boom",
  );
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] success category=nar runYmd=20260628 status=success parquetKey=nar/20260628/day-base.parquet error=-",
  );
  errorSpy.mockRestore();
  logSpy.mockRestore();
});

test("prewarmCategory requests the category-scoped DO with category, daysAhead, and runDate query params", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 3, env: makeEnv(), runYmd: "20260701" });
  expect(containerDoIdFromNameMock).toHaveBeenCalledWith("predict-jra");
  const request = (containerDoFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(request.url).toBe("http://do/prewarm-day-base?category=jra&daysAhead=3&runDate=20260701");
  logSpy.mockRestore();
  warnSpy.mockRestore();
});

test("prewarmCategory logs a success outcome when the container returns status success", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "jra",
          parquetKey: "jra/20260628/day-base.parquet",
          runDate: "20260628",
          status: "success",
        }),
        { status: 200 },
      ),
    ),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] success category=jra runYmd=20260628 status=success parquetKey=jra/20260628/day-base.parquet error=-",
  );
  logSpy.mockRestore();
});

test("prewarmCategory logs an empty outcome when the container returns status empty", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(resultLineBody({ category: "nar", runDate: "20260628", status: "empty" }), {
        status: 200,
      }),
    ),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await prewarmCategory({ category: "nar", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] empty category=nar runYmd=20260628 status=empty parquetKey=- error=-",
  );
  logSpy.mockRestore();
});

test("prewarmCategory logs a warning when the container returns status error inside a 200 response", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "jra",
          error: "boom",
          runDate: "20260628",
          status: "error",
        }),
        { status: 200 },
      ),
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=jra runYmd=20260628 status=error parquetKey=- error=boom",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory logs a warning and does not throw for a non-ok HTTP response", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response("server error", { status: 500 })),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await expect(
    prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBeUndefined();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] non-ok response category=jra runYmd=20260628 status=500",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory logs a warning and does not throw when the response body is null", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response(null, { status: 200 })),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await expect(
    prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBeUndefined();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] non-ok response category=jra runYmd=20260628 status=200",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory logs a failed outcome with the status placeholder when the result line omits status", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response('{"type":"result","category":"jra"}\n', { status: 200 })),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=jra runYmd=20260628 status=- parquetKey=- error=-",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory logs a warning when the final ndjson line is not a result line", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response('{"type":"progress","message":"working"}\n', { status: 200 })),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] unparseable result category=jra runYmd=20260628",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory catches and logs without throwing when the response body is unparseable JSON", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response("not-json-at-all\n", { status: 200 })),
  );
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await expect(
    prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBeUndefined();
  expect(errorSpy).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("prewarmCategory catches and logs without throwing when the DO stub fetch rejects", async () => {
  containerDoFetchMock.mockImplementation(() => Promise.reject(new Error("network down")));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await expect(
    prewarmCategory({ category: "ban-ei", daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBeUndefined();
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=ban-ei runYmd=20260628: Error: network down",
  );
  errorSpy.mockRestore();
});
