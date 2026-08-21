// Run with bun. Tests for the day-base prewarm cron dispatch: distinct
// per-category GET /prewarm-day-base calls against the Container DO, with
// per-category failure isolation.

import { beforeEach, expect, test, vi } from "vitest";
import type { RaceEntry } from "./cron-decision";

interface DayBaseHeadHit {
  size: number;
}

type DayBaseHeadResult = DayBaseHeadHit | null;

const {
  claimContainerSlotMock,
  enumerateTodaysRacesMock,
  headDayBaseObjectMock,
  pickUpPrewarmDayBaseMock,
  releaseContainerSlotMock,
} = vi.hoisted(() => ({
  claimContainerSlotMock: vi.fn(
    async (): Promise<{ proceed: boolean; state?: string }> => ({ proceed: true }),
  ),
  enumerateTodaysRacesMock: vi.fn(async (): Promise<RaceEntry[]> => []),
  headDayBaseObjectMock: vi.fn(async (): Promise<DayBaseHeadResult> => null),
  pickUpPrewarmDayBaseMock: vi.fn(async (): Promise<boolean> => false),
  releaseContainerSlotMock: vi.fn(async () => undefined),
}));

vi.mock("./cron-decision", () => ({
  enumerateTodaysRaces: enumerateTodaysRacesMock,
}));

vi.mock("./do-state", () => ({
  claimContainerSlot: claimContainerSlotMock,
  releaseContainerSlot: releaseContainerSlotMock,
}));

vi.mock("./day-base-prewarm-pickup", () => ({
  buildDayBaseObjectKey: (params: { category: string; runYmd: string }) =>
    `feat-daybase/catalog-v1/${params.category}/${params.runYmd}/features.parquet`,
  headDayBaseObject: headDayBaseObjectMock,
  pickUpPrewarmDayBase: pickUpPrewarmDayBaseMock,
}));

import { prewarmCategory, runDayBasePrewarm } from "./day-base-prewarm";
import type { Env } from "./types";

const containerDoFetchMock = vi.fn((_request: Request) =>
  Promise.resolve(new Response("", { status: 200 })),
);
const containerDoGetMock = vi.fn(() => ({ fetch: containerDoFetchMock }));
const containerDoIdFromNameMock = vi.fn((name: string) => ({ name }));
const queueSendMock = vi.fn(async () => undefined);

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {
    get: containerDoGetMock,
    idFromName: containerDoIdFromNameMock,
  } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: { send: queueSendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: {} as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

const resultLineBody = (fields: Record<string, string>): string =>
  `${JSON.stringify({ type: "result", ...fields })}\n`;

const isRequest = (value: unknown): value is Request =>
  typeof value === "object" && value !== null && "url" in value;

beforeEach(() => {
  enumerateTodaysRacesMock.mockClear();
  containerDoFetchMock.mockClear();
  containerDoGetMock.mockClear();
  containerDoIdFromNameMock.mockClear();
  queueSendMock.mockClear();
  headDayBaseObjectMock.mockReset();
  pickUpPrewarmDayBaseMock.mockReset();
  claimContainerSlotMock.mockClear();
  releaseContainerSlotMock.mockReset();
  headDayBaseObjectMock.mockResolvedValue(null);
  pickUpPrewarmDayBaseMock.mockResolvedValue(false);
  claimContainerSlotMock.mockResolvedValue({ proceed: true });
  releaseContainerSlotMock.mockResolvedValue(undefined);
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
  ).resolves.toBe(false);
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
  ).resolves.toBe(false);
  expect(containerDoFetchMock).toHaveBeenCalledTimes(2);
  const firstCall = containerDoFetchMock.mock.calls[0];
  if (firstCall === undefined) throw new Error("expected first fetch");
  const firstRequest = firstCall[0];
  if (!isRequest(firstRequest)) throw new Error("expected first Request");
  const secondCall = containerDoFetchMock.mock.calls[1];
  if (secondCall === undefined) throw new Error("expected second fetch");
  const secondRequest = secondCall[0];
  if (!isRequest(secondRequest)) throw new Error("expected second Request");
  expect(firstRequest.url).toBe(
    "http://do/prewarm-day-base?category=jra&daysAhead=2&runDate=20260628",
  );
  expect(secondRequest.url).toBe(
    "http://do/prewarm-day-base?category=nar&daysAhead=2&runDate=20260628",
  );
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=jra runYmd=20260628: Error: boom",
  );
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] success category=nar runYmd=20260628 status=success parquetKey=nar/20260628/day-base.parquet watermark=absent error=-",
  );
  errorSpy.mockRestore();
  logSpy.mockRestore();
});

test("prewarmCategory still builds when FEATURES_CACHE has an unwatermarked object", async () => {
  headDayBaseObjectMock.mockResolvedValueOnce(null);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "ban-ei",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260816",
  });
  expect(landed).toBe(false);
  expect(containerDoFetchMock).toHaveBeenCalledTimes(1);
  warnSpy.mockRestore();
});

test("prewarmCategory does not trust a merely present FEATURES_CACHE object", async () => {
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 87257 });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "ban-ei",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260816",
  });
  expect(landed).toBe(false);
  expect(containerDoFetchMock).toHaveBeenCalledTimes(1);
  logSpy.mockRestore();
});

test("prewarmCategory requests the category-scoped DO with category, daysAhead, and runDate query params", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 3, env: makeEnv(), runYmd: "20260701" });
  expect(containerDoIdFromNameMock).toHaveBeenCalledWith("predict-jra");
  const firstCall = containerDoFetchMock.mock.calls[0];
  if (firstCall === undefined) throw new Error("expected a fetch");
  const request = firstCall[0];
  if (!isRequest(request)) throw new Error("expected a Request");
  expect(request.url).toBe("http://do/prewarm-day-base?category=jra&daysAhead=3&runDate=20260701");
  logSpy.mockRestore();
  warnSpy.mockRestore();
});

test("prewarmCategory logs a started outcome when the container returns status accepted with a parquet key", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "jra",
          parquetKey: "feat-daybase/catalog-v1/jra/20260628/features.parquet",
          runDate: "20260628",
          status: "accepted",
        }),
        { status: 200 },
      ),
    ),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] started category=jra runYmd=20260628 status=accepted parquetKey=feat-daybase/catalog-v1/jra/20260628/features.parquet watermark=absent error=-",
  );
  logSpy.mockRestore();
});

test("prewarmCategory logs a failed outcome when status is accepted but parquetKey is missing", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(resultLineBody({ category: "jra", runDate: "20260628", status: "accepted" }), {
        status: 200,
      }),
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=jra runYmd=20260628 status=accepted parquetKey=- watermark=absent error=-",
  );
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
    "[day-base-prewarm] success category=jra runYmd=20260628 status=success parquetKey=jra/20260628/day-base.parquet watermark=absent error=-",
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
    "[day-base-prewarm] empty category=nar runYmd=20260628 status=empty parquetKey=- watermark=absent error=-",
  );
  logSpy.mockRestore();
});

test("prewarmCategory logs watermark=present when the result line carries a daybaseWatermark", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        `${JSON.stringify({
          category: "jra",
          daybaseWatermark: {
            maxDataSakuseiNengappi: "20260712",
            rowCount: 946,
            rsPredictedAtMax: "2026-07-18T09:00:00",
            rsRowCount: 12,
          },
          parquetKey: "jra/20260628/day-base.parquet",
          runDate: "20260628",
          status: "success",
          type: "result",
        })}\n`,
        { status: 200 },
      ),
    ),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] success category=jra runYmd=20260628 status=success parquetKey=jra/20260628/day-base.parquet watermark=present error=-",
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
    "[day-base-prewarm] failed category=jra runYmd=20260628 status=error parquetKey=- watermark=absent error=boom",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory releases the day-base slot after a non-ok HTTP response", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response("server error", { status: 500 })),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "ban-ei",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(releaseContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-ban-ei",
    env: expect.any(Object),
    kind: "day-base",
  });
  warnSpy.mockRestore();
});

test("prewarmCategory logs a warning and does not throw for a non-ok HTTP response", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response("server error", { status: 500 })),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await expect(
    prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBe(false);
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
  ).resolves.toBe(false);
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] non-ok response category=jra runYmd=20260628 status=200",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory logs a warning when status is success but parquetKey is missing", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(resultLineBody({ category: "jra", runDate: "20260628", status: "success" }), {
        status: 200,
      }),
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=jra runYmd=20260628 status=success parquetKey=- watermark=absent error=-",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory logs a warning when status is success but parquetKey is blank", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "jra",
          parquetKey: "   ",
          runDate: "20260628",
          status: "success",
        }),
        { status: 200 },
      ),
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=jra runYmd=20260628 status=success parquetKey=    watermark=absent error=-",
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
    "[day-base-prewarm] failed category=jra runYmd=20260628 status=- parquetKey=- watermark=absent error=-",
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
  ).resolves.toBe(false);
  expect(errorSpy).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("prewarmCategory catches and logs without throwing when the DO stub fetch rejects", async () => {
  containerDoFetchMock.mockImplementation(() => Promise.reject(new Error("network down")));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await expect(
    prewarmCategory({ category: "ban-ei", daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBe(false);
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed category=ban-ei runYmd=20260628: Error: network down",
  );
  errorSpy.mockRestore();
});

test("prewarmCategory logs missing-object when pickup does not land the day-base object", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "jra",
          parquetKey: "jra/20260628/day-base.parquet",
          runDate: "20260628",
          status: "accepted",
        }),
        { status: 200 },
      ),
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(pickUpPrewarmDayBaseMock).toHaveBeenCalledTimes(1);
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] pickup-scheduled category=jra runYmd=20260628 status=missing-object parquetKey=feat-daybase/catalog-v1/jra/20260628/features.parquet watermark=absent error=day-base object missing after prewarm",
  );
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "jra",
      runYmd: "20260628",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  warnSpy.mockRestore();
  logSpy.mockRestore();
});

test("prewarmCategory picks up a detached payload after container freshness validation", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "ban-ei",
          parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
          runDate: "20260816",
          status: "accepted",
        }),
        { status: 200 },
      ),
    ),
  );
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 87257 });
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "ban-ei",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260816",
  });
  expect(landed).toBe(true);
  expect(pickUpPrewarmDayBaseMock).toHaveBeenCalledTimes(1);
  expect(containerDoFetchMock).toHaveBeenCalledTimes(1);
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] started category=ban-ei runYmd=20260816 status=accepted parquetKey=feat-daybase/catalog-v1/ban-ei/20260816/features.parquet watermark=absent error=-",
  );
  logSpy.mockRestore();
});

test("prewarmCategory returns true after a started build then pickup lands the object", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "ban-ei",
          parquetKey: "feat-daybase/catalog-v1/ban-ei/20260816/features.parquet",
          runDate: "20260816",
          status: "accepted",
        }),
        { status: 200 },
      ),
    ),
  );
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 87257 });
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "ban-ei",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260816",
  });
  expect(landed).toBe(true);
  expect(pickUpPrewarmDayBaseMock).toHaveBeenCalledTimes(1);
  expect(containerDoFetchMock).toHaveBeenCalledTimes(1);
  logSpy.mockRestore();
});

test("runDayBasePrewarm revalidates every category even when day-base objects are present", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "jra", keibajoCode: "05", raceBango: "01" },
    { category: "nar", keibajoCode: "44", raceBango: "01" },
  ]);
  headDayBaseObjectMock.mockResolvedValue({ size: 1 });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(
    runDayBasePrewarm({ daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBe(false);
  expect(containerDoFetchMock).toHaveBeenCalledTimes(2);
  expect(pickUpPrewarmDayBaseMock).toHaveBeenCalledTimes(2);
  logSpy.mockRestore();
});

test("prewarmCategory logs unparseable when the last ndjson line is JSON null", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response("null\n", { status: 200 })),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] unparseable result category=jra runYmd=20260628",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory logs unparseable when the last ndjson line has a non-string type", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(new Response('{"type":1}\n', { status: 200 })),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "jra", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] unparseable result category=jra runYmd=20260628",
  );
  warnSpy.mockRestore();
});

test("pickUpPrewarmDayBase is invoked after a container prewarm when the object is missing", async () => {
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "nar",
          parquetKey: "nar/20260628/day-base.parquet",
          runDate: "20260628",
          status: "empty",
        }),
        { status: 200 },
      ),
    ),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({ category: "nar", daysAhead: 2, env: makeEnv(), runYmd: "20260628" });
  expect(pickUpPrewarmDayBaseMock).toHaveBeenCalledTimes(1);
  logSpy.mockRestore();
  warnSpy.mockRestore();
});

test("prewarmCategory logs capped when the slot claim omits state", async () => {
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "nar",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] container slot capped doName=predict-nar kind=day-base category=nar runYmd=20260628 -- skipping start",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory skips the container start when the slot is capped", async () => {
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "capped" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] container slot capped doName=predict-jra kind=day-base category=jra runYmd=20260628 -- skipping start",
  );
  warnSpy.mockRestore();
});

test("prewarmCategory claims a Ban-ei reserved day-base slot before starting", async () => {
  await prewarmCategory({
    category: "ban-ei",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(claimContainerSlotMock).toHaveBeenCalledWith({
    category: "ban-ei",
    doName: "predict-ban-ei",
    env: expect.any(Object),
    kind: "day-base",
    staleAfterMs: 3_600_000,
  });
});

test("prewarmCategory releases the day-base slot after pickup lands the object", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 12 });
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "jra",
          parquetKey: "jra/20260628/day-base.parquet",
          runDate: "20260628",
          status: "accepted",
        }),
        { status: 200 },
      ),
    ),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(true);
  expect(releaseContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra",
    env: expect.any(Object),
    kind: "day-base",
  });
  logSpy.mockRestore();
});

test("prewarmCategory releases the day-base slot when the container fetch throws", async () => {
  containerDoFetchMock.mockImplementation(() => Promise.reject(new Error("boom")));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "nar",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(releaseContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-nar",
    env: expect.any(Object),
    kind: "day-base",
  });
  errorSpy.mockRestore();
});

test("prewarmCategory keeps the Ban-ei day-base slot while accepted pickup is pending", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(false);
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(false);
  headDayBaseObjectMock.mockResolvedValue(null);
  containerDoFetchMock.mockImplementation(() =>
    Promise.resolve(
      new Response(
        resultLineBody({
          category: "ban-ei",
          parquetKey: "ban-ei/20260628/day-base.parquet",
          runDate: "20260628",
          status: "accepted",
        }),
        { status: 200 },
      ),
    ),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "ban-ei",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("prewarmCategory still reports landed when slot release fails after pickup", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(false);
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 12 });
  releaseContainerSlotMock.mockRejectedValueOnce(new Error("release failed"));
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
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(true);
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed to release container slot category=jra doName=predict-jra: Error: release failed",
  );
  logSpy.mockRestore();
  errorSpy.mockRestore();
});

test("prewarmCategory logs a release failure without throwing", async () => {
  containerDoFetchMock.mockImplementation(() => Promise.reject(new Error("boom")));
  releaseContainerSlotMock.mockRejectedValueOnce(new Error("release failed"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed to release container slot category=jra doName=predict-jra: Error: release failed",
  );
  errorSpy.mockRestore();
});
