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
  controlSendMock,
  enumerateTodaysRacesMock,
  fanOutPredictionsAfterDayBaseHitMock,
  getFocusedFullDayBaseReadinessMock,
  headDayBaseObjectMock,
  pickUpPrewarmDayBaseMock,
  releaseContainerSlotMock,
} = vi.hoisted(() => ({
  claimContainerSlotMock: vi.fn(
    async (): Promise<{ proceed: boolean; state?: string }> => ({ proceed: true }),
  ),
  controlSendMock: vi.fn(async () => undefined),
  enumerateTodaysRacesMock: vi.fn(async (): Promise<RaceEntry[]> => []),
  fanOutPredictionsAfterDayBaseHitMock: vi.fn(async (): Promise<number> => 1),
  getFocusedFullDayBaseReadinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
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
vi.mock("./feature-hit-prediction", () => ({
  fanOutPredictionsAfterDayBaseHit: fanOutPredictionsAfterDayBaseHitMock,
}));
vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: getFocusedFullDayBaseReadinessMock,
}));

import {
  enqueueDayBasePrewarm,
  isDayBasePrewarmMessage,
  isDayBasePrewarmQueueMessage,
  prewarmCategory,
  runDayBasePrewarm,
} from "./day-base-prewarm";
import type { Env } from "./types";

const containerDoFetchMock = vi.fn((_request: Request) =>
  Promise.resolve(new Response("", { status: 200 })),
);
const containerDoGetMock = vi.fn(() => ({ fetch: containerDoFetchMock }));
const containerDoIdFromNameMock = vi.fn((name: string) => ({ name }));
const queueSendMock = vi.fn(async () => undefined);

const makeEnv = (): Env => ({
  CONTAINER_CONTROL_QUEUE: {
    send: controlSendMock,
  } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
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

test("validates day-base prewarm messages and queue wrappers", () => {
  const valid = {
    category: "jra",
    daysAhead: 0,
    requestedAt: "2026-08-22T00:00:00.000Z",
    runYmd: "20260822",
    type: "day-base-prewarm",
  };
  expect(isDayBasePrewarmMessage(valid)).toBe(true);
  expect(isDayBasePrewarmMessage({ ...valid, force: true })).toBe(true);
  expect(isDayBasePrewarmQueueMessage({ body: valid } as Message<unknown>)).toBe(true);
  for (const invalid of [
    null,
    {},
    { ...valid, type: "other" },
    { ...valid, category: "overseas" },
    { ...valid, runYmd: 20260822 },
    { ...valid, runYmd: "2026-08-22" },
    { ...valid, daysAhead: "0" },
    { ...valid, daysAhead: Number.NaN },
    { ...valid, requestedAt: 1 },
    { ...valid, generatePredictionsAfterHit: "yes" },
    { ...valid, force: "yes" },
  ]) {
    expect(isDayBasePrewarmMessage(invalid)).toBe(false);
  }
});

test("enqueueDayBasePrewarm preserves generation intent and explicit request time", async () => {
  const env = makeEnv();
  await enqueueDayBasePrewarm({
    category: "ban-ei",
    daysAhead: 0,
    env,
    force: true,
    generatePredictionsAfterHit: true,
    requestedAt: new Date("2026-08-22T00:00:00.000Z"),
    runYmd: "20260822",
  });
  expect(queueSendMock).toHaveBeenCalledWith({
    category: "ban-ei",
    daysAhead: 0,
    force: true,
    generatePredictionsAfterHit: true,
    requestedAt: "2026-08-22T00:00:00.000Z",
    runYmd: "20260822",
    type: "day-base-prewarm",
  });
});

beforeEach(() => {
  enumerateTodaysRacesMock.mockClear();
  fanOutPredictionsAfterDayBaseHitMock.mockClear();
  getFocusedFullDayBaseReadinessMock.mockReset();
  getFocusedFullDayBaseReadinessMock.mockResolvedValue({ ready: true, reason: "ready" });
  containerDoFetchMock.mockClear();
  containerDoGetMock.mockClear();
  containerDoIdFromNameMock.mockClear();
  queueSendMock.mockReset();
  queueSendMock.mockResolvedValue(undefined);
  headDayBaseObjectMock.mockReset();
  pickUpPrewarmDayBaseMock.mockReset();
  claimContainerSlotMock.mockClear();
  controlSendMock.mockReset();
  controlSendMock.mockResolvedValue(undefined);
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
  expect(queueSendMock).toHaveBeenCalledTimes(3);
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "jra", type: "day-base-prewarm" }),
  );
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "nar", type: "day-base-prewarm" }),
  );
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "ban-ei", type: "day-base-prewarm" }),
  );
  expect(containerDoFetchMock).not.toHaveBeenCalled();
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

test("runDayBasePrewarm continues enqueueing other categories when one queue send rejects", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "jra", keibajoCode: "05", raceBango: "01" },
    { category: "nar", keibajoCode: "44", raceBango: "01" },
  ]);
  queueSendMock.mockRejectedValueOnce(new Error("boom")).mockResolvedValueOnce(undefined);
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(
    runDayBasePrewarm({ daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBe(false);
  expect(queueSendMock).toHaveBeenCalledTimes(2);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] enqueue failed category=jra runYmd=20260628: Error: boom",
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

test("prewarmCategory fans out predictions only when a fresh HIT and generation intent coincide", async () => {
  containerDoFetchMock.mockResolvedValue(
    new Response(
      resultLineBody({
        category: "jra",
        parquetKey: "jra/20260822/day-base.parquet",
        runDate: "20260822",
        status: "success",
      }),
      { status: 200 },
    ),
  );
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await prewarmCategory({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    generatePredictionsAfterHit: true,
    runYmd: "20260822",
  });
  expect(fanOutPredictionsAfterDayBaseHitMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    runYmd: "20260822",
  });
  logSpy.mockRestore();
});

test("prewarmCategory arms a pickup when a feature-hit request finds the category slot busy", async () => {
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "leased" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await prewarmCategory({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    generatePredictionsAfterHit: true,
    runYmd: "20260822",
  });
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "jra",
      generatePredictionsAfterHit: true,
      runYmd: "20260822",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  warnSpy.mockRestore();
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

test("prewarmCategory stops the day-base container after a non-ok HTTP response", async () => {
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
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-ban-ei",
      type: "container-stop",
      workKey: "day-base:20260628:ban-ei",
    }),
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

test("runDayBasePrewarm queues every category without directly reading or starting containers", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "jra", keibajoCode: "05", raceBango: "01" },
    { category: "nar", keibajoCode: "44", raceBango: "01" },
  ]);
  headDayBaseObjectMock.mockResolvedValue({ size: 1 });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(
    runDayBasePrewarm({ daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBe(true);
  expect(queueSendMock).toHaveBeenCalledTimes(2);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(pickUpPrewarmDayBaseMock).not.toHaveBeenCalled();
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
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "jra",
      runYmd: "20260628",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
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
    workKey: "day-base:20260628:ban-ei",
  });
});

test("prewarmCategory stops the day-base container after pickup lands the object", async () => {
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
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra",
      type: "container-stop",
      workKey: "day-base:20260628:jra",
    }),
  );
  logSpy.mockRestore();
});

test("prewarmCategory stops the day-base container when the container fetch throws", async () => {
  containerDoFetchMock.mockImplementation(() => Promise.reject(new Error("boom")));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "nar",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-nar",
      type: "container-stop",
      workKey: "day-base:20260628:nar",
    }),
  );
  errorSpy.mockRestore();
});

test("prewarmCategory releases only the slot when setup fails before Container start", async () => {
  containerDoIdFromNameMock.mockImplementationOnce(() => {
    throw new Error("DO setup failed");
  });
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await expect(
    prewarmCategory({ category: "nar", daysAhead: 2, env: makeEnv(), runYmd: "20260628" }),
  ).resolves.toBe(false);

  expect(releaseContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-nar",
    env: expect.any(Object),
    kind: "day-base",
    workKey: "day-base:20260628:nar",
  });
  expect(controlSendMock).not.toHaveBeenCalled();
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

test("prewarmCategory still reports landed when direct stop falls back to cleanup", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(false);
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 12 });
  controlSendMock.mockRejectedValueOnce(new Error("control unavailable"));
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
    "[container-cleanup] stop enqueue failed name=predict-jra role=legacy workKey=day-base:20260628:jra:",
    "Error: control unavailable",
  );
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ name: "predict-jra", type: "container-cleanup" }),
    { delaySeconds: 30 },
  );
  logSpy.mockRestore();
  errorSpy.mockRestore();
});

test("prewarmCategory logs a cleanup handoff failure without throwing", async () => {
  containerDoFetchMock.mockImplementation(() => Promise.reject(new Error("boom")));
  controlSendMock.mockRejectedValueOnce(new Error("control unavailable"));
  queueSendMock.mockRejectedValueOnce(new Error("cleanup unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const landed = await prewarmCategory({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    runYmd: "20260628",
  });
  expect(landed).toBe(false);
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-prewarm] failed to hand off container cleanup category=jra doName=predict-jra: Error: cleanup unavailable",
  );
  errorSpy.mockRestore();
});

test("prewarmCategory fails closed when a landed build cannot schedule terminal cleanup", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(false);
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 12 });
  controlSendMock.mockRejectedValue(new Error("control unavailable"));
  queueSendMock.mockRejectedValue(new Error("cleanup unavailable"));
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

  await expect(
    prewarmCategory({
      category: "jra",
      daysAhead: 2,
      env: makeEnv(),
      runYmd: "20260628",
    }),
  ).rejects.toThrow("cleanup unavailable");

  expect(fanOutPredictionsAfterDayBaseHitMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
  logSpy.mockRestore();
});
