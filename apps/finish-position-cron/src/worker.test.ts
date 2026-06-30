// Run with bun. Tests for the Worker fetch (health + on-demand trigger) +
// scheduled handlers with mocked Container binding and D1.

import { beforeEach, expect, test, vi } from "vitest";

const {
  startMock,
  getContainerMock,
  warmNeonMock,
  enqueueMock,
  handleQueueMock,
  coordinatorTickMock,
  claimRescoreRaceMock,
} = vi.hoisted(() => {
  const start = vi.fn(async () => undefined);
  const warmNeon = vi.fn(async () => undefined);
  const enqueuePredict = vi.fn(async (_p: Record<string, unknown>) => ["jra", "nar", "ban-ei"]);
  const handleQueue = vi.fn(async () => undefined);
  const runRaceCoordinatorTick = vi.fn(async () => []);
  const claimRescoreRace = vi.fn(async () => ({ proceed: true }));
  return {
    getContainerMock: vi.fn(() => ({ start })),
    startMock: start,
    warmNeonMock: warmNeon,
    enqueueMock: enqueuePredict,
    handleQueueMock: handleQueue,
    coordinatorTickMock: runRaceCoordinatorTick,
    claimRescoreRaceMock: claimRescoreRace,
  };
});

vi.mock("@cloudflare/containers", () => ({
  Container: class {},
  getContainer: getContainerMock,
}));

vi.mock("./neon-warm", () => ({
  warmNeon: warmNeonMock,
}));

vi.mock("./queue-producer", () => ({ enqueuePredict: enqueueMock }));

vi.mock("./queue-consumer", () => ({ handleQueue: handleQueueMock }));

vi.mock("./race-coordinator", () => ({
  DEFAULT_RESCORE_LEAD_MINUTES: 25,
  runRaceCoordinatorTick: coordinatorTickMock,
}));

vi.mock("./do-state", () => ({ claimRescoreRace: claimRescoreRaceMock }));

import workerDefault, { handleFetch, handleScheduled } from "./worker";
import type { Env } from "./types";

const runMock = vi.fn(async () => ({ success: true }));
const bindMock = vi.fn(() => ({ run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));
const predictQueueSendMock = vi.fn(async () => undefined);
const realtimeAllMock = vi.fn(async () => ({
  results: [{ keibajo_code: "05", race_bango: "11", source: "jra" }],
}));
const realtimeBindMock = vi.fn(() => ({ all: realtimeAllMock }));
const realtimePrepareMock = vi.fn(() => ({ bind: realtimeBindMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: { send: predictQueueSendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: { prepare: realtimePrepareMock } as unknown as D1Database,
  RESCORE_ENABLED: "1",
  TRIGGER_TOKEN: "secret-token",
});

const makeEvent = (cron: string): ScheduledEvent =>
  ({ cron, scheduledTime: Date.parse("2026-06-02T18:00:00.000Z") }) as ScheduledEvent;

const healthRequest = (): Request => new Request("https://cron.example/", { method: "GET" });

const triggerRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/run", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

beforeEach(() => {
  startMock.mockClear();
  getContainerMock.mockClear();
  prepareMock.mockClear();
  bindMock.mockClear();
  runMock.mockClear();
  warmNeonMock.mockClear();
  enqueueMock.mockClear();
  handleQueueMock.mockClear();
  coordinatorTickMock.mockClear();
  claimRescoreRaceMock.mockClear();
  predictQueueSendMock.mockClear();
  realtimeAllMock.mockClear();
  realtimeBindMock.mockClear();
  realtimePrepareMock.mockClear();
  enqueueMock.mockResolvedValue(["jra", "nar", "ban-ei"]);
  coordinatorTickMock.mockResolvedValue([]);
  claimRescoreRaceMock.mockResolvedValue({ proceed: true });
});

const internalRescoreRaceRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/api/internal/rescore-race", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

const silenceFeatureBuildCronLog = (): ReturnType<typeof vi.spyOn> =>
  vi.spyOn(console, "log").mockImplementation(() => undefined);

test("fetch returns a health payload for GET", async () => {
  const response = await workerDefault.fetch(healthRequest(), makeEnv());
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean; cron: string };
  expect(body.ok).toBe(true);
  expect(body.cron).toBe("0 18 * * *");
});

test("handleFetch rejects an unauthenticated trigger with 401", async () => {
  const response = await handleFetch(triggerRequest(null, ""), makeEnv());
  expect(response.status).toBe(401);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch rejects a wrong-token trigger with 401", async () => {
  const response = await handleFetch(triggerRequest("wrong-token", ""), makeEnv());
  expect(response.status).toBe(401);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch enqueues predict and returns 202 for an authorized explicit RUN_DATE", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  const body = (await response.json()) as { ok: boolean; runDate: string; queued: string[] };
  expect(body.ok).toBe(true);
  expect(body.runDate).toBe("2026-06-03");
  expect(body.queued).toStrictEqual(["jra", "nar", "ban-ei"]);
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(startMock).not.toHaveBeenCalled();
});

test("handleFetch defaults to mode full when body omits mode", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ mode: "full" }));
});

test("handleFetch passes mode rescore when body specifies mode rescore", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ mode: "rescore", runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ mode: "rescore" }));
});

test("handleFetch defaults to today's JST date when the body omits runDate", async () => {
  const response = await handleFetch(triggerRequest("secret-token", ""), makeEnv());
  expect(response.status).toBe(202);
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(startMock).not.toHaveBeenCalled();
});

test("handleFetch returns 400 for a malformed RUN_DATE", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "2026-06-03" })),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled is a no-op for an unmatched cron", async () => {
  await handleScheduled(makeEvent("*/10 * * * *"), makeEnv());
  expect(getContainerMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
});

test("handleScheduled starts the container for the configured cron", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(getContainerMock).toHaveBeenCalledTimes(1);
  expect(startMock).toHaveBeenCalledTimes(1);
});

test("handleScheduled writes a started audit row", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(prepareMock).toHaveBeenCalledTimes(1);
  expect(runMock).toHaveBeenCalledTimes(1);
});

test("scheduled default handler delegates to handleScheduled", async () => {
  await workerDefault.scheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(startMock).toHaveBeenCalledTimes(1);
});

test("handleScheduled calls warmNeon for the pre-NAR warm cron", async () => {
  await handleScheduled(makeEvent("55 17 * * *"), makeEnv());
  expect(warmNeonMock).toHaveBeenCalledTimes(1);
  expect(warmNeonMock).toHaveBeenCalledWith("postgres://example");
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled calls warmNeon for the pre-JRA warm cron", async () => {
  await handleScheduled(makeEvent("25 0 * * *"), makeEnv());
  expect(warmNeonMock).toHaveBeenCalledTimes(1);
  expect(warmNeonMock).toHaveBeenCalledWith("postgres://example");
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled calls warmNeon for the race-hours warm cron", async () => {
  await handleScheduled(makeEvent("*/30 1-11 * * *"), makeEnv());
  expect(warmNeonMock).toHaveBeenCalledTimes(1);
  expect(warmNeonMock).toHaveBeenCalledWith("postgres://example");
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled does not call warmNeon for the predict cron", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(getContainerMock).toHaveBeenCalledTimes(1);
});

test("handleScheduled enqueues rescore for RESCORE_CRON_RACE_HOURS", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ daysAhead: 0, mode: "rescore" }),
  );
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled rescore enqueue does not start container", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
});

test("handleScheduled runs the per-race coordinator for the coordinator cron", async () => {
  await handleScheduled(makeEvent("*/10 1-11 * * *"), makeEnv());
  expect(coordinatorTickMock).toHaveBeenCalledTimes(1);
  expect(coordinatorTickMock).toHaveBeenCalledWith(expect.objectContaining({ leadMinutes: 25 }));
});

test("handleScheduled coordinator cron does not start container or warm or enqueue per-category", async () => {
  await handleScheduled(makeEvent("*/10 1-11 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled does not run the coordinator for the rescore cron", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(coordinatorTickMock).not.toHaveBeenCalled();
});

test("handleScheduled skips direct full-mode enqueue for the feature-build cron", async () => {
  const logSpy = silenceFeatureBuildCronLog();
  await handleScheduled(makeEvent("30 0 * * *"), makeEnv());
  expect(enqueueMock).not.toHaveBeenCalled();
  expect(logSpy).toHaveBeenCalledWith(
    expect.stringContaining("Feature-build cron skipped; waiting for running-style completion"),
  );
  logSpy.mockRestore();
});

test("handleScheduled feature-build cron does not read today's races from REALTIME_DB", async () => {
  const logSpy = silenceFeatureBuildCronLog();
  await handleScheduled(makeEvent("30 0 * * *"), makeEnv());
  expect(realtimePrepareMock).not.toHaveBeenCalled();
  expect(realtimeBindMock).not.toHaveBeenCalled();
  logSpy.mockRestore();
});

test("handleScheduled feature-build cron does not consult realtime DB results", async () => {
  const logSpy = silenceFeatureBuildCronLog();
  await handleScheduled(makeEvent("30 0 * * *"), makeEnv());
  expect(enqueueMock).not.toHaveBeenCalled();
  expect(realtimeAllMock).not.toHaveBeenCalled();
  logSpy.mockRestore();
});

test("handleScheduled feature-build cron does not start container or warm or coordinate", async () => {
  const logSpy = silenceFeatureBuildCronLog();
  await handleScheduled(makeEvent("30 0 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(coordinatorTickMock).not.toHaveBeenCalled();
  logSpy.mockRestore();
});

test("queue default handler delegates to handleQueue", async () => {
  const batch = { messages: [] } as unknown as MessageBatch<import("./types").PredictQueueMessage>;
  await workerDefault.queue(batch, makeEnv());
  expect(handleQueueMock).toHaveBeenCalledTimes(1);
  expect(handleQueueMock).toHaveBeenCalledWith(
    batch,
    expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
  );
});

test("handleFetch passes category nar when body specifies category nar", async () => {
  enqueueMock.mockResolvedValue(["nar"]);
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ category: "nar", runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: "nar" }));
});

test("handleFetch omits category when body does not specify category", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: undefined }));
});

test("handleFetch ignores invalid category and calls enqueue without category", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ category: "invalid", runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: undefined }));
});

test("handleFetch does not write an audit row when enqueueing", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(prepareMock).not.toHaveBeenCalled();
  expect(enqueueMock).toHaveBeenCalledTimes(1);
});

test("handleFetch forwards keibajoCode and raceBango for a per-race NAR rescore", async () => {
  enqueueMock.mockResolvedValue(["nar"]);
  await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        mode: "rescore",
        raceBango: "12",
        runDate: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "nar",
      keibajoCode: "45",
      mode: "rescore",
      raceBango: "12",
    }),
  );
});

test("handleFetch forwards downstream full per-race trigger fields with skipDedup", async () => {
  enqueueMock.mockResolvedValue(["jra"]);
  const response = await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "05",
        mode: "full",
        raceBango: "11",
        runDate: "20260628",
        skipDedup: true,
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  const body = (await response.json()) as { ok: boolean; queued: string[]; runDate: string };
  expect(body).toStrictEqual({ ok: true, queued: ["jra"], runDate: "2026-06-28" });
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "jra",
      daysAhead: 2,
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      runDate: "2026-06-28",
      runYmd: "20260628",
      skipDedup: true,
    }),
  );
});

test("handleFetch trims whitespace from keibajoCode and raceBango", async () => {
  enqueueMock.mockResolvedValue(["nar"]);
  await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: " 45 ",
        mode: "rescore",
        raceBango: " 12 ",
        runDate: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "45", raceBango: "12" }),
  );
});

test("handleFetch treats a blank keibajoCode as absent", async () => {
  await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({ category: "nar", keibajoCode: "   ", raceBango: "12", runDate: "20260619" }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: undefined, raceBango: "12" }),
  );
});

test("handleFetch treats a non-string raceBango as absent", async () => {
  await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({ category: "nar", keibajoCode: "45", raceBango: 12, runDate: "20260619" }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "45", raceBango: undefined }),
  );
});

test("handleFetch passes skipDedup true when body specifies skipDedup true", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603", skipDedup: true })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ skipDedup: true }));
});

test("handleFetch omits skipDedup when body specifies skipDedup as string true", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603", skipDedup: "true" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ skipDedup: expect.anything() }),
  );
});

test("handleFetch omits skipDedup when body does not specify skipDedup", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ skipDedup: expect.anything() }),
  );
});

test("handleFetch omits per-race fields for the per-category path", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: undefined, raceBango: undefined }),
  );
});

test("internal rescore-race endpoint claims, enqueues a per-race rescore message, and returns 202", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(claimRescoreRaceMock).toHaveBeenCalledTimes(1);
  expect(claimRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "nar",
      keibajoCode: "45",
      raceBango: "12",
      runYmd: "20260619",
    }),
  );
  expect(predictQueueSendMock).toHaveBeenCalledTimes(1);
  expect(predictQueueSendMock).toHaveBeenCalledWith({
    category: "nar",
    daysAhead: 0,
    keibajoCode: "45",
    mode: "rescore",
    raceBango: "12",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
});

test("internal rescore-race endpoint response body marks claimed true when proceed", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "05",
        raceBango: "11",
        runYmd: "20260620",
      }),
    ),
    makeEnv(),
  );
  const body = (await response.json()) as { ok: boolean; claimed: boolean };
  expect(body.ok).toBe(true);
  expect(body.claimed).toBe(true);
});

test("internal rescore-race endpoint returns 200 with claimed=false on claim collision", async () => {
  claimRescoreRaceMock.mockResolvedValueOnce({ proceed: false });
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "05",
        raceBango: "11",
        runYmd: "20260620",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean; claimed: boolean };
  expect(body.ok).toBe(true);
  expect(body.claimed).toBe(false);
  expect(predictQueueSendMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 401 when authorization header is missing", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      null,
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(401);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
  expect(predictQueueSendMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 401 when bearer token mismatches", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "wrong-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(401);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint is a no-op when rescore is disabled", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    { ...makeEnv(), RESCORE_ENABLED: "0" },
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    claimed: false,
    ok: true,
    rescoreEnabled: false,
  });
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
  expect(predictQueueSendMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when category is missing", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({ keibajoCode: "45", raceBango: "12", runYmd: "20260619" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when category is invalid", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "garbage",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when keibajoCode is blank", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "   ",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when raceBango is missing", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({ category: "nar", keibajoCode: "45", runYmd: "20260619" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when runYmd is malformed", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "2026-06-19",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when body is not parseable JSON", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest("secret-token", "{not-json"),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint trims whitespace from keibajoCode and raceBango", async () => {
  await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "ban-ei",
        keibajoCode: "  83  ",
        raceBango: "  11  ",
        runYmd: "20260620",
      }),
    ),
    makeEnv(),
  );
  expect(claimRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "83", raceBango: "11" }),
  );
  expect(predictQueueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "ban-ei", keibajoCode: "83", raceBango: "11" }),
  );
});

test("non-trigger non-rescore request falls through to health response", async () => {
  const response = await handleFetch(
    new Request("https://cron.example/api/other", { method: "POST" }),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean };
  expect(body.ok).toBe(true);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});
