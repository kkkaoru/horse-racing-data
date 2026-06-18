// Run with bun. Tests for the Worker fetch (health + on-demand trigger) +
// scheduled handlers with mocked Container binding and D1.

import { beforeEach, expect, test, vi } from "vitest";

const { startMock, getContainerMock, warmNeonMock, enqueueMock, handleQueueMock } = vi.hoisted(
  () => {
    const start = vi.fn(async () => undefined);
    const warmNeon = vi.fn(async () => undefined);
    const enqueuePredict = vi.fn(async () => ["jra", "nar", "ban-ei"]);
    const handleQueue = vi.fn(async () => undefined);
    return {
      getContainerMock: vi.fn(() => ({ start })),
      startMock: start,
      warmNeonMock: warmNeon,
      enqueueMock: enqueuePredict,
      handleQueueMock: handleQueue,
    };
  },
);

vi.mock("@cloudflare/containers", () => ({
  Container: class {},
  getContainer: getContainerMock,
}));

vi.mock("./neon-warm", () => ({
  warmNeon: warmNeonMock,
}));

vi.mock("./queue-producer", () => ({ enqueuePredict: enqueueMock }));

vi.mock("./queue-consumer", () => ({ handleQueue: handleQueueMock }));

import workerDefault, { handleFetch, handleScheduled } from "./worker";
import type { Env } from "./types";

const runMock = vi.fn(async () => ({ success: true }));
const bindMock = vi.fn(() => ({ run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
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
  enqueueMock.mockResolvedValue(["jra", "nar", "ban-ei"]);
});

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
