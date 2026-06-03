// Run with bun. Tests for the Worker fetch (health + on-demand trigger) +
// scheduled handlers with mocked Container binding and D1.

import { beforeEach, expect, test, vi } from "vitest";

const { startMock, getContainerMock } = vi.hoisted(() => {
  const start = vi.fn(async () => undefined);
  return { getContainerMock: vi.fn(() => ({ start })), startMock: start };
});

vi.mock("@cloudflare/containers", () => ({
  Container: class {},
  getContainer: getContainerMock,
}));

import workerDefault, { handleFetch, handleScheduled } from "./worker";
import type { Env } from "./types";

const runMock = vi.fn(async () => ({ success: true }));
const bindMock = vi.fn(() => ({ run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));

const makeEnv = (): Env => ({
  FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
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
  expect(startMock).not.toHaveBeenCalled();
});

test("handleFetch rejects a wrong-token trigger with 401", async () => {
  const response = await handleFetch(triggerRequest("wrong-token", ""), makeEnv());
  expect(response.status).toBe(401);
  expect(startMock).not.toHaveBeenCalled();
});

test("handleFetch starts the container for an authorized explicit RUN_DATE", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean; runDate: string };
  expect(body.ok).toBe(true);
  expect(body.runDate).toBe("2026-06-03");
  expect(startMock).toHaveBeenCalledTimes(1);
});

test("handleFetch writes a started audit row for an authorized trigger", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(prepareMock).toHaveBeenCalledTimes(1);
  expect(runMock).toHaveBeenCalledTimes(1);
});

test("handleFetch defaults to today's JST date when the body omits runDate", async () => {
  const response = await handleFetch(triggerRequest("secret-token", ""), makeEnv());
  expect(response.status).toBe(200);
  expect(startMock).toHaveBeenCalledTimes(1);
});

test("handleFetch returns 400 for a malformed RUN_DATE", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "2026-06-03" })),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(startMock).not.toHaveBeenCalled();
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
