// Run with: bun run --filter pipeline-health-monitor test
import { afterEach, expect, it, vi } from "vitest";

vi.mock("./scheduled-handler", () => ({
  runScheduled: vi.fn(async () => undefined),
}));

vi.mock("./queue-handler", () => ({
  runQueue: vi.fn(async () => undefined),
}));

vi.mock("./incident-state", () => ({
  acknowledgeIncident: vi.fn(),
}));

import { acknowledgeIncident } from "./incident-state";
import { runQueue } from "./queue-handler";
import { runScheduled } from "./scheduled-handler";
import worker from "./worker";
import type { Env } from "./types";

afterEach(() => {
  vi.restoreAllMocks();
});

it("worker.fetch returns a JSON ok response", async () => {
  const response = await worker.fetch(
    new Request("https://monitor.example/"),
    {} as unknown as Env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toBe("application/json");
  const body = (await response.json()) as { ok: boolean };
  expect(body).toStrictEqual({ ok: true });
});

it("worker.fetch authenticates and records incident acknowledgements", async () => {
  const env = { ALERT_ACK_TOKEN: "ack-secret" } as unknown as Env;
  const url = "https://monitor.example/api/internal/incidents/incident-1/ack";
  const unauthorized = await worker.fetch(new Request(url, { method: "POST" }), env);
  expect(unauthorized.status).toBe(401);

  vi.mocked(acknowledgeIncident).mockResolvedValue(null);
  const missing = await worker.fetch(
    new Request(url, { headers: { authorization: "Bearer ack-secret" }, method: "POST" }),
    env,
  );
  expect(missing.status).toBe(404);

  vi.mocked(acknowledgeIncident).mockResolvedValue({
    acknowledgedAt: "2026-08-15T00:00:00Z",
    closedAt: null,
    incidentId: "incident-1",
    lastSentAt: null,
    lastSeverity: null,
    lastStage: null,
    openedAt: "2026-08-15T00:00:00Z",
    sendCount: 0,
    signalKey: "key",
  });
  const acknowledged = await worker.fetch(
    new Request(url, { headers: { authorization: "Bearer ack-secret" }, method: "POST" }),
    env,
  );
  expect(acknowledged.status).toBe(200);
  await expect(acknowledged.json()).resolves.toMatchObject({ incidentId: "incident-1" });
});

it("worker.scheduled forwards a Date built from scheduledTime to runScheduled via ctx.waitUntil", () => {
  const env = {} as unknown as Env;
  const ctx = { waitUntil: vi.fn() };
  const controller = { scheduledTime: Date.parse("2026-06-28T06:00:00Z"), cron: "0 * * * *" };
  worker.scheduled(controller as never, env, ctx as never);
  expect(ctx.waitUntil).toHaveBeenCalledTimes(1);
  expect(vi.mocked(runScheduled)).toHaveBeenCalledWith({
    env,
    now: new Date("2026-06-28T06:00:00Z"),
  });
});

it("worker.queue delegates to runQueue with the same batch and env", async () => {
  const env = {} as unknown as Env;
  const batch = { messages: [] };
  await worker.queue(batch as never, env);
  expect(vi.mocked(runQueue)).toHaveBeenCalledWith({ batch, env });
});
