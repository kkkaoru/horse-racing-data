// Run with: bun run --filter pipeline-health-monitor test
import { afterEach, expect, it, vi } from "vitest";

vi.mock("./scheduled-handler", () => ({
  runScheduled: vi.fn(async () => undefined),
}));

vi.mock("./queue-handler", () => ({
  runQueue: vi.fn(async () => undefined),
}));

import { runQueue } from "./queue-handler";
import { runScheduled } from "./scheduled-handler";
import worker from "./worker";
import type { Env } from "./types";

afterEach(() => {
  vi.restoreAllMocks();
});

it("worker.fetch returns a JSON ok response", async () => {
  const response = worker.fetch();
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toBe("application/json");
  const body = (await response.json()) as { ok: boolean };
  expect(body).toStrictEqual({ ok: true });
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
