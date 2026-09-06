import { expect, test, vi } from "vitest";
import {
  triggerWorkerFallback,
  waitForWorkerFallback,
  type TriggerResult,
  type WorkerFallbackFetch,
} from "./worker-fallback";

test("triggers the provider-specific daily Worker path", async () => {
  const fetcher = vi.fn<WorkerFallbackFetch>(async (request, init) => {
    expect(String(request)).toBe("https://daily.example/admin/trigger");
    expect(init).toMatchObject({
      body: '{"action":"run","force":true,"provider":"nv"}',
      headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
      method: "POST",
    });
    return Response.json({
      action: "run",
      provider: "nv",
      runDate: "20260904",
      runId: "run-1",
    });
  });

  await expect(
    triggerWorkerFallback("nv", "https://daily.example", "admin", fetcher),
  ).resolves.toStrictEqual({
    action: "run",
    provider: "nv",
    runDate: "20260904",
    runId: "run-1",
  });
});

test("requires the Worker URL and admin token", async () => {
  await expect(triggerWorkerFallback("jv", undefined, "admin")).rejects.toThrow(
    "DAILY_KEIBA_SYNC_BASE_URL",
  );
  await expect(triggerWorkerFallback("jv", "", "admin")).rejects.toThrow(
    "DAILY_KEIBA_SYNC_BASE_URL",
  );
  await expect(triggerWorkerFallback("jv", "https://daily.example", undefined)).rejects.toThrow(
    "DAILY_KEIBA_SYNC_ADMIN_TOKEN",
  );
  await expect(triggerWorkerFallback("jv", "https://daily.example", "")).rejects.toThrow(
    "DAILY_KEIBA_SYNC_ADMIN_TOKEN",
  );
});

test("reports only a safe HTTP status for Worker failures", async () => {
  const fetcher = vi.fn<WorkerFallbackFetch>(async () => new Response("private", { status: 502 }));
  await expect(
    triggerWorkerFallback("jv", "https://daily.example", "admin", fetcher),
  ).rejects.toThrow("daily-keiba-sync trigger failed with HTTP 502");
});

const result: TriggerResult = {
  action: "run",
  provider: "nv",
  runDate: "20260904",
  runId: "run-1",
};

test("waits for the exact triggered run to succeed", async () => {
  const fetcher = vi
    .fn<WorkerFallbackFetch>()
    .mockResolvedValueOnce(
      Response.json({ error_stage: null, run_id: "newer-run", status: "queued" }),
    )
    .mockResolvedValueOnce(
      Response.json({ error_stage: null, run_id: "run-1", status: "succeeded_empty" }),
    );
  const delay = vi.fn(async () => undefined);

  await expect(
    waitForWorkerFallback(result, "https://daily.example", "admin", fetcher, delay, 2),
  ).resolves.toBeUndefined();
  expect(delay).toHaveBeenCalledWith(5_000);
  expect(String(fetcher.mock.calls[0]?.[0])).toBe(
    "https://daily.example/admin/status?provider=nv&runDate=20260904",
  );
});

test("reports safe terminal and status failures", async () => {
  await expect(
    waitForWorkerFallback(
      result,
      "https://daily.example",
      "admin",
      vi.fn<WorkerFallbackFetch>(async () =>
        Response.json({ error_stage: "neon-schema", run_id: "run-1", status: "neon_failed" }),
      ),
    ),
  ).rejects.toThrow("run failed at neon-schema");
  await expect(
    waitForWorkerFallback(
      result,
      "https://daily.example",
      "admin",
      vi.fn<WorkerFallbackFetch>(async () => new Response(null, { status: 502 })),
    ),
  ).rejects.toThrow("status failed with HTTP 502");
});

test("rejects malformed status responses and times out after bounded polling", async () => {
  await expect(
    waitForWorkerFallback(
      result,
      "https://daily.example",
      "admin",
      vi.fn<WorkerFallbackFetch>(async () => Response.json({ status: "queued" })),
    ),
  ).rejects.toThrow("invalid response");
  await expect(
    waitForWorkerFallback(
      result,
      "https://daily.example",
      "admin",
      vi.fn<WorkerFallbackFetch>(async () => new Response(null, { status: 404 })),
      async () => undefined,
      1,
    ),
  ).rejects.toThrow("fallback timeout");
});

test.each([
  null,
  [],
  {},
  { action: "monitor", provider: "jv", runDate: "20260904", runId: "run-1" },
  { action: "run", provider: "nv", runDate: "20260904", runId: "run-1" },
  { action: "run", provider: "jv", runDate: 20260904, runId: "run-1" },
  { action: "run", provider: "jv", runDate: "invalid", runId: "run-1" },
  { action: "run", provider: "jv", runDate: "20260904", runId: 1 },
  { action: "run", provider: "jv", runDate: "20260904", runId: "" },
])("rejects a malformed successful response", async (value) => {
  const fetcher = vi.fn<WorkerFallbackFetch>(async () => Response.json(value));
  await expect(
    triggerWorkerFallback("jv", "https://daily.example", "admin", fetcher),
  ).rejects.toThrow("invalid response");
});
