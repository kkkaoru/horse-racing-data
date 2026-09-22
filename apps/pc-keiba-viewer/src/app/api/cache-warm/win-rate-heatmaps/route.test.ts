// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  createBatchMock: vi.fn<(batch: unknown[]) => Promise<unknown[]>>(),
  getRacesByDateMock: vi.fn<(year: string, month: string, day: string) => Promise<unknown[]>>(),
  safeGetCloudflareEnvMock: vi.fn<() => Promise<unknown>>(),
}));

vi.mock("../../../../db/queries", () => ({
  getRacesByDate: mocks.getRacesByDateMock,
}));

vi.mock("../../../../lib/cloudflare-context.server", () => ({
  safeGetCloudflareEnv: mocks.safeGetCloudflareEnvMock,
}));

const { createBatchMock, getRacesByDateMock, safeGetCloudflareEnvMock } = mocks;

import { POST } from "./route";

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-09-22T23:00:00.000Z"));
  createBatchMock.mockReset();
  createBatchMock.mockResolvedValue([]);
  getRacesByDateMock.mockReset();
  safeGetCloudflareEnvMock.mockReset();
});

it("rejects requests without the scheduled header", async () => {
  const response = await POST(
    new Request("https://example.com/api/cache-warm/win-rate-heatmaps", { method: "POST" }),
  );
  expect(response.status).toBe(404);
  expect(getRacesByDateMock).not.toHaveBeenCalled();
});

it("returns 503 when the workflow binding is missing", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue({});
  const response = await POST(
    new Request("https://example.com/api/cache-warm/win-rate-heatmaps?date=2026-09-23", {
      headers: { "X-PC-Keiba-Cache-Warm": "scheduled" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({
    date: "2026-09-23",
    error: "HEATMAP_WARM_WORKFLOW binding is unavailable",
  });
});

it("returns 503 when the cloudflare env is unavailable", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  const response = await POST(
    new Request("https://example.com/api/cache-warm/win-rate-heatmaps?date=2026-09-23", {
      headers: { "X-PC-Keiba-Cache-Warm": "scheduled" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(503);
});

it("starts one workflow per venue for the requested date", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue({
    HEATMAP_WARM_WORKFLOW: { createBatch: createBatchMock },
  });
  getRacesByDateMock.mockResolvedValue([
    { keibajoCode: "30", raceBango: "01", source: "nar" },
    { keibajoCode: "42", raceBango: "01", source: "nar" },
  ]);
  const response = await POST(
    new Request("https://example.com/api/cache-warm/win-rate-heatmaps?date=2026-09-23", {
      headers: { "X-PC-Keiba-Cache-Warm": "scheduled" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    date: "2026-09-23",
    instanceIds: ["heatmap-20260923-30-16mqk", "heatmap-20260923-42-16mqk"],
    raceCount: 2,
  });
  expect(getRacesByDateMock).toHaveBeenCalledWith("2026", "09", "23");
  expect(createBatchMock).toHaveBeenCalledTimes(1);
});

it("defaults to today in JST", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue({
    HEATMAP_WARM_WORKFLOW: { createBatch: createBatchMock },
  });
  getRacesByDateMock.mockResolvedValue([]);
  const response = await POST(
    new Request("https://example.com/api/cache-warm/win-rate-heatmaps", {
      headers: { "X-PC-Keiba-Cache-Warm": "scheduled" },
      method: "POST",
    }),
  );
  expect(await response.json()).toStrictEqual({
    date: "2026-09-23",
    instanceIds: [],
    raceCount: 0,
  });
  expect(getRacesByDateMock).toHaveBeenCalledWith("2026", "09", "23");
});
