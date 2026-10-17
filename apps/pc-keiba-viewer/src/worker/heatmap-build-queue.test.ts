// Run with bun (vitest).
import { afterEach, expect, it, vi } from "vitest";

import { handleHeatmapBuildQueue, isHeatmapBuildMessage } from "./heatmap-build-queue";

const OWNER = "123e4567-e89b-42d3-a456-426614174000";

const params = {
  day: "18",
  keibajoCode: "05",
  month: "10",
  ownerToken: OWNER,
  query: "",
  raceNumbers: ["11"],
  source: "jra",
  year: "2026",
};

const compareText = (left: string, right: string): number => left.localeCompare(right);

const message = (body: unknown) => ({
  ack: vi.fn<() => void>(),
  body,
  retry: vi.fn<() => void>(),
});

afterEach(() => vi.restoreAllMocks());

it("accepts only owned single-race build messages", () => {
  expect(isHeatmapBuildMessage({ params })).toBe(true);
  expect(isHeatmapBuildMessage({ params: { ...params, ownerToken: undefined } })).toBe(false);
  expect(isHeatmapBuildMessage({ params: { ...params, raceNumbers: ["10", "11"] } })).toBe(false);
  expect(isHeatmapBuildMessage(null)).toBe(false);
  expect(isHeatmapBuildMessage([])).toBe(false);
});

it("runs the owned build through the section route and acks it", async () => {
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const fetchSelf = vi.fn<(request: Request) => Promise<Response>>(
    async () => new Response(null, { headers: { "X-Win-Rate-Heatmap-Cache": "MISS-STORED" } }),
  );
  const item = message({ params });
  await handleHeatmapBuildQueue({ messages: [item], queue: "pc-keiba-heatmap-build" }, fetchSelf);
  expect(fetchSelf).toHaveBeenCalledTimes(1);
  const request = fetchSelf.mock.calls[0]?.[0];
  expect(new URL(request?.url ?? "").searchParams.get("__heatmapOwner")).toBe(OWNER);
  expect(item.ack).toHaveBeenCalledTimes(1);
  expect(item.retry).not.toHaveBeenCalled();
  const logged = JSON.parse(String(log.mock.calls[0]?.[0]));
  expect(logged.status).toBe("stored");
  expect(logged.race).toBe("20261018:05:11");
});

it("acks failed and invalid builds instead of letting Queues redeliver a claimed owner", async () => {
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const failing = vi.fn<(request: Request) => Promise<Response>>(async () => {
    throw new Error("aborted");
  });
  const failed = message({ params });
  const rejectedWithValue = message({ params });
  const invalid = message({ params: { ...params, source: "x" } });
  await handleHeatmapBuildQueue(
    { messages: [failed, invalid], queue: "pc-keiba-heatmap-build" },
    failing,
  );
  await handleHeatmapBuildQueue(
    { messages: [rejectedWithValue], queue: "pc-keiba-heatmap-build" },
    vi.fn<(request: Request) => Promise<Response>>(() => Promise.reject("socket closed")),
  );
  expect(failed.ack).toHaveBeenCalledTimes(1);
  expect(invalid.ack).toHaveBeenCalledTimes(1);
  expect(rejectedWithValue.ack).toHaveBeenCalledTimes(1);
  const logged = log.mock.calls.map((call) => JSON.parse(String(call[0])));
  expect(logged.map((entry) => String(entry.status)).toSorted(compareText)).toStrictEqual([
    "failed: aborted",
    "failed: socket closed",
    "invalid",
  ]);
  expect(logged.find((entry) => entry.status === "invalid")?.race).toBeNull();
});
