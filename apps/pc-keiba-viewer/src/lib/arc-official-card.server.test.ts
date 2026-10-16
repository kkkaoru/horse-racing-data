// Runs with Bun through Vitest. All network, runtime and KV operations are mocked.
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import evidence from "./__fixtures__/arc-official-card.json";
import { ARC_OFFICIAL_URL } from "./arc-official-card";
import { loadOfficialCard } from "./arc-official-card.server";

const mocks = vi.hoisted(() => ({
  runtime: vi.fn<(...args: unknown[]) => Promise<unknown>>(),
  get: vi.fn<(...args: unknown[]) => Promise<unknown>>(),
  put: vi.fn<(...args: unknown[]) => Promise<void>>(),
  parse: vi.fn<typeof import("./arc-official-card").parseOfficialCard>(),
  fetch: vi.fn<typeof fetch>(),
}));
const scope = {
  source: "jra",
  year: "2026",
  month: "10",
  day: "04",
  keibajoCode: "A8",
  raceNumber: "05",
};
const failureStages: (keyof typeof mocks)[] = ["runtime", "get", "fetch", "parse", "put"];
vi.mock("server-only", () => ({}));
vi.mock("./cloudflare-context.server", () => ({ safeGetCloudflareRuntime: mocks.runtime }));
vi.mock("./arc-official-card", async (original) => ({
  ...(await original<typeof import("./arc-official-card")>()),
  parseOfficialCard: mocks.parse,
}));
const response = (body: BodyInit | null = "body", status: number = 200): Response => {
  const value = new Response(body, { status });
  vi.spyOn(value, "url", "get").mockReturnValue(ARC_OFFICIAL_URL);
  return value;
};
beforeEach(() => {
  vi.resetAllMocks();
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-04T06:17:19.144Z"));
  vi.stubGlobal("HTMLRewriter", vi.fn<() => void>());
  vi.stubGlobal("fetch", mocks.fetch);
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  mocks.runtime.mockResolvedValue({
    env: { DETAIL_SECTION_CACHE_KV: { get: mocks.get, put: mocks.put } },
  });
  mocks.get.mockResolvedValue(null);
  mocks.put.mockResolvedValue(undefined);
  mocks.parse.mockResolvedValue(evidence.card);
  mocks.fetch.mockResolvedValue(response());
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
  vi.restoreAllMocks();
});
it("never accesses runtime for domestic races", async () => {
  expect(await loadOfficialCard({ ...scope, keibajoCode: "05" })).toBeNull();
  expect(mocks.runtime).not.toHaveBeenCalled();
  expect(mocks.fetch).not.toHaveBeenCalled();
});
it("does not fetch without the native parser", async () => {
  vi.stubGlobal("HTMLRewriter", undefined);
  expect(await loadOfficialCard(scope)).toBeNull();
  expect(mocks.runtime).not.toHaveBeenCalled();
});
it("uses a fresh provenance-checked cache without a network request", async () => {
  mocks.get.mockResolvedValue({
    html: "cached",
    retrievedAt: "2026-10-04T06:17:00.000Z",
    url: ARC_OFFICIAL_URL,
  });
  expect((await loadOfficialCard(scope))?.horses.length).toBe(16);
  expect(mocks.fetch).not.toHaveBeenCalled();
  expect(mocks.parse).toHaveBeenCalledWith({
    html: "cached",
    retrievedAt: "2026-10-04T06:17:00.000Z",
    url: "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110520260101051/9D",
  });
});
it.each([
  null,
  { html: "stale", retrievedAt: "2026-10-04T06:16:00.000Z", url: ARC_OFFICIAL_URL },
  { html: "future", retrievedAt: "2026-10-04T06:18:00.000Z", url: ARC_OFFICIAL_URL },
  { html: "wrong source", retrievedAt: "2026-10-04T06:17:00.000Z", url: "https://example.com" },
  { html: "invalid date", retrievedAt: "invalid", url: ARC_OFFICIAL_URL },
  { html: 123 },
])("fetches and validates instead of returning an invalid cached card %j", async (cached) => {
  mocks.get.mockResolvedValue(cached);
  expect((await loadOfficialCard(scope))?.horses.length).toBe(16);
  expect(mocks.fetch).toHaveBeenCalledTimes(1);
  expect(mocks.fetch.mock.calls[0]?.[1]?.redirect).toBe("manual");
  expect(mocks.parse).toHaveBeenCalledWith({
    html: "body",
    retrievedAt: "2026-10-04T06:17:19.144Z",
    url: "https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0110520260101051/9D",
  });
  expect(mocks.put.mock.calls[0]?.[2]).toStrictEqual({ expirationTtl: 60 });
});
it("still validates fresh fetched data when there is no optional KV binding", async () => {
  mocks.runtime.mockResolvedValue({ env: undefined });
  expect((await loadOfficialCard(scope))?.horses.length).toBe(16);
  expect(mocks.put).not.toHaveBeenCalled();
});
it.each(failureStages)("fails closed and records a diagnostic on %s failure", async (stage) => {
  mocks[stage].mockRejectedValue(new Error("unavailable"));
  expect(await loadOfficialCard(scope)).toBeNull();
  expect(console.warn).toHaveBeenCalledWith("arc_official_card_unavailable", {
    error: "unavailable",
  });
});
it("handles non-Error failures without leaking arbitrary values", async () => {
  mocks.runtime.mockRejectedValue({ private: "not logged" });
  expect(await loadOfficialCard(scope)).toBeNull();
  expect(console.warn).toHaveBeenCalledWith("arc_official_card_unavailable", {
    error: "Unknown official card failure",
  });
});
it.each(["http", "url", "body"])("rejects an invalid upstream response %s", async (kind) => {
  const value = response(kind === "body" ? null : "body", kind === "http" ? 503 : 200);
  if (kind === "url") vi.spyOn(value, "url", "get").mockReturnValue("https://example.com");
  mocks.fetch.mockResolvedValue(value);
  expect(await loadOfficialCard(scope)).toBeNull();
  expect(mocks.parse).not.toHaveBeenCalled();
  expect(mocks.put).not.toHaveBeenCalled();
});
it("rejects redirects without following them", async () => {
  mocks.fetch.mockResolvedValue(response("redirect", 302));
  expect(await loadOfficialCard(scope)).toBeNull();
  expect(mocks.fetch.mock.calls[0]?.[1]?.redirect).toBe("manual");
  expect(mocks.parse).not.toHaveBeenCalled();
});
it("rejects oversized response streams before parsing or storing", async () => {
  mocks.fetch.mockResolvedValue(response("x".repeat(512 * 1024 + 1)));
  expect(await loadOfficialCard(scope)).toBeNull();
  expect(mocks.parse).not.toHaveBeenCalled();
  expect(mocks.put).not.toHaveBeenCalled();
});
it("rejects invalid CP932 instead of replacing characters", async () => {
  mocks.fetch.mockResolvedValue(response(new Uint8Array([0x81])));
  expect(await loadOfficialCard(scope)).toBeNull();
  expect(mocks.parse).not.toHaveBeenCalled();
});
