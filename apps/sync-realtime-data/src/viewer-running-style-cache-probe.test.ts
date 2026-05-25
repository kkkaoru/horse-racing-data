// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { RunningStyleInferenceRace } from "./running-style-d1";
import type { Env } from "./types";

vi.mock("./d1-query-cache", () => ({
  readD1QueryCache: vi.fn(),
}));

interface CacheLike {
  match: (request: Request) => Promise<Response | undefined>;
}

interface CachesGlobal {
  default?: CacheLike;
}

const RACE: RunningStyleInferenceRace = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0512",
  keibajoCode: "08",
  raceBango: "01",
  raceKey: "jra:2026:0512:08:01",
  source: "jra",
};

const originalCaches = (globalThis as { caches?: CachesGlobal }).caches;

beforeEach(() => {
  delete (globalThis as { caches?: CachesGlobal }).caches;
});

afterEach(() => {
  vi.restoreAllMocks();
  if (originalCaches === undefined) {
    delete (globalThis as { caches?: CachesGlobal }).caches;
  } else {
    (globalThis as { caches?: CachesGlobal }).caches = originalCaches;
  }
});

it("returns true when D1 query cache has rows", async () => {
  const { isViewerRunningStyleRaceCacheReady } = await import("./viewer-running-style-cache-probe");
  const { readD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(readD1QueryCache).mockResolvedValue([{ horseNumber: 1 }]);
  const env = {} as unknown as Env;
  expect(await isViewerRunningStyleRaceCacheReady(env, RACE)).toBe(true);
});

it("returns false when D1 cache empty and caches global unavailable", async () => {
  const { isViewerRunningStyleRaceCacheReady } = await import("./viewer-running-style-cache-probe");
  const { readD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(readD1QueryCache).mockResolvedValue(null);
  const env = {} as unknown as Env;
  expect(await isViewerRunningStyleRaceCacheReady(env, RACE)).toBe(false);
});

it("returns false when D1 cache empty and ttl is zero", async () => {
  const { isViewerRunningStyleRaceCacheReady } = await import("./viewer-running-style-cache-probe");
  const { readD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(readD1QueryCache).mockResolvedValue(null);
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-13T00:00:01+09:00"));
  const match = vi.fn(async () => undefined);
  (globalThis as { caches?: CachesGlobal }).caches = { default: { match } };
  const env = {} as unknown as Env;
  expect(await isViewerRunningStyleRaceCacheReady(env, RACE)).toBe(false);
  expect(match).not.toHaveBeenCalled();
});

it("returns true when D1 cache empty and url cache match is ok", async () => {
  const { isViewerRunningStyleRaceCacheReady } = await import("./viewer-running-style-cache-probe");
  const { readD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(readD1QueryCache).mockResolvedValue([]);
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-12T10:00:00+09:00"));
  const match = vi.fn(async () => new Response(null, { status: 200 }));
  (globalThis as { caches?: CachesGlobal }).caches = { default: { match } };
  const env = {} as unknown as Env;
  expect(await isViewerRunningStyleRaceCacheReady(env, RACE)).toBe(true);
});

it("returns false when D1 cache empty and url cache match is not ok", async () => {
  const { isViewerRunningStyleRaceCacheReady } = await import("./viewer-running-style-cache-probe");
  const { readD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(readD1QueryCache).mockResolvedValue([]);
  vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-12T10:00:00+09:00"));
  const match = vi.fn(async () => new Response(null, { status: 404 }));
  (globalThis as { caches?: CachesGlobal }).caches = { default: { match } };
  const env = {} as unknown as Env;
  expect(await isViewerRunningStyleRaceCacheReady(env, RACE)).toBe(false);
});
