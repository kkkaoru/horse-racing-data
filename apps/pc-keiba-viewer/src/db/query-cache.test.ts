// Run with bun; tests are executed by Vitest.
import { afterEach, expect, test, vi } from "vitest";

import { withDbQueryCache } from "./query-cache";

const mocks = vi.hoisted(() => ({
  values: new Map<string, string>(),
}));
vi.mock("./client", () => ({ getDatabaseTarget: () => "cloudflare" }));
vi.mock("./db-retry", () => ({ withDbRetry: (load: () => Promise<unknown>) => load() }));
vi.mock("../lib/cloudflare-context.server", () => ({
  safeGetCloudflareEnv: async () => ({
    DETAIL_SECTION_CACHE_KV: {
      get: async (key: string) => mocks.values.get(key) ?? null,
      put: async (key: string, value: string) => {
        mocks.values.set(key, value);
      },
    },
  }),
}));

afterEach(() => {
  mocks.values.clear();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

test("does not keep a two-race listing after the next minute's source sync", async () => {
  vi.stubGlobal("caches", undefined);
  vi.stubEnv("PC_KEIBA_DB_CACHE_TTL_SECONDS", "3600");
  const clock = vi.spyOn(Date, "now").mockReturnValue(59999);
  const load = vi
    .fn<() => Promise<{ races: number }[]>>()
    .mockResolvedValueOnce([{ races: 2 }])
    .mockResolvedValueOnce([{ races: 36 }]);
  expect(await withDbQueryCache(["getRacesByDate", "2026", "09", "06"], load)).toStrictEqual([
    { races: 2 },
  ]);
  expect(await withDbQueryCache(["getRacesByDate", "2026", "09", "06"], load)).toStrictEqual([
    { races: 2 },
  ]);
  clock.mockReturnValue(60000);
  expect(await withDbQueryCache(["getRacesByDate", "2026", "09", "06"], load)).toStrictEqual([
    { races: 36 },
  ]);
  expect(load).toHaveBeenCalledTimes(2);
});

test("retains expensive detail data across minute boundaries", async () => {
  vi.stubGlobal("caches", undefined);
  vi.stubEnv("PC_KEIBA_DB_CACHE_TTL_SECONDS", "3600");
  const clock = vi.spyOn(Date, "now").mockReturnValue(59999);
  const load = vi.fn<() => Promise<{ horses: number }[]>>().mockResolvedValue([{ horses: 16 }]);
  await withDbQueryCache(["getRaceRunners", "2026", "09", "06", "01", "01"], load);
  clock.mockReturnValue(60000);
  expect(
    await withDbQueryCache(["getRaceRunners", "2026", "09", "06", "01", "01"], load),
  ).toStrictEqual([{ horses: 16 }]);
  expect(load).toHaveBeenCalledTimes(1);
});
