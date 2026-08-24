// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import { buildRaceCacheWarmMarkerKey, type RaceCacheBustRequest } from "./race-cache-bust";
import {
  markRaceCacheWarmGeneration,
  readRaceCacheWarmGeneration,
} from "./race-cache-warm-generation";

const race: RaceCacheBustRequest = {
  keibajoCode: "05",
  mmdd: "0824",
  raceBango: "07",
  source: "jra",
  year: "2026",
};

it("buildRaceCacheWarmMarkerKey scopes markers by kind and race generation key", () => {
  expect(buildRaceCacheWarmMarkerKey("race-trend", race)).toBe(
    "race-cache:warm:v1:race-trend:race-cache:gen:jra:2026:0824:05:07",
  );
});

it("readRaceCacheWarmGeneration returns null without KV", async () => {
  await expect(
    readRaceCacheWarmGeneration({ kind: "race-trend", kv: undefined, race }),
  ).resolves.toBeNull();
});

it("readRaceCacheWarmGeneration treats a matching marker as valid", async () => {
  const get = vi.fn<(key: string) => Promise<string | null>>();
  get.mockResolvedValueOnce("4").mockResolvedValueOnce("4");
  const state = await readRaceCacheWarmGeneration({
    kind: "race-detail-ssr",
    kv: {
      get,
      put: vi.fn<
        (key: string, value: string, options?: { expirationTtl?: number }) => Promise<void>
      >(),
    },
    race,
  });
  expect(state).toStrictEqual({
    generation: "4",
    markerKey: "race-cache:warm:v1:race-detail-ssr:race-cache:gen:jra:2026:0824:05:07",
    valid: true,
  });
});

it("readRaceCacheWarmGeneration uses generation zero before the first bust", async () => {
  const get = vi.fn<(key: string) => Promise<string | null>>();
  get.mockResolvedValueOnce(null).mockResolvedValueOnce(null);
  const state = await readRaceCacheWarmGeneration({
    kind: "race-trend",
    kv: {
      get,
      put: vi.fn<
        (key: string, value: string, options?: { expirationTtl?: number }) => Promise<void>
      >(),
    },
    race,
  });
  expect(state).toStrictEqual({
    generation: "0",
    markerKey: "race-cache:warm:v1:race-trend:race-cache:gen:jra:2026:0824:05:07",
    valid: false,
  });
});

it("readRaceCacheWarmGeneration fails open when KV read rejects", async () => {
  const get = vi.fn<(key: string) => Promise<string | null>>().mockRejectedValue(new Error("kv"));
  await expect(
    readRaceCacheWarmGeneration({
      kind: "race-trend",
      kv: {
        get,
        put: vi.fn<
          (key: string, value: string, options?: { expirationTtl?: number }) => Promise<void>
        >(),
      },
      race,
    }),
  ).resolves.toBeNull();
});

it("markRaceCacheWarmGeneration writes only the current generation", async () => {
  const get = vi.fn<(key: string) => Promise<string | null>>().mockResolvedValue("8");
  const put = vi
    .fn<(key: string, value: string, options?: { expirationTtl?: number }) => Promise<void>>()
    .mockResolvedValue(undefined);
  await expect(
    markRaceCacheWarmGeneration({
      generation: "8",
      kind: "race-trend",
      kv: { get, put },
      race,
    }),
  ).resolves.toBe(true);
  expect(put).toHaveBeenCalledWith(
    "race-cache:warm:v1:race-trend:race-cache:gen:jra:2026:0824:05:07",
    "8",
    { expirationTtl: 2592000 },
  );
});

it("markRaceCacheWarmGeneration rejects stale work and fails open without KV", async () => {
  const get = vi.fn<(key: string) => Promise<string | null>>().mockResolvedValue("9");
  const put = vi.fn<(key: string, value: string) => Promise<void>>();
  await expect(
    markRaceCacheWarmGeneration({
      generation: "8",
      kind: "race-trend",
      kv: { get, put },
      race,
    }),
  ).resolves.toBe(false);
  expect(put).not.toHaveBeenCalled();
  await expect(
    markRaceCacheWarmGeneration({
      generation: "8",
      kind: "race-trend",
      kv: undefined,
      race,
    }),
  ).resolves.toBe(false);
});

it("markRaceCacheWarmGeneration contains KV failures", async () => {
  const get = vi.fn<(key: string) => Promise<string | null>>().mockRejectedValue(new Error("kv"));
  const put = vi.fn<(key: string, value: string) => Promise<void>>();
  await expect(
    markRaceCacheWarmGeneration({
      generation: "2",
      kind: "race-detail-ssr",
      kv: { get, put },
      race,
    }),
  ).resolves.toBe(false);
});
