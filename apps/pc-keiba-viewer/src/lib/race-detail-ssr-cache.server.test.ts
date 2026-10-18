import { describe, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));
interface TestRuntime {
  ctx: { waitUntil: (promise: Promise<unknown>) => void } | null;
  env: { DETAIL_SECTION_CACHE_KV: { put: (...args: unknown[]) => Promise<void> } } | null;
}

const runtime: TestRuntime = vi.hoisted((): TestRuntime => ({ ctx: null, env: null }));
vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareRuntime: vi.fn<() => Promise<TestRuntime>>(async () => runtime),
}));

import {
  buildRaceDetailSsrCacheKey,
  getRaceDetailSsrCacheTtlSeconds,
  getRaceDetailSsrKvTtlSeconds,
  isRaceDetailSsrSnapshotStale,
  putRaceDetailSsrSnapshot,
  refreshRaceDetailSsrSnapshotIfStale,
  type RaceDetailSsrSnapshot,
} from "./race-detail-ssr-cache.server";
import type { RaceDetail } from "./race-types";

const params = {
  day: "12",
  keibajoCode: "06",
  month: "09",
  raceNumber: "01",
  source: "jra",
  year: "2026",
} as const;

const buildRace = (hassoJikoku: string | null): RaceDetail => ({
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  gradeCode: null,
  hassoJikoku,
  juryoShubetsuCode: null,
  kaisaiKai: "04",
  kaisaiNen: "2026",
  kaisaiNichime: "03",
  kaisaiTsukihi: "0912",
  keibajoCode: "06",
  kyori: "1200",
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosomeiKakkonai: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  raceBango: "01",
  shussoTosu: "12",
  source: "jra",
  tenkoCode: null,
  torokuTosu: "12",
  trackCode: "24",
});

describe("race detail SSR cache", () => {
  it("uses the confirmed-runner cache namespace", () => {
    expect(buildRaceDetailSsrCacheKey(params)).toBe("race-detail-ssr:v4:jra:2026:09:12:06:01");
  });

  it("caps pre-race snapshots at 60 seconds", () => {
    const nowMs = Date.parse("2026-09-12T09:40:00+09:00");
    expect(getRaceDetailSsrCacheTtlSeconds(params, { race: buildRace("0945") }, null, nowMs)).toBe(
      60,
    );
  });

  it("retains the configured post-race cache window", () => {
    const nowMs = Date.parse("2026-09-12T09:46:00+09:00");
    expect(getRaceDetailSsrCacheTtlSeconds(params, { race: buildRace("0945") }, null, nowMs)).toBe(
      21_540,
    );
  });

  it("uses the end-of-day fallback for a missing start time", () => {
    const nowMs = Date.parse("2026-09-12T23:59:00+09:00");
    expect(getRaceDetailSsrCacheTtlSeconds(params, { race: buildRace(null) }, null, nowMs)).toBe(
      21_659,
    );
  });

  it("keeps pre-race snapshots in KV until the post-start window ends", () => {
    const nowMs = Date.parse("2026-09-12T09:40:00+09:00");
    expect(getRaceDetailSsrKvTtlSeconds(params, { race: buildRace("0945") }, null, nowMs)).toBe(
      21_900,
    );
  });

  it("treats snapshots without a refresh time as stale", () => {
    expect(isRaceDetailSsrSnapshotStale({}, 1000)).toBe(true);
    expect(isRaceDetailSsrSnapshotStale({ refreshAt: 1000 }, 999)).toBe(false);
    expect(isRaceDetailSsrSnapshotStale({ refreshAt: 1000 }, 1000)).toBe(true);
  });

  it("writes the long KV window and the short refresh time", async () => {
    vi.useFakeTimers({ now: Date.parse("2026-09-12T09:40:00+09:00") });
    const put = vi.fn<(...args: unknown[]) => Promise<void>>(async () => undefined);
    runtime.env = { DETAIL_SECTION_CACHE_KV: { put } };
    await putRaceDetailSsrSnapshot({
      cacheKey: "k",
      params,
      snapshot: { courseInfo: null, race: buildRace("0945"), runners: [], sameVenueRaces: [] },
    });
    vi.useRealTimers();
    runtime.env = null;
    expect(
      put.mock.calls.map((call) => [call[0], JSON.parse(String(call[1])).refreshAt, call[2]]),
    ).toStrictEqual([["k", 1_789_173_660_000, { expirationTtl: 21_900 }]]);
  });

  it("skips the KV write once the post-start window has passed", async () => {
    vi.useFakeTimers({ now: Date.parse("2026-09-12T16:00:00+09:00") });
    const put = vi.fn<(...args: unknown[]) => Promise<void>>(async () => undefined);
    runtime.env = { DETAIL_SECTION_CACHE_KV: { put } };
    await putRaceDetailSsrSnapshot({
      cacheKey: "k",
      params,
      snapshot: { courseInfo: null, race: buildRace("0945"), runners: [], sameVenueRaces: [] },
    });
    vi.useRealTimers();
    runtime.env = null;
    expect(put.mock.calls.length).toBe(0);
  });

  it("refreshes a stale snapshot after the response and leaves fresh ones alone", async () => {
    const pending: Promise<unknown>[] = [];
    runtime.ctx = { waitUntil: (promise) => pending.push(promise) };
    const snapshot = { courseInfo: null, race: buildRace("0945"), runners: [], sameVenueRaces: [] };
    const load = vi.fn<() => Promise<RaceDetailSsrSnapshot | null>>(async () => null);
    await refreshRaceDetailSsrSnapshotIfStale({
      cacheKey: "k",
      load,
      params,
      snapshot: { ...snapshot, refreshAt: Date.now() + 60_000 },
    });
    await refreshRaceDetailSsrSnapshotIfStale({ cacheKey: "k", load, params, snapshot });
    await Promise.all(pending);
    const failing = vi.fn<() => Promise<RaceDetailSsrSnapshot | null>>(async () => {
      throw new Error("db down");
    });
    await refreshRaceDetailSsrSnapshotIfStale({ cacheKey: "k", load: failing, params, snapshot });
    await Promise.all(pending);
    const reload = vi.fn<() => Promise<RaceDetailSsrSnapshot | null>>(async () => snapshot);
    await refreshRaceDetailSsrSnapshotIfStale({ cacheKey: "k", load: reload, params, snapshot });
    await Promise.all(pending);
    runtime.ctx = null;
    expect([
      load.mock.calls.length,
      failing.mock.calls.length,
      reload.mock.calls.length,
      pending.length,
    ]).toStrictEqual([1, 1, 1, 3]);
  });
});
