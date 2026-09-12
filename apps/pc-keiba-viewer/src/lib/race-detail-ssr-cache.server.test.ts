import { describe, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));
vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareRuntime: vi.fn<() => Promise<{ ctx: null; env: null }>>(async () => ({
    ctx: null,
    env: null,
  })),
}));

import {
  buildRaceDetailSsrCacheKey,
  getRaceDetailSsrCacheTtlSeconds,
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
});
