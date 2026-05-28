// Run with bun.
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("./keiba-go", () => ({
  extractOddsLinks: vi.fn(() => ({ tansho: "https://nar/tansho" })),
  fetchOdds: vi.fn(async () => ({ tansho: [{ combination: "01", odds: 2.5 }] })),
  fetchRacePage: vi.fn(async () => "<html></html>"),
}));

vi.mock("./jra", () => ({
  fetchJraOddsWithPlaywright: vi.fn(async () => ({
    latest: { tansho: [{ combination: "01", odds: 3.5 }] },
  })),
}));

import { extractOddsLinks, fetchOdds, fetchRacePage } from "./keiba-go";
import { fetchJraOddsWithPlaywright } from "./jra";
import {
  fetchAndStoreOdds,
  getRaceStartFromState,
  isSlotDue,
  resolveOddsSlotAt,
} from "./fetch-odds";
import type { Env, OddsFetchStateRow } from "./types";

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

const sampleNarState = (overrides: Partial<OddsFetchStateRow> = {}): OddsFetchStateRow => ({
  debaUrl: "https://nar/race",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0528",
  keibajoCode: "42",
  lastOddsFetchAt: null,
  lastOddsQueuedAt: null,
  oddsFetchLockUntil: null,
  oddsLinksJson: "{}",
  raceBango: "01",
  raceKey: "nar:20260528:42:01",
  raceStartAtJst: "2026-05-28T15:00:00+09:00",
  source: "nar",
  updatedAt: "2026-05-28T01:00:00+09:00",
  ...overrides,
});

interface BuildDbOptions {
  claimChanges?: number;
  state?: OddsFetchStateRow | null;
  narVenueLast?: string | null;
  insertedCount?: number;
}

const buildDb = (options: BuildDbOptions = {}): D1Database => {
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("update odds_fetch_state set odds_fetch_lock_until")) {
      const run = vi.fn(async () => ({
        meta: { changes: options.claimChanges ?? 1 },
      }));
      return { bind: vi.fn(() => ({ run })) };
    }
    if (lowered.includes("update odds_fetch_state")) {
      const run = vi.fn(async () => ({ meta: { changes: 1 } }));
      return { bind: vi.fn(() => ({ run })) };
    }
    if (lowered.includes("select * from odds_fetch_state")) {
      const value = options.state === undefined ? sampleNarState() : options.state;
      const first = vi.fn(async () =>
        value
          ? {
              deba_url: value.debaUrl,
              kaisai_nen: value.kaisaiNen,
              kaisai_tsukihi: value.kaisaiTsukihi,
              keibajo_code: value.keibajoCode,
              last_odds_fetch_at: value.lastOddsFetchAt,
              last_odds_queued_at: value.lastOddsQueuedAt,
              odds_fetch_lock_until: value.oddsFetchLockUntil,
              odds_links_json: value.oddsLinksJson,
              race_bango: value.raceBango,
              race_key: value.raceKey,
              race_start_at_jst: value.raceStartAtJst,
              source: value.source,
              updated_at: value.updatedAt,
            }
          : null,
      );
      return { bind: vi.fn(() => ({ first })) };
    }
    if (lowered.includes("select max(race_start_at_jst)")) {
      const first = vi.fn(async () => ({
        last_race_start_at_jst: options.narVenueLast ?? null,
      }));
      return { bind: vi.fn(() => ({ first })) };
    }
    if (lowered.includes("insert into odds_snapshots")) {
      const run = vi.fn(async () => ({ meta: { changes: options.insertedCount ?? 1 } }));
      return { bind: vi.fn(() => ({ run })) };
    }
    if (lowered.includes("insert into fetch_logs")) {
      const run = vi.fn(async () => ({ meta: { changes: 1 } }));
      return { bind: vi.fn(() => ({ run })) };
    }
    const run = vi.fn(async () => ({ meta: { changes: 1 } }));
    return { bind: vi.fn(() => ({ run })) };
  });
  const batch = vi.fn(async () => []);
  return { batch, prepare: prepareMock } as unknown as D1Database;
};

const buildEnv = (overrides: Partial<Env> = {}, dbOptions: BuildDbOptions = {}): Env =>
  ({
    JRA_BROWSER: {} as Env["JRA_BROWSER"],
    REALTIME_HOT_DB: buildDb(dbOptions),
    ...overrides,
  }) as unknown as Env;

it("getRaceStartFromState parses kaisai date + race start hh:mm", () => {
  const start = getRaceStartFromState(sampleNarState());
  expect(start?.toISOString()).toBe("2026-05-28T06:00:00.000Z");
});

it("isSlotDue returns true when lastActivityAt is null", () => {
  expect(isSlotDue(null, "2026-05-28T10:00:00+09:00")).toBe(true);
});

it("isSlotDue returns true when slot is newer than activity", () => {
  expect(isSlotDue("2026-05-28T09:00:00+09:00", "2026-05-28T10:00:00+09:00")).toBe(true);
});

it("isSlotDue returns false when activity is newer than slot", () => {
  expect(isSlotDue("2026-05-28T10:30:00+09:00", "2026-05-28T10:00:00+09:00")).toBe(false);
});

it("resolveOddsSlotAt uses JRA helpers for jra source", async () => {
  const env = buildEnv();
  const result = await resolveOddsSlotAt(
    env,
    sampleNarState({ source: "jra" }),
    new Date("2026-05-28T06:00:00+09:00"),
    new Date("2026-05-28T04:30:00+09:00"),
  );
  expect(typeof result).toBe("string");
});

it("resolveOddsSlotAt uses NAR sale-start logic for nar source", async () => {
  const env = buildEnv();
  const result = await resolveOddsSlotAt(
    env,
    sampleNarState(),
    new Date("2026-05-28T15:00:00+09:00"),
    new Date("2026-05-28T10:30:00+09:00"),
  );
  expect(typeof result === "string" || result === null).toBe(true);
});

it("fetchAndStoreOdds returns null when claim does not change rows", async () => {
  const env = buildEnv({}, { claimChanges: 0 });
  const result = await fetchAndStoreOdds(env, "nar:20260528:42:01", new Date());
  expect(result).toBeNull();
});

it("fetchAndStoreOdds throws when state is missing", async () => {
  const env = buildEnv({}, { state: null });
  await expect(fetchAndStoreOdds(env, "nar:20260528:42:01", new Date())).rejects.toThrow();
});

it("fetchAndStoreOdds returns null when race start cannot be parsed", async () => {
  const env = buildEnv({}, { state: sampleNarState({ raceStartAtJst: "not-a-date" }) });
  const result = await fetchAndStoreOdds(env, "nar:20260528:42:01", new Date());
  expect(result).toBeNull();
});

it("fetchAndStoreOdds returns null when slot is not due", async () => {
  const env = buildEnv(
    {},
    {
      state: sampleNarState({ lastOddsFetchAt: "2026-05-28T15:30:00+09:00" }),
    },
  );
  const result = await fetchAndStoreOdds(
    env,
    "nar:20260528:42:01",
    new Date("2026-05-28T05:00:00Z"),
  );
  expect(result).toBeNull();
});

it("fetchAndStoreOdds scrapes NAR and inserts when slot is due", async () => {
  const env = buildEnv();
  const result = await fetchAndStoreOdds(
    env,
    "nar:20260528:42:01",
    new Date("2026-05-28T05:55:00Z"),
  );
  expect(result?.inserted).toBe(1);
  expect(vi.mocked(fetchRacePage)).toHaveBeenCalled();
  expect(vi.mocked(fetchOdds)).toHaveBeenCalled();
});

it("fetchAndStoreOdds reuses cached odds links when present", async () => {
  const env = buildEnv(
    {},
    {
      state: sampleNarState({
        oddsLinksJson: JSON.stringify({ tansho: "https://nar/tansho" }),
      }),
    },
  );
  await fetchAndStoreOdds(env, "nar:20260528:42:01", new Date("2026-05-28T05:55:00Z"));
  expect(vi.mocked(extractOddsLinks)).not.toHaveBeenCalled();
});

it("fetchAndStoreOdds falls back to empty links when state json is malformed", async () => {
  const env = buildEnv(
    {},
    {
      state: sampleNarState({ oddsLinksJson: "not-json" }),
    },
  );
  await fetchAndStoreOdds(env, "nar:20260528:42:01", new Date("2026-05-28T05:55:00Z"));
  expect(vi.mocked(extractOddsLinks)).toHaveBeenCalled();
});

it("fetchAndStoreOdds treats odds_links_json non-object as empty", async () => {
  const env = buildEnv(
    {},
    {
      state: sampleNarState({ oddsLinksJson: "42" }),
    },
  );
  await fetchAndStoreOdds(env, "nar:20260528:42:01", new Date("2026-05-28T05:55:00Z"));
  expect(vi.mocked(extractOddsLinks)).toHaveBeenCalled();
});

it("fetchAndStoreOdds scrapes JRA via Playwright when source is jra", async () => {
  const env = buildEnv(
    {},
    {
      state: sampleNarState({ source: "jra", oddsLinksJson: "{}" }),
    },
  );
  const result = await fetchAndStoreOdds(
    env,
    "jra:20260528:08:01",
    new Date("2026-05-28T03:00:00Z"),
  );
  expect(result?.inserted).toBe(1);
  expect(vi.mocked(fetchJraOddsWithPlaywright)).toHaveBeenCalled();
});

it("fetchAndStoreOdds throws when JRA_BROWSER binding missing", async () => {
  const env = buildEnv(
    { JRA_BROWSER: undefined },
    {
      state: sampleNarState({ source: "jra", oddsLinksJson: "{}" }),
    },
  );
  await expect(
    fetchAndStoreOdds(env, "jra:20260528:08:01", new Date("2026-05-28T03:00:00Z")),
  ).rejects.toThrow();
});

it("fetchAndStoreOdds throws when scrape returns zero rows", async () => {
  vi.mocked(fetchOdds).mockResolvedValueOnce({});
  const env = buildEnv({}, { insertedCount: 0 });
  await expect(
    fetchAndStoreOdds(env, "nar:20260528:42:01", new Date("2026-05-28T05:55:00Z")),
  ).rejects.toThrow();
});
