// Run with bun.
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("./keiba-go", () => ({
  extractOddsLinks: vi.fn(() => ({ tansho: "https://nar/tansho" })),
  fetchOdds: vi.fn(async () => ({ tansho: [{ combination: "01", odds: 2.5 }] })),
  fetchRacePage: vi.fn(async () => "<html></html>"),
}));

vi.mock("./jra", () => ({
  fetchJraOddsWithPlaywright: vi.fn(async () => ({
    entryHtml: "<html></html>",
    latest: { tansho: [{ combination: "01", odds: 3.5 }] },
    missingTypes: [],
  })),
}));

import { extractOddsLinks, fetchOdds, fetchRacePage } from "./keiba-go";
import { fetchJraOddsWithPlaywright } from "./jra";
import {
  fetchAndStoreOdds,
  getRaceStartFromState,
  isRetryableScrapeError,
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

const buildKv = (): KVNamespace =>
  ({
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  }) as unknown as KVNamespace;

const buildEnv = (overrides: Partial<Env> = {}, dbOptions: BuildDbOptions = {}): Env =>
  ({
    JRA_BROWSER: {} as Env["JRA_BROWSER"],
    ODDS_HOT_KV: buildKv(),
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

it("isRetryableScrapeError returns true for transient errors (K1-B)", () => {
  expect(isRetryableScrapeError(new Error("Playwright timeout"))).toBe(true);
});

it("isRetryableScrapeError returns false when the message contains JRA_BROWSER binding (K1-B)", () => {
  expect(isRetryableScrapeError(new Error("JRA_BROWSER binding required for jra:x"))).toBe(false);
});

it("isRetryableScrapeError returns false when the message contains odds_fetch_state not found (K1-B)", () => {
  expect(isRetryableScrapeError(new Error("odds_fetch_state not found: nar:y"))).toBe(false);
});

it("isRetryableScrapeError stringifies non-Error rejection values to evaluate retryability (K1-B)", () => {
  expect(isRetryableScrapeError("random string failure")).toBe(true);
});

it("fetchAndStoreOdds releases the enqueue lock when sale has not opened yet (K1-B)", async () => {
  // NAR sale opens at 09:00 JST; with now = 08:00 JST the slot resolver
  // returns null and we drop the lock so the next planner tick can take
  // over once sale starts.
  const env = buildEnv();
  const result = await fetchAndStoreOdds(
    env,
    "nar:20260528:42:01",
    new Date("2026-05-27T23:00:00Z"),
  );
  expect(result).toBeNull();
  expect(env.ODDS_HOT_KV.delete).toHaveBeenCalledWith("odds:enqueue-lock:nar:20260528:42:01");
});

it("fetchAndStoreOdds keeps the enqueue lock when slot was already fetched (K1-B not-due maintain)", async () => {
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
  expect(env.ODDS_HOT_KV.delete).not.toHaveBeenCalled();
});

it("fetchAndStoreOdds releases the enqueue lock on a retryable scrape error (K1-B)", async () => {
  vi.mocked(fetchOdds).mockRejectedValueOnce(new Error("network reset"));
  const env = buildEnv();
  await expect(
    fetchAndStoreOdds(env, "nar:20260528:42:01", new Date("2026-05-28T05:55:00Z")),
  ).rejects.toThrow("network reset");
  expect(env.ODDS_HOT_KV.delete).toHaveBeenCalledWith("odds:enqueue-lock:nar:20260528:42:01");
});

it("fetchAndStoreOdds keeps the enqueue lock when JRA_BROWSER binding is missing (K1-B non-retryable)", async () => {
  const env = buildEnv(
    { JRA_BROWSER: undefined },
    {
      state: sampleNarState({ source: "jra", oddsLinksJson: "{}" }),
    },
  );
  await expect(
    fetchAndStoreOdds(env, "jra:20260528:08:01", new Date("2026-05-28T03:00:00Z")),
  ).rejects.toThrow("JRA_BROWSER binding");
  expect(env.ODDS_HOT_KV.delete).not.toHaveBeenCalled();
});

it("fetchAndStoreOdds keeps the enqueue lock when raceStart cannot be parsed (K1-B structural)", async () => {
  const env = buildEnv({}, { state: sampleNarState({ raceStartAtJst: "not-a-date" }) });
  const result = await fetchAndStoreOdds(env, "nar:20260528:42:01", new Date());
  expect(result).toBeNull();
  expect(env.ODDS_HOT_KV.delete).not.toHaveBeenCalled();
});

it("fetchAndStoreOdds writes a partial-fetch warn log when missingTypes is non-empty (K1-A)", async () => {
  vi.mocked(fetchJraOddsWithPlaywright).mockResolvedValueOnce({
    entryHtml: "<html></html>",
    latest: { tansho: [{ combination: "01", odds: 3.5 }] },
    missingTypes: ["umaren", "wide"],
  });
  const dbOptions: BuildDbOptions = {
    state: sampleNarState({ source: "jra", oddsLinksJson: "{}" }),
  };
  const env = buildEnv({}, dbOptions);
  const prepareMock = env.REALTIME_HOT_DB.prepare as unknown as ReturnType<typeof vi.fn>;
  await fetchAndStoreOdds(env, "jra:20260528:08:01", new Date("2026-05-28T03:00:00Z"));
  const fetchLogCalls = prepareMock.mock.calls.filter((call: unknown[]) =>
    String(call[0]).toLowerCase().includes("insert into fetch_logs"),
  );
  expect(fetchLogCalls.length >= 2).toBe(true);
});

it("fetchAndStoreOdds passes a 3-minute lockUntil (now + 3min in JST iso) to claimOddsFetch", async () => {
  const bindSpy = vi.fn((..._bindArgs: string[]) => ({
    run: vi.fn(async () => ({ meta: { changes: 1 } })),
  }));
  const prepareSpy = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("update odds_fetch_state set odds_fetch_lock_until")) {
      return { bind: bindSpy };
    }
    if (lowered.includes("select * from odds_fetch_state")) {
      const state = sampleNarState();
      const first = vi.fn(async () => ({
        deba_url: state.debaUrl,
        kaisai_nen: state.kaisaiNen,
        kaisai_tsukihi: state.kaisaiTsukihi,
        keibajo_code: state.keibajoCode,
        last_odds_fetch_at: state.lastOddsFetchAt,
        last_odds_queued_at: state.lastOddsQueuedAt,
        odds_fetch_lock_until: state.oddsFetchLockUntil,
        odds_links_json: state.oddsLinksJson,
        race_bango: state.raceBango,
        race_key: state.raceKey,
        race_start_at_jst: state.raceStartAtJst,
        source: state.source,
        updated_at: state.updatedAt,
      }));
      return { bind: vi.fn(() => ({ first })) };
    }
    if (lowered.includes("select max(race_start_at_jst)")) {
      const first = vi.fn(async () => ({ last_race_start_at_jst: null }));
      return { bind: vi.fn(() => ({ first })) };
    }
    const run = vi.fn(async () => ({ meta: { changes: 1 } }));
    return { bind: vi.fn(() => ({ run })) };
  });
  const db = { batch: vi.fn(async () => []), prepare: prepareSpy } as unknown as D1Database;
  const env = { JRA_BROWSER: {}, ODDS_HOT_KV: buildKv(), REALTIME_HOT_DB: db } as unknown as Env;
  await fetchAndStoreOdds(env, "nar:20260528:42:01", new Date("2026-05-28T05:55:00Z"));
  const firstCall = bindSpy.mock.calls[0];
  const lockUntilArg = firstCall ? firstCall[0] : null;
  expect(lockUntilArg).toBe("2026-05-28T14:58:00+09:00");
});

it("fetchAndStoreOdds skips the partial-fetch log when every JRA tab succeeded (K1-A)", async () => {
  vi.mocked(fetchJraOddsWithPlaywright).mockResolvedValueOnce({
    entryHtml: "<html></html>",
    latest: { tansho: [{ combination: "01", odds: 3.5 }] },
    missingTypes: [],
  });
  const dbOptions: BuildDbOptions = {
    state: sampleNarState({ source: "jra", oddsLinksJson: "{}" }),
  };
  const env = buildEnv({}, dbOptions);
  const prepareMock = env.REALTIME_HOT_DB.prepare as unknown as ReturnType<typeof vi.fn>;
  await fetchAndStoreOdds(env, "jra:20260528:08:01", new Date("2026-05-28T03:00:00Z"));
  const fetchLogCalls = prepareMock.mock.calls.filter((call: unknown[]) =>
    String(call[0]).toLowerCase().includes("insert into fetch_logs"),
  );
  expect(fetchLogCalls).toHaveLength(1);
});
