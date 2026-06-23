// Run with bun.
import { afterEach, expect, it, vi } from "vitest";

import type { Env } from "../types";
import {
  ARCHIVE_LAST_SUCCESS_KV_KEY,
  CLOSING_BACKFILL_LAST_RUN_KV_KEY,
  buildArchiveCronCheck,
  buildClosingBackfillCheck,
  buildCronHeartbeatCheck,
  buildHealthReport,
  buildRecentErrorsCheck,
  buildTodayPollingProgressCheck,
  buildTodayRacesPopulatedCheck,
  summarizePollingProgressRows,
} from "./health";

interface KvCallTracker {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

interface BuildKvOptions {
  get?: ReturnType<typeof vi.fn>;
}

const buildKv = (options: BuildKvOptions = {}): { kv: KVNamespace; tracker: KvCallTracker } => {
  const tracker: KvCallTracker = {
    get: options.get ?? vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  };
  const kv = {
    delete: vi.fn(async () => undefined),
    get: tracker.get,
    put: tracker.put,
  } as unknown as KVNamespace;
  return { kv, tracker };
};

interface BuildDbOptions {
  fetchStateCount?: number;
  pollingRows?: { results: unknown[] };
  fetchLogRows?: { results: unknown[] };
  archiveFailureRow?: { created_at: string } | null;
}

const buildDb = (options: BuildDbOptions = {}): D1Database => {
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("count(*)") && lowered.includes("from odds_fetch_state")) {
      const first = vi.fn(async () => ({ count: options.fetchStateCount ?? 0 }));
      return { bind: vi.fn(() => ({ first })) };
    }
    if (
      lowered.includes("from odds_fetch_state") &&
      lowered.includes("race_start_at_jst") &&
      lowered.includes("last_odds_fetch_at")
    ) {
      const all = vi.fn(async () => options.pollingRows ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (
      lowered.includes("from fetch_logs") &&
      lowered.includes("scheduled-archive-to-r2") &&
      lowered.includes("status = 'warn'")
    ) {
      const first = vi.fn(async () => options.archiveFailureRow ?? null);
      return { first };
    }
    if (lowered.includes("from fetch_logs") && lowered.includes("status = 'error'")) {
      const all = vi.fn(async () => options.fetchLogRows ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    return {
      bind: vi.fn(() => ({
        all: vi.fn(async () => ({ results: [] })),
        first: vi.fn(async () => null),
      })),
    };
  });
  return { prepare: prepareMock } as unknown as D1Database;
};

interface BuildEnvOptions {
  kv?: KVNamespace;
  db?: D1Database;
}

const buildEnv = (options: BuildEnvOptions = {}): Env =>
  ({
    ODDS_HOT_KV: options.kv ?? buildKv().kv,
    PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret",
    REALTIME_HOT_DB: options.db ?? buildDb(),
  }) as unknown as Env;

afterEach(() => {
  vi.useRealTimers();
});

it("buildCronHeartbeatCheck returns ok=true when the heartbeat is fresh", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T05:23:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "cron:heartbeat:scheduled" ? "2026-06-23T05:22:30.000Z" : null,
    ),
  });
  const result = await buildCronHeartbeatCheck(buildEnv({ kv }), new Date());
  expect(result).toStrictEqual({
    ageSeconds: 30,
    lastTickAt: "2026-06-23T05:22:30.000Z",
    ok: true,
    thresholdSeconds: 300,
  });
});

it("buildCronHeartbeatCheck returns ok=false when the heartbeat key is missing", async () => {
  const result = await buildCronHeartbeatCheck(buildEnv(), new Date());
  expect(result).toStrictEqual({
    ageSeconds: null,
    lastTickAt: null,
    ok: false,
    thresholdSeconds: 300,
  });
});

it("buildCronHeartbeatCheck returns ok=false when the heartbeat exceeds the threshold", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T06:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "cron:heartbeat:scheduled" ? "2026-06-23T05:00:00.000Z" : null,
    ),
  });
  const result = await buildCronHeartbeatCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(false);
  expect(result.ageSeconds).toBe(3600);
});

it("buildCronHeartbeatCheck treats a KV throw as missing key", async () => {
  const { kv } = buildKv({
    get: vi.fn(async () => {
      throw new Error("kv outage");
    }),
  });
  const result = await buildCronHeartbeatCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(false);
  expect(result.lastTickAt).toBeNull();
});

it("buildArchiveCronCheck returns ok=true when the success key is fresh and queries failure timestamp on cache miss", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T00:00:00Z"));
  const kvGet = vi.fn(async (key: string) => {
    if (key === "cron:archive:last-success") return "2026-06-23T18:00:00.000Z";
    return null;
  });
  const { kv, tracker } = buildKv({ get: kvGet });
  const db = buildDb({ archiveFailureRow: { created_at: "2026-06-22T03:00:00+09:00" } });
  const result = await buildArchiveCronCheck(buildEnv({ db, kv }), new Date());
  expect(result.ok).toBe(true);
  expect(result.lastSuccessAt).toBe("2026-06-23T18:00:00.000Z");
  expect(result.lastFailureAt).toBe("2026-06-22T03:00:00+09:00");
  expect(result.thresholdSeconds).toBe(86400);
  expect(
    tracker.put.mock.calls.some(([key]) => key === "monitor:archive:last-failure-snapshot"),
  ).toBe(true);
});

it("buildArchiveCronCheck returns ok=false when the success key is missing", async () => {
  const result = await buildArchiveCronCheck(buildEnv(), new Date());
  expect(result.ok).toBe(false);
  expect(result.lastSuccessAt).toBeNull();
  expect(result.ageSinceSuccessSeconds).toBeNull();
});

it("buildArchiveCronCheck returns ok=false when the success key is older than 24h", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T00:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "cron:archive:last-success" ? "2026-06-22T18:00:00.000Z" : null,
    ),
  });
  const result = await buildArchiveCronCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(false);
  expect(result.ageSinceSuccessSeconds).toBe(108000);
});

it("buildArchiveCronCheck reuses the cached failure snapshot without querying D1", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T00:00:00Z"));
  const kvGet = vi.fn(async (key: string) => {
    if (key === "cron:archive:last-success") return "2026-06-23T20:00:00.000Z";
    if (key === "monitor:archive:last-failure-snapshot") return "2026-06-23T03:00:00+09:00";
    return null;
  });
  const { kv } = buildKv({ get: kvGet });
  const db = buildDb({ archiveFailureRow: { created_at: "should-not-be-used" } });
  const result = await buildArchiveCronCheck(buildEnv({ db, kv }), new Date());
  expect(result.lastFailureAt).toBe("2026-06-23T03:00:00+09:00");
});

it("buildClosingBackfillCheck returns ok=true when last-run KV has ok status, 0 failures, within threshold", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T05:00:00Z"));
  const lastRunJson = JSON.stringify({
    at: "2026-06-23T13:30:00.000Z",
    candidates: 0,
    failures: 0,
    status: "ok",
  });
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "cron:closing-backfill:last-run" ? lastRunJson : null,
    ),
  });
  const result = await buildClosingBackfillCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(true);
  expect(result.lastRunStatus).toBe("ok");
  expect(result.lastCandidates).toBe(0);
  expect(result.lastFailures).toBe(0);
  expect(result.thresholdSeconds).toBe(90000);
});

it("buildClosingBackfillCheck returns ok=false when last-run has failures > 0", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T05:00:00Z"));
  const lastRunJson = JSON.stringify({
    at: "2026-06-23T13:30:00.000Z",
    candidates: 5,
    failures: 2,
    status: "warn",
  });
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "cron:closing-backfill:last-run" ? lastRunJson : null,
    ),
  });
  const result = await buildClosingBackfillCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(false);
  expect(result.lastFailures).toBe(2);
});

it("buildClosingBackfillCheck returns ok=false when the KV key is missing", async () => {
  const result = await buildClosingBackfillCheck(buildEnv(), new Date());
  expect(result.ok).toBe(false);
  expect(result.lastRunAt).toBeNull();
});

it("buildClosingBackfillCheck returns ok=false when the KV value is age-stale", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-25T00:00:00Z"));
  const lastRunJson = JSON.stringify({
    at: "2026-06-23T00:00:00.000Z",
    candidates: 0,
    failures: 0,
    status: "ok",
  });
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "cron:closing-backfill:last-run" ? lastRunJson : null,
    ),
  });
  const result = await buildClosingBackfillCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(false);
});

it("buildClosingBackfillCheck returns ok=false when KV value is malformed JSON", async () => {
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "cron:closing-backfill:last-run" ? "not json" : null,
    ),
  });
  const result = await buildClosingBackfillCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(false);
  expect(result.lastRunAt).toBeNull();
});

it("buildClosingBackfillCheck returns ok=false when KV JSON is missing required fields", async () => {
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "cron:closing-backfill:last-run" ? JSON.stringify({ at: "x" }) : null,
    ),
  });
  const result = await buildClosingBackfillCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(false);
});

it("buildTodayRacesPopulatedCheck returns ok=true when actual >= expected", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => {
      if (key === "expected-race-count:20260624") return "48";
      return null;
    }),
  });
  const db = buildDb({ fetchStateCount: 48 });
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ db, kv }), new Date());
  expect(result.ok).toBe(true);
  expect(result.expected).toBe(48);
  expect(result.actual).toBe(48);
  expect(result.yyyymmdd).toBe("20260624");
});

it("buildTodayRacesPopulatedCheck returns ok=false when actual < expected", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => (key === "expected-race-count:20260624" ? "48" : null)),
  });
  const db = buildDb({ fetchStateCount: 12 });
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ db, kv }), new Date());
  expect(result.ok).toBe(false);
  expect(result.actual).toBe(12);
});

it("buildTodayRacesPopulatedCheck returns ok=true when expected is zero (no races scheduled)", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => (key === "expected-race-count:20260624" ? "0" : null)),
  });
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ kv }), new Date());
  expect(result.ok).toBe(true);
  expect(result.expected).toBe(0);
});

it("buildTodayRacesPopulatedCheck returns ok=false when expected KV is missing", async () => {
  const db = buildDb({ fetchStateCount: 5 });
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ db }), new Date());
  expect(result.ok).toBe(false);
  expect(result.expected).toBeNull();
});

it("buildTodayRacesPopulatedCheck returns ok=false when D1 throws (actual=null)", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => (key === "expected-race-count:20260624" ? "48" : null)),
  });
  const prepareMock = vi.fn(() => ({
    bind: vi.fn(() => ({
      first: vi.fn(async () => {
        throw new Error("d1 down");
      }),
    })),
  }));
  const db = { prepare: prepareMock } as unknown as D1Database;
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ db, kv }), new Date());
  expect(result.ok).toBe(false);
  expect(result.actual).toBeNull();
});

it("buildTodayRacesPopulatedCheck reuses the snapshot cache on subsequent calls", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:00:00Z"));
  const cachedSnapshot = JSON.stringify({ actual: 48, expected: 48 });
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "monitor:today-races:snapshot:20260624" ? cachedSnapshot : null,
    ),
  });
  const db = buildDb({ fetchStateCount: 999 });
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ db, kv }), new Date());
  expect(result.actual).toBe(48);
  expect(result.expected).toBe(48);
  expect(vi.mocked(db.prepare)).not.toHaveBeenCalled();
});

it("buildTodayRacesPopulatedCheck ignores a malformed snapshot and re-queries", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => {
      if (key === "monitor:today-races:snapshot:20260624") return "not json";
      if (key === "expected-race-count:20260624") return "1";
      return null;
    }),
  });
  const db = buildDb({ fetchStateCount: 1 });
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ db, kv }), new Date());
  expect(result.ok).toBe(true);
});

it("buildTodayRacesPopulatedCheck ignores a snapshot with wrong field types", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => {
      if (key === "monitor:today-races:snapshot:20260624") return JSON.stringify({ actual: "bad" });
      if (key === "expected-race-count:20260624") return "1";
      return null;
    }),
  });
  const db = buildDb({ fetchStateCount: 1 });
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ db, kv }), new Date());
  expect(result.ok).toBe(true);
});

it("buildTodayRacesPopulatedCheck handles a malformed expected-race-count KV value as null", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "expected-race-count:20260624" ? "not a number" : null,
    ),
  });
  const result = await buildTodayRacesPopulatedCheck(buildEnv({ kv }), new Date());
  expect(result.expected).toBeNull();
});

it("summarizePollingProgressRows counts recent and started-not-polled correctly", () => {
  const now = new Date("2026-06-24T05:30:00Z");
  const rows = [
    { last_odds_fetch_at: "2026-06-24T05:28:00Z", race_start_at_jst: "2026-06-24T06:00:00Z" },
    { last_odds_fetch_at: null, race_start_at_jst: "2026-06-24T05:00:00Z" },
    { last_odds_fetch_at: "2026-06-24T04:00:00Z", race_start_at_jst: "2026-06-24T05:00:00Z" },
    { last_odds_fetch_at: "2026-06-24T05:00:00Z", race_start_at_jst: "2026-06-24T06:00:00Z" },
  ];
  const summary = summarizePollingProgressRows(rows, now);
  expect(summary).toStrictEqual({ recent: 1, startedNotPolled: 2, total: 4 });
});

it("buildTodayPollingProgressCheck returns ok=true outside the polling window", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T23:30:00Z"));
  const result = await buildTodayPollingProgressCheck(buildEnv(), new Date());
  expect(result.ok).toBe(true);
});

it("buildTodayPollingProgressCheck returns ok=false inside window when races are started but not polled", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T04:00:00Z"));
  const db = buildDb({
    pollingRows: {
      results: [{ last_odds_fetch_at: null, race_start_at_jst: "2026-06-24T03:00:00Z" }],
    },
  });
  const result = await buildTodayPollingProgressCheck(buildEnv({ db }), new Date());
  expect(result.ok).toBe(false);
  expect(result.racesStartedNotPolled).toBe(1);
});

it("buildTodayPollingProgressCheck reuses a cached polling snapshot", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T04:00:00Z"));
  const cached = JSON.stringify({ recent: 5, startedNotPolled: 0, total: 12 });
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "monitor:today-polling:snapshot:20260624" ? cached : null,
    ),
  });
  const db = buildDb();
  const result = await buildTodayPollingProgressCheck(buildEnv({ db, kv }), new Date());
  expect(result.totalRaces).toBe(12);
  expect(result.racesWithRecentFetch).toBe(5);
  expect(result.racesStartedNotPolled).toBe(0);
  expect(result.ok).toBe(true);
  expect(vi.mocked(db.prepare)).not.toHaveBeenCalled();
});

it("buildTodayPollingProgressCheck ignores a malformed polling snapshot and re-queries D1", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T04:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "monitor:today-polling:snapshot:20260624" ? "not json" : null,
    ),
  });
  const db = buildDb({ pollingRows: { results: [] } });
  const result = await buildTodayPollingProgressCheck(buildEnv({ db, kv }), new Date());
  expect(result.totalRaces).toBe(0);
});

it("buildTodayPollingProgressCheck treats D1 throw as zero rows", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T04:00:00Z"));
  const prepareMock = vi.fn(() => ({
    bind: vi.fn(() => ({
      all: vi.fn(async () => {
        throw new Error("d1 down");
      }),
    })),
  }));
  const db = { prepare: prepareMock } as unknown as D1Database;
  const result = await buildTodayPollingProgressCheck(buildEnv({ db }), new Date());
  expect(result.totalRaces).toBe(0);
});

it("buildTodayPollingProgressCheck rejects a snapshot whose shape is wrong", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T04:00:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "monitor:today-polling:snapshot:20260624" ? JSON.stringify({ recent: "bad" }) : null,
    ),
  });
  const db = buildDb({ pollingRows: { results: [] } });
  const result = await buildTodayPollingProgressCheck(buildEnv({ db, kv }), new Date());
  expect(result.totalRaces).toBe(0);
});

it("buildRecentErrorsCheck returns ok=true when error count is below threshold", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T05:00:00Z"));
  const db = buildDb({
    fetchLogRows: {
      results: [
        { created_at: "2026-06-24T04:50:00+09:00", message: "boom-1" },
        { created_at: "2026-06-24T04:55:00+09:00", message: "boom-2" },
      ],
    },
  });
  const result = await buildRecentErrorsCheck(buildEnv({ db }), new Date());
  expect(result.ok).toBe(true);
  expect(result.errorsLastHour).toBe(2);
  expect(result.thresholdCount).toBe(5);
  expect(result.samplesLastHour).toStrictEqual(["boom-1", "boom-2"]);
});

it("buildRecentErrorsCheck returns ok=false when error count exceeds threshold", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T05:00:00Z"));
  const errors = Array.from({ length: 7 }, (_, idx) => ({
    created_at: "2026-06-24T04:00:00+09:00",
    message: `err-${idx}`,
  }));
  const db = buildDb({ fetchLogRows: { results: errors } });
  const result = await buildRecentErrorsCheck(buildEnv({ db }), new Date());
  expect(result.ok).toBe(false);
  expect(result.errorsLastHour).toBe(7);
});

it("buildRecentErrorsCheck reuses a cached snapshot without touching D1", async () => {
  const cached = JSON.stringify({
    errorsLastHour: 1,
    samplesLastHour: ["cached message"],
  });
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => (key === "monitor:recent-errors:snapshot" ? cached : null)),
  });
  const db = buildDb();
  const result = await buildRecentErrorsCheck(buildEnv({ db, kv }), new Date());
  expect(result.errorsLastHour).toBe(1);
  expect(result.samplesLastHour).toStrictEqual(["cached message"]);
  expect(vi.mocked(db.prepare)).not.toHaveBeenCalled();
});

it("buildRecentErrorsCheck treats D1 throw as zero errors", async () => {
  const prepareMock = vi.fn(() => ({
    bind: vi.fn(() => ({
      all: vi.fn(async () => {
        throw new Error("d1 down");
      }),
    })),
  }));
  const db = { prepare: prepareMock } as unknown as D1Database;
  const result = await buildRecentErrorsCheck(buildEnv({ db }), new Date());
  expect(result.errorsLastHour).toBe(0);
  expect(result.ok).toBe(true);
});

it("buildRecentErrorsCheck drops null messages from the sample", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-24T05:00:00Z"));
  const db = buildDb({
    fetchLogRows: {
      results: [
        { created_at: "2026-06-24T04:50:00+09:00", message: null },
        { created_at: "2026-06-24T04:51:00+09:00", message: "real-error" },
      ],
    },
  });
  const result = await buildRecentErrorsCheck(buildEnv({ db }), new Date());
  expect(result.samplesLastHour).toStrictEqual(["real-error"]);
});

it("buildRecentErrorsCheck ignores a malformed snapshot and re-queries D1", async () => {
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "monitor:recent-errors:snapshot" ? "not json" : null,
    ),
  });
  const db = buildDb({ fetchLogRows: { results: [] } });
  const result = await buildRecentErrorsCheck(buildEnv({ db, kv }), new Date());
  expect(result.errorsLastHour).toBe(0);
});

it("buildRecentErrorsCheck rejects a snapshot with wrong shape", async () => {
  const { kv } = buildKv({
    get: vi.fn(async (key: string) =>
      key === "monitor:recent-errors:snapshot" ? JSON.stringify({ errorsLastHour: "bad" }) : null,
    ),
  });
  const db = buildDb({ fetchLogRows: { results: [] } });
  const result = await buildRecentErrorsCheck(buildEnv({ db, kv }), new Date());
  expect(result.errorsLastHour).toBe(0);
});

it("buildRecentErrorsCheck filters non-string samples out of the cached snapshot", async () => {
  const cached = JSON.stringify({
    errorsLastHour: 2,
    samplesLastHour: ["good", 123, "also-good"],
  });
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => (key === "monitor:recent-errors:snapshot" ? cached : null)),
  });
  const result = await buildRecentErrorsCheck(buildEnv({ kv }), new Date());
  expect(result.samplesLastHour).toStrictEqual(["good", "also-good"]);
});

it("buildHealthReport returns ok=true when every check is ok", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-23T22:30:00Z"));
  const { kv } = buildKv({
    get: vi.fn(async (key: string) => {
      if (key === "cron:heartbeat:scheduled") return "2026-06-23T22:29:30.000Z";
      if (key === "cron:archive:last-success") return "2026-06-23T20:00:00.000Z";
      if (key === "cron:closing-backfill:last-run") {
        return JSON.stringify({
          at: "2026-06-23T13:30:00.000Z",
          candidates: 0,
          failures: 0,
          status: "ok",
        });
      }
      if (key === "expected-race-count:20260624") return "48";
      return null;
    }),
  });
  const db = buildDb({ fetchStateCount: 48 });
  const report = await buildHealthReport(buildEnv({ db, kv }), new Date());
  expect(report.ok).toBe(true);
  expect(report.checks.cron_heartbeat.ok).toBe(true);
  expect(report.checks.archive_cron.ok).toBe(true);
  expect(report.checks.closing_backfill_cron.ok).toBe(true);
  expect(report.checks.today_races_populated.ok).toBe(true);
  expect(report.checks.today_polling_progress.ok).toBe(true);
  expect(report.checks.recent_errors.ok).toBe(true);
});

it("buildHealthReport returns ok=false when any check fails", async () => {
  const report = await buildHealthReport(buildEnv(), new Date());
  expect(report.ok).toBe(false);
});

it("ARCHIVE_LAST_SUCCESS_KV_KEY is exported as cron:archive:last-success", () => {
  expect(ARCHIVE_LAST_SUCCESS_KV_KEY).toBe("cron:archive:last-success");
});

it("CLOSING_BACKFILL_LAST_RUN_KV_KEY is exported as cron:closing-backfill:last-run", () => {
  expect(CLOSING_BACKFILL_LAST_RUN_KV_KEY).toBe("cron:closing-backfill:last-run");
});
