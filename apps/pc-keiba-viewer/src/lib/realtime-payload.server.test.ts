// Run with bun. `bun run --filter pc-keiba-viewer test`
import { DatabaseSync } from "node:sqlite";

import { expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

import {
  buildRaceKey,
  buildRealtimePayloadForRequest,
  fetchHorseWeightsLatest,
  fetchOddsFromHot,
  fetchRaceEntriesFromD1,
  isConfirmedPastRaceRequest,
  loadInitialRealtimePayloadServer,
  resolveHorseWeights,
} from "./realtime-payload.server";

type AnyMockFn = (...args: never[]) => unknown;
type HotFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

interface PreparedStub {
  all: ReturnType<typeof vi.fn<AnyMockFn>>;
  bind: ReturnType<typeof vi.fn<AnyMockFn>>;
  first: ReturnType<typeof vi.fn<AnyMockFn>>;
  run: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface D1Stub {
  prepare: ReturnType<typeof vi.fn<AnyMockFn>>;
}

const isPreparedStatement = (value: unknown): value is PcKeibaD1PreparedStatement =>
  typeof value === "object" &&
  value !== null &&
  "bind" in value &&
  typeof value.bind === "function";

const emptyBatch = <T = unknown>(): Promise<PcKeibaD1Result<T>[]> => Promise.resolve([]);
const noopExec = (): Promise<PcKeibaD1RunResult> => Promise.resolve({ success: true });

const buildD1WithRows = (rows: unknown[]): PcKeibaD1Database => {
  const all = vi.fn<AnyMockFn>().mockResolvedValue({ results: rows, success: true });
  const bind = vi.fn<AnyMockFn>();
  const first = vi.fn<AnyMockFn>().mockResolvedValue(null);
  const run = vi.fn<AnyMockFn>().mockResolvedValue({ success: true });
  const prepared: PreparedStub = { all, bind, first, run };
  bind.mockReturnValue(prepared);
  const raw: D1Stub = { prepare: vi.fn<AnyMockFn>().mockReturnValue(prepared) };
  const typedPrepare = (query: string): PcKeibaD1PreparedStatement => {
    const result = Reflect.apply(raw.prepare, raw, [query]);
    if (!isPreparedStatement(result)) {
      throw new Error("Stub returned an invalid prepared statement");
    }
    return result;
  };
  return { batch: emptyBatch, exec: noopExec, prepare: typedPrepare };
};

const buildD1Throwing = (): PcKeibaD1Database => ({
  batch: emptyBatch,
  exec: noopExec,
  prepare: (): PcKeibaD1PreparedStatement => {
    throw new Error("d1 down");
  },
});

it("buildRaceKey pads single-digit raceNumber to 2 chars for jra", () => {
  expect(
    buildRaceKey({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "1",
      source: "jra",
      year: "2026",
    }),
  ).toBe("jra:2026:0529:05:01");
});

it("buildRaceKey leaves 2-digit raceNumber unchanged for nar", () => {
  expect(
    buildRaceKey({
      day: "29",
      keibajoCode: "47",
      month: "05",
      raceNumber: "12",
      source: "nar",
      year: "2026",
    }),
  ).toBe("nar:2026:0529:47:12");
});

it("fetchOddsFromHot returns null when binding is undefined", async () => {
  const result = await fetchOddsFromHot(undefined, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot fetches and parses ok payload", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T07:30:00.000Z",
          history: [],
          historyByType: {},
          latest: { tansho: [{ combination: "1", odds: 1.5 }] },
          raceKey: "jra:2026:0529:05:01",
        }),
        { status: 200 },
      ),
    ),
  );
  const result = await fetchOddsFromHot({ fetch: fetchMock }, "jra:2026:0529:05:01");
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T07:30:00.000Z",
    history: [],
    historyByType: {},
    latest: { tansho: [{ combination: "1", odds: 1.5 }] },
    raceKey: "jra:2026:0529:05:01",
  });
  expect(fetchMock).toHaveBeenCalledWith(
    "https://sync-realtime-data-hot.kkk4oru.com/api/odds/jra%3A2026%3A0529%3A05%3A01",
  );
});

it("fetchOddsFromHot returns null when hot payload raceKey does not match the request", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T07:30:00.000Z",
          history: [],
          historyByType: {},
          latest: { tansho: [{ combination: "1", odds: 1.5 }] },
          raceKey: "jra:2026:0529:05:02",
        }),
        { status: 200 },
      ),
    ),
  );
  const result = await fetchOddsFromHot({ fetch: fetchMock }, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot returns null when hot worker is non-2xx", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response("upstream", { status: 502 })),
  );
  const result = await fetchOddsFromHot({ fetch: fetchMock }, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot returns null when fetch throws", async () => {
  const fetchMock = vi.fn<HotFetch>(async () => {
    throw new Error("boom");
  });
  const result = await fetchOddsFromHot({ fetch: fetchMock }, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot returns null when payload shape is invalid", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response(JSON.stringify({ wrong: true }), { status: 200 })),
  );
  const result = await fetchOddsFromHot({ fetch: fetchMock }, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchHorseWeightsLatest returns parsed snapshot when DO returns 200", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T08:00:00.000Z",
          horses: [
            {
              changeAmount: -2,
              changeSign: "-",
              horseName: "Alpha",
              horseNumber: "1",
              weight: 460,
            },
          ],
        }),
        { status: 200 },
      ),
    ),
  );
  const result = await fetchHorseWeightsLatest({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "01",
    realtimeData: { fetch: fetchMock },
    source: "jra",
    year: "2026",
  });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T08:00:00.000Z",
    horses: [
      {
        changeAmount: -2,
        changeSign: "-",
        horseName: "Alpha",
        horseNumber: "1",
        weight: 460,
      },
    ],
  });
});

it("fetchHorseWeightsLatest returns null when DO returns non-200", async () => {
  const fetchMock = vi.fn<HotFetch>(async () => Promise.resolve(new Response("", { status: 204 })));
  const result = await fetchHorseWeightsLatest({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "01",
    realtimeData: { fetch: fetchMock },
    source: "jra",
    year: "2026",
  });
  expect(result).toBeNull();
});

it("fetchHorseWeightsLatest returns null when DO returns malformed json", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response(JSON.stringify({ foo: "bar" }), { status: 200 })),
  );
  const result = await fetchHorseWeightsLatest({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "01",
    realtimeData: { fetch: fetchMock },
    source: "jra",
    year: "2026",
  });
  expect(result).toBeNull();
});

it("fetchHorseWeightsLatest treats an empty DO snapshot as unavailable", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T08:00:00.000Z",
          horses: [],
        }),
        { status: 200 },
      ),
    ),
  );
  const result = await fetchHorseWeightsLatest({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "01",
    realtimeData: { fetch: fetchMock },
    source: "jra",
    year: "2026",
  });
  expect(result).toBeNull();
});

it("fetchHorseWeightsLatest returns null when fetch throws", async () => {
  const fetchMock = vi.fn<HotFetch>(async () => {
    throw new Error("offline");
  });
  const result = await fetchHorseWeightsLatest({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "01",
    realtimeData: { fetch: fetchMock },
    source: "jra",
    year: "2026",
  });
  expect(result).toBeNull();
});

it("resolveHorseWeights returns DO snapshot directly when available", async () => {
  const result = await resolveHorseWeights({
    db: buildD1WithRows([]),
    fromDO: {
      fetchedAt: "2026-05-29T09:00:00.000Z",
      horses: [
        {
          changeAmount: 0,
          changeSign: " ",
          horseName: "FromDO",
          horseNumber: "1",
          weight: 500,
        },
      ],
    },
    raceKey: "jra:2026:0529:05:01",
  });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T09:00:00.000Z",
    horses: [
      {
        changeAmount: 0,
        changeSign: " ",
        horseName: "FromDO",
        horseNumber: "1",
        weight: 500,
      },
    ],
  });
});

it("resolveHorseWeights returns null when fromDO is null and db is missing", async () => {
  const result = await resolveHorseWeights({
    db: undefined,
    fromDO: null,
    raceKey: "jra:2026:0529:05:01",
  });
  expect(result).toBeNull();
});

it("resolveHorseWeights falls back to D1 when DO is null", async () => {
  const db = buildD1WithRows([
    {
      change_amount: -2,
      change_sign: "-",
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: "Alpha",
      horse_number: "2",
      weight: 460,
    },
  ]);
  const result = await resolveHorseWeights({
    db,
    fromDO: null,
    raceKey: "jra:2026:0529:05:01",
  });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T08:00:00.000Z",
    horses: [
      {
        changeAmount: -2,
        changeSign: "-",
        horseName: "Alpha",
        horseNumber: "2",
        weight: 460,
      },
    ],
  });
});

it("resolveHorseWeights falls back to D1 when DO snapshot has no horses", async () => {
  const db = buildD1WithRows([
    {
      change_amount: 4,
      change_sign: "+",
      fetched_at: "2026-05-29T08:05:00.000Z",
      horse_name: "Fallback",
      horse_number: "3",
      weight: 488,
    },
  ]);
  const result = await resolveHorseWeights({
    db,
    fromDO: {
      fetchedAt: "2026-05-29T08:00:00.000Z",
      horses: [],
    },
    raceKey: "jra:2026:0529:05:01",
  });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T08:05:00.000Z",
    horses: [
      {
        changeAmount: 4,
        changeSign: "+",
        horseName: "Fallback",
        horseNumber: "3",
        weight: 488,
      },
    ],
  });
});

it("resolveHorseWeights returns null when D1 fallback throws", async () => {
  const result = await resolveHorseWeights({
    db: buildD1Throwing(),
    fromDO: null,
    raceKey: "jra:2026:0529:05:01",
  });
  expect(result).toBeNull();
});

it("buildRealtimePayloadForRequest returns degraded payload when env is null", async () => {
  const result = await buildRealtimePayloadForRequest({
    env: null,
    request: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    },
  });
  expect(result).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("buildRealtimePayloadForRequest merges odds and DO horseWeights when both are present", async () => {
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T07:30:00.000Z",
          history: [],
          historyByType: {},
          latest: { tansho: [{ combination: "1", odds: 1.5 }] },
        }),
        { status: 200 },
      ),
    ),
  );
  const realtimeFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T08:00:00.000Z",
          horses: [
            {
              changeAmount: -2,
              changeSign: "-",
              horseName: "Alpha",
              horseNumber: "2",
              weight: 460,
            },
          ],
        }),
        { status: 200 },
      ),
    ),
  );
  const result = await buildRealtimePayloadForRequest({
    env: { REALTIME_DATA: { fetch: realtimeFetch }, REALTIME_HOT: { fetch: hotFetch } },
    request: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    },
  });
  expect(result).toStrictEqual({
    horseWeights: {
      fetchedAt: "2026-05-29T08:00:00.000Z",
      horses: [
        {
          changeAmount: -2,
          changeSign: "-",
          horseName: "Alpha",
          horseNumber: "2",
          weight: 460,
        },
      ],
    },
    odds: {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [],
      historyByType: {},
      horseTrends: [],
      latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      trendsByType: {},
    },
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("buildRealtimePayloadForRequest returns odds-only payload when DO has nothing and D1 binding is missing", async () => {
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T07:30:00.000Z",
          history: [],
          historyByType: {},
          latest: {},
        }),
        { status: 200 },
      ),
    ),
  );
  const result = await buildRealtimePayloadForRequest({
    env: { REALTIME_HOT: { fetch: hotFetch } },
    request: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    },
  });
  expect(result).toStrictEqual({
    horseWeights: null,
    odds: {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [],
      historyByType: {},
      horseTrends: [],
      latest: {},
      trendsByType: {},
    },
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("loadInitialRealtimePayloadServer returns full payload when bindings respond fast", async () => {
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T07:30:00.000Z",
          history: [],
          historyByType: {},
          latest: { tansho: [{ combination: "1", odds: 1.5 }] },
        }),
        { status: 200 },
      ),
    ),
  );
  const result = await loadInitialRealtimePayloadServer({
    env: { REALTIME_HOT: { fetch: hotFetch } },
    now: new Date("2026-05-29T09:00:00+09:00"),
    request: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    },
    timeoutMs: 5_000,
  });
  expect(result).toStrictEqual({
    horseWeights: null,
    odds: {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [],
      historyByType: {},
      horseTrends: [],
      latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      trendsByType: {},
    },
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("loadInitialRealtimePayloadServer returns null when bindings exceed timeout", async () => {
  const slowFetch = vi.fn<HotFetch>(
    async () =>
      new Promise((resolve) => {
        setTimeout(() => {
          resolve(new Response("{}", { status: 200 }));
        }, 100);
      }),
  );
  const result = await loadInitialRealtimePayloadServer({
    env: { REALTIME_HOT: { fetch: slowFetch } },
    now: new Date("2026-05-29T09:00:00+09:00"),
    request: {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    },
    timeoutMs: 5,
  });
  expect(result).toBeNull();
});

it("loadInitialRealtimePayloadServer returns degraded payload when env is null", async () => {
  const result = await loadInitialRealtimePayloadServer({
    env: null,
    now: new Date("2026-05-29T09:00:00+09:00"),
    request: {
      day: "29",
      keibajoCode: "47",
      month: "05",
      raceNumber: "01",
      source: "nar",
      year: "2026",
    },
    timeoutMs: 1_000,
  });
  expect(result).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "nar:2026:0529:47:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("loadInitialRealtimePayloadServer uses default timeout when none is provided", async () => {
  const result = await loadInitialRealtimePayloadServer({
    env: null,
    now: new Date("2026-05-29T09:00:00+09:00"),
    request: {
      day: "29",
      keibajoCode: "47",
      month: "05",
      raceNumber: "01",
      source: "nar",
      year: "2026",
    },
  });
  expect(result).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "nar:2026:0529:47:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("isConfirmedPastRaceRequest returns true for a request date before JST today", () => {
  const result = isConfirmedPastRaceRequest(
    { day: "28", keibajoCode: "05", month: "05", raceNumber: "01", source: "jra", year: "2026" },
    new Date("2026-05-29T09:00:00+09:00"),
  );
  expect(result).toBe(true);
});

it("isConfirmedPastRaceRequest returns false for a request date equal to JST today", () => {
  const result = isConfirmedPastRaceRequest(
    { day: "29", keibajoCode: "05", month: "05", raceNumber: "01", source: "jra", year: "2026" },
    new Date("2026-05-29T09:00:00+09:00"),
  );
  expect(result).toBe(false);
});

it("isConfirmedPastRaceRequest returns false for a request date after JST today", () => {
  const result = isConfirmedPastRaceRequest(
    { day: "30", keibajoCode: "05", month: "05", raceNumber: "01", source: "jra", year: "2026" },
    new Date("2026-05-29T09:00:00+09:00"),
  );
  expect(result).toBe(false);
});

it("isConfirmedPastRaceRequest defaults now to the current date when omitted", () => {
  const result = isConfirmedPastRaceRequest({
    day: "01",
    keibajoCode: "05",
    month: "01",
    raceNumber: "01",
    source: "jra",
    year: "2000",
  });
  expect(result).toBe(true);
});

it("loadInitialRealtimePayloadServer returns null immediately for a confirmed past race without calling bindings", async () => {
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response("{}", { status: 200 })),
  );
  const result = await loadInitialRealtimePayloadServer({
    env: { REALTIME_HOT: { fetch: hotFetch } },
    now: new Date("2026-05-29T09:00:00+09:00"),
    request: {
      day: "28",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      source: "jra",
      year: "2026",
    },
    timeoutMs: 5_000,
  });
  expect(result).toBeNull();
  expect(hotFetch).not.toHaveBeenCalled();
});

// Status rows transcribed from the saved production evidence:
// .cache/statistics-rollout-20261004/official-entry-check/entry-snapshots.json.
it.each([
  {
    raceNumber: "1",
    horseNumber: "6",
    horseName: "マーベラスバンブー",
    jockeyName: "田知弘",
    status: "出走取消",
    fetchedAt: "2026-10-04T10:01:54+09:00",
  },
  {
    raceNumber: "3",
    horseNumber: "7",
    horseName: "カロヤカ",
    jockeyName: "加藤聡",
    status: "競走除外",
    fetchedAt: "2026-10-04T11:12:07+09:00",
  },
])(
  "delivers saved $status through both shared API and SSR loaders",
  async ({ raceNumber, horseNumber, horseName, jockeyName, status, fetchedAt }) => {
    const db = buildD1WithRows([
      {
        fetched_at: fetchedAt,
        horse_number: horseNumber,
        horse_name: horseName,
        jockey_name: jockeyName,
        status,
      },
    ]);
    const originalPrepare = db.prepare.bind(db);
    const prepare = vi.spyOn(db, "prepare");
    // The entry source must work even when weights have no snapshot.
    prepare.mockImplementation((query) =>
      query.includes("horse_weight_snapshots")
        ? buildD1WithRows([]).prepare(query)
        : originalPrepare(query),
    );
    const request = {
      day: "04",
      month: "10",
      year: "2026",
      keibajoCode: "46",
      raceNumber,
      source: "nar",
    } satisfies Parameters<typeof buildRaceKey>[0];
    const api = await buildRealtimePayloadForRequest({ env: { REALTIME_DB: db }, request });
    const ssr = await loadInitialRealtimePayloadServer({
      env: { REALTIME_DB: db },
      request,
      now: new Date("2026-10-04T12:00:00+09:00"),
      timeoutMs: 100,
    });
    expect(api.raceEntries).toMatchObject({
      fetchedAt,
      horses: [{ fetchedAt, horseNumber, horseName, jockeyName, status }],
    });
    expect(ssr?.raceEntries).toMatchObject({
      fetchedAt,
      horses: [{ fetchedAt, horseNumber, horseName, jockeyName, status }],
    });
    expect(api.horseWeights).toBeNull();
    expect(api.odds).toBeNull();
  },
);

it("queries only the latest cohort for the bound race using the existing index", async () => {
  using sqlite = new DatabaseSync(":memory:");
  sqlite.exec(`
    create table race_entry_snapshots (race_key text, fetched_at text, horse_number text, horse_name text, jockey_name text, status text);
    create index idx_race_entry_snapshots_race_time_horse on race_entry_snapshots (race_key, fetched_at, horse_number);
    insert into race_entry_snapshots values
      ('nar:2026:1004:46:01', '2026-10-04T09:00:00+09:00', '6', null, null, '出走取消'),
      ('nar:2026:1004:46:01', '2026-10-04T10:01:54+09:00', '10', null, null, null),
      ('nar:2026:1004:46:01', '2026-10-04T10:01:54+09:00', '6', 'マーベラスバンブー', '田知弘', '出走取消'),
      ('nar:2026:1004:46:01', '2026-10-04T10:01:54+09:00', '2', null, null, null),
      ('nar:2026:1004:46:01', '2026-10-04T09:00:00+09:00', '1', null, null, '競走除外'),
      ('nar:2026:1004:46:03', '2026-10-04T11:12:07+09:00', '7', 'カロヤカ', '加藤聡', '競走除外');
  `);
  const db = buildD1WithRows([]);
  const prepare = vi.spyOn(db, "prepare").mockImplementation((query) => {
    const statement = buildD1WithRows([]).prepare(query);
    vi.spyOn(statement, "bind").mockImplementation((...values) => {
      expect(values).toStrictEqual(["nar:2026:1004:46:01", "nar:2026:1004:46:01"]);
      return buildD1WithRows(
        sqlite.prepare(query).all("nar:2026:1004:46:01", "nar:2026:1004:46:01"),
      ).prepare(query);
    });
    const plan = sqlite
      .prepare(`explain query plan ${query}`)
      .all("nar:2026:1004:46:01", "nar:2026:1004:46:01");
    expect(
      plan.filter((row) => String(row.detail).includes("SEARCH race_entry_snapshots USING")).length,
    ).toBe(2);
    return statement;
  });
  expect(await fetchRaceEntriesFromD1(db, "nar:2026:1004:46:01")).toStrictEqual({
    fetchedAt: "2026-10-04T10:01:54+09:00",
    horses: [
      {
        fetchedAt: "2026-10-04T10:01:54+09:00",
        horseNumber: "2",
        horseName: null,
        jockeyName: null,
        status: null,
      },
      {
        fetchedAt: "2026-10-04T10:01:54+09:00",
        horseNumber: "6",
        horseName: "マーベラスバンブー",
        jockeyName: "田知弘",
        status: "出走取消",
      },
      {
        fetchedAt: "2026-10-04T10:01:54+09:00",
        horseNumber: "10",
        horseName: null,
        jockeyName: null,
        status: null,
      },
    ],
  });
  expect(prepare).toHaveBeenCalledTimes(1);
});

it("keeps absent entry source, empty rows, and D1 failures unknown", async () => {
  expect(await fetchRaceEntriesFromD1(undefined, "nar:2026:1004:46:01")).toBeNull();
  expect(await fetchRaceEntriesFromD1(buildD1WithRows([]), "nar:2026:1004:46:01")).toBeNull();
  expect(await fetchRaceEntriesFromD1(buildD1Throwing(), "nar:2026:1004:46:01")).toBeNull();
  const db = buildD1WithRows([]);
  vi.spyOn(db, "prepare").mockImplementation((query) => {
    const statement = buildD1WithRows([]).prepare(query);
    vi.spyOn(statement, "all").mockRejectedValue(new Error("D1 query failed"));
    return statement;
  });
  expect(await fetchRaceEntriesFromD1(db, "nar:2026:1004:46:01")).toBeNull();
});

it("normalizes missing optional entry fields without inventing cancellation", async () => {
  expect(
    await fetchRaceEntriesFromD1(
      buildD1WithRows([{ fetched_at: "2026-10-04T10:01:54+09:00", horse_number: "6" }]),
      "nar:2026:1004:46:01",
    ),
  ).toStrictEqual({
    fetchedAt: "2026-10-04T10:01:54+09:00",
    horses: [
      {
        fetchedAt: "2026-10-04T10:01:54+09:00",
        horseNumber: "6",
        horseName: null,
        jockeyName: null,
        status: null,
      },
    ],
  });
  expect(
    await fetchRaceEntriesFromD1(
      buildD1WithRows([{ fetched_at: "", horse_number: "6" }]),
      "nar:2026:1004:46:01",
    ),
  ).toBeNull();
});

it("rejects unsuccessful D1 results even if rows are returned", async () => {
  const db = buildD1WithRows([]);
  vi.spyOn(db, "prepare").mockImplementation((query) => {
    const statement = buildD1WithRows([]).prepare(query);
    vi.spyOn(statement, "all").mockResolvedValue({
      results: [{ fetched_at: "2026-10-04T10:01:54+09:00", horse_number: "6", status: "出走取消" }],
      success: false,
    });
    return statement;
  });
  expect(await fetchRaceEntriesFromD1(db, "nar:2026:1004:46:01")).toBeNull();
});

it("bounds SSR latency while the entry snapshot query remains pending", async () => {
  vi.useFakeTimers();
  try {
    const db = buildD1WithRows([]);
    const prepare = vi.spyOn(db, "prepare").mockImplementation((query) => {
      const statement = buildD1WithRows([]).prepare(query);
      vi.spyOn(statement, "all").mockImplementation(() => new Promise(() => {}));
      return statement;
    });
    const pending = loadInitialRealtimePayloadServer({
      env: { REALTIME_DB: db },
      request: {
        day: "04",
        month: "10",
        year: "2026",
        keibajoCode: "46",
        raceNumber: "1",
        source: "nar",
      },
      now: new Date("2026-10-04T12:00:00+09:00"),
      timeoutMs: 5,
    });
    await vi.advanceTimersByTimeAsync(5);
    expect(await pending).toBeNull();
    expect(prepare).toHaveBeenCalledTimes(1);
  } finally {
    vi.useRealTimers();
  }
});

it.each([false, true])(
  "preserves hot odds and DO weights when entry D1 fails=%s",
  async (fails) => {
    const db = fails
      ? buildD1Throwing()
      : buildD1WithRows([
          { fetched_at: "2026-10-04T10:01:54+09:00", horse_number: "6", status: "出走取消" },
        ]);
    const prepare = vi.spyOn(db, "prepare");
    const result = await buildRealtimePayloadForRequest({
      env: {
        REALTIME_DB: db,
        REALTIME_HOT: {
          fetch: vi.fn<HotFetch>().mockResolvedValue(
            new Response(
              JSON.stringify({
                fetchedAt: "2026-10-04T10:02:00+09:00",
                latest: { tansho: [{ combination: "2", odds: 1.5 }] },
                history: [],
                historyByType: {},
              }),
            ),
          ),
        },
        REALTIME_DATA: {
          fetch: vi.fn<HotFetch>().mockResolvedValue(
            new Response(
              JSON.stringify({
                fetchedAt: "2026-10-04T10:03:00+09:00",
                horses: [
                  {
                    horseNumber: "2",
                    horseName: "エムティモートン",
                    weight: 460,
                    changeSign: "-",
                    changeAmount: 2,
                  },
                ],
              }),
            ),
          ),
        },
      },
      request: {
        day: "04",
        month: "10",
        year: "2026",
        keibajoCode: "46",
        raceNumber: "1",
        source: "nar",
      },
    });
    expect(result.odds).toStrictEqual({
      fetchedAt: "2026-10-04T10:02:00+09:00",
      latest: { tansho: [{ combination: "2", odds: 1.5 }] },
      history: [],
      historyByType: {},
      horseTrends: [],
      trendsByType: {},
    });
    expect(result.horseWeights).toStrictEqual({
      fetchedAt: "2026-10-04T10:03:00+09:00",
      horses: [
        {
          horseNumber: "2",
          horseName: "エムティモートン",
          weight: 460,
          changeSign: "-",
          changeAmount: 2,
        },
      ],
    });
    expect(result).toMatchObject({
      raceEntries: fails ? null : { horses: [{ status: "出走取消" }] },
    });
    // Only entries use D1 when the DO already supplied weights.
    expect(prepare).toHaveBeenCalledTimes(1);
    expect(prepare.mock.calls[0]?.[0]).toMatch(/from race_entry_snapshots/);
  },
);

it("preserves hot official odds and histories for Arc instead of replacing them with a card", async () => {
  const hotFetch = vi.fn<HotFetch>(async () =>
    Response.json({
      raceKey: "jra:2026:1004:A8:05",
      fetchedAt: "2026-10-04T19:06:26+09:00",
      latest: {
        tansho: [{ combination: "1", odds: 3.7, rank: 2 }],
        fukusho: [{ combination: "1", minOdds: 1.5, maxOdds: 2.1 }],
      },
      history: [
        { horseNumber: "1", points: [{ fetchedAt: "2026-10-04T19:06:26+09:00", odds: 3.7 }] },
      ],
      historyByType: {},
    }),
  );
  const payload = await buildRealtimePayloadForRequest({
    env: { REALTIME_HOT: { fetch: hotFetch } },
    request: {
      source: "jra",
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "A8",
      raceNumber: "05",
    },
  });
  expect(payload.odds?.latest.tansho).toStrictEqual([{ combination: "1", odds: 3.7, rank: 2 }]);
  expect(payload.odds?.horseTrends).toStrictEqual([
    { horseNumber: "1", points: [{ fetchedAt: "2026-10-04T19:06:26+09:00", odds: 3.7 }] },
  ]);
  expect(payload.odds?.latest.fukusho).toStrictEqual([
    { combination: "1", minOdds: 1.5, maxOdds: 2.1 },
  ]);
});

it("fails closed for Arc when the native official reader is unavailable", async () => {
  const payload = await buildRealtimePayloadForRequest({
    env: null,
    request: {
      source: "jra",
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "A8",
      raceNumber: "05",
    },
  });
  expect(payload.odds).toBeNull();
});

vi.mock("./arc-official-card.server", () => ({
  loadOfficialCard: vi.fn<() => Promise<unknown>>().mockResolvedValue(null),
}));
import officialEvidence from "./__fixtures__/arc-official-card.json";
import { loadOfficialCard } from "./arc-official-card.server";
it.each([false, true])(
  "preserves Arc cancellation entries when official prices are available=%s",
  async (available) => {
    vi.mocked(loadOfficialCard).mockResolvedValueOnce(available ? officialEvidence.card : null);
    const payload = await buildRealtimePayloadForRequest({
      env: {
        REALTIME_DB: buildD1WithRows([
          { fetched_at: "2026-10-04T06:00:00Z", horse_number: "6", status: "出走取消" },
        ]),
      },
      request: {
        source: "jra",
        year: "2026",
        month: "10",
        day: "04",
        keibajoCode: "A8",
        raceNumber: "05",
      },
    });
    expect(payload.raceEntries).toMatchObject({
      horses: [{ horseNumber: "6", status: "出走取消" }],
    });
  },
);
it("exposes all official single-win prices and popularity in existing units without invented bets", async () => {
  vi.mocked(loadOfficialCard).mockResolvedValueOnce(officialEvidence.card);
  const payload = await buildRealtimePayloadForRequest({
    env: null,
    request: {
      source: "jra",
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "A8",
      raceNumber: "05",
    },
  });
  expect(
    payload.odds?.latest.tansho?.map((r) => [Number(r.combination), r.odds, r.rank]),
  ).toStrictEqual([
    [1, 4.3, 2],
    [2, 65.9, 13],
    [3, 122.4, 15],
    [4, 10.8, 4],
    [5, 2.5, 1],
    [6, 137.2, 16],
    [7, 45.6, 12],
    [8, 44, 11],
    [9, 7.9, 3],
    [10, 32.3, 10],
    [11, 16.5, 6],
    [12, 25.8, 9],
    [13, 83.4, 14],
    [14, 12, 5],
    [15, 17, 7],
    [16, 20.2, 8],
  ]);
  expect(payload.odds?.latest.tansho?.[0]?.combination).toBe("1");
  expect(Object.keys(payload.odds?.latest ?? {})).toStrictEqual(["tansho"]);
  expect(payload.odds?.history).toStrictEqual([]);
  expect(payload.odds?.fetchedAt).toBe("2026-10-04T06:17:19.144Z");
});
