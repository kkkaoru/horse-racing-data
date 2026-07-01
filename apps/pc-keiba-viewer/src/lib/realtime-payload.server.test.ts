// Run with bun. `bun run --filter pc-keiba-viewer test`
import { expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

import {
  buildRaceKey,
  buildRealtimePayloadForRequest,
  fetchHorseWeightsLatest,
  fetchOddsFromHot,
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

interface HorseWeightSnapshotRow {
  horse_number: string;
  horse_name: string | null;
  weight: number | null;
  change_sign: string | null;
  change_amount: number | null;
  fetched_at: string;
}

const isPreparedStatement = (value: unknown): value is PcKeibaD1PreparedStatement =>
  typeof value === "object" &&
  value !== null &&
  "bind" in value &&
  typeof value.bind === "function";

const emptyBatch = <T = unknown>(): Promise<PcKeibaD1Result<T>[]> => Promise.resolve([]);
const noopExec = (): Promise<PcKeibaD1RunResult> => Promise.resolve({ success: true });

const buildD1WithRows = (rows: HorseWeightSnapshotRow[]): PcKeibaD1Database => {
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
  });
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
