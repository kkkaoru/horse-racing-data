// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

type AnyMockFn = (...args: never[]) => unknown;
type HotFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

const mocks = vi.hoisted(() => ({
  buildDevRealtimePayloadMock: vi.fn<(...args: never[]) => unknown>(),
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
  isDevScraperEnabledMock: vi.fn<() => boolean>(),
}));

vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: mocks.getCloudflareContextMock,
}));

vi.mock("../../../../../../../../../lib/dev-realtime-scraper.server", () => ({
  buildDevRealtimePayload: mocks.buildDevRealtimePayloadMock,
  isDevScraperEnabled: mocks.isDevScraperEnabledMock,
}));

const { buildDevRealtimePayloadMock, getCloudflareContextMock, isDevScraperEnabledMock } = mocks;

import {
  GET,
  buildRaceKey,
  buildRealtimePayloadFromHot,
  fetchOddsFromHot,
  type HotOddsPayload,
} from "./route";

interface HotStub {
  fetch: ReturnType<typeof vi.fn<HotFetch>>;
}

const buildOkResponseFor = (payload: HotOddsPayload): Response =>
  new Response(JSON.stringify(payload), { status: 200 });

const buildErrorResponse = (): Response => new Response("upstream error", { status: 502 });

beforeEach(() => {
  getCloudflareContextMock.mockReset();
  buildDevRealtimePayloadMock.mockReset();
  isDevScraperEnabledMock.mockReset();
  isDevScraperEnabledMock.mockReturnValue(false);
});

afterEach(() => {
  vi.unstubAllGlobals();
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

it("fetchOddsFromHot returns null when REALTIME_HOT binding is undefined", async () => {
  const result = await fetchOddsFromHot(undefined, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot calls binding fetch with hot worker URL and returns parsed json", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const hot: HotStub = { fetch: fetchMock };
  const result = await fetchOddsFromHot(hot, "jra:2026:0529:05:01");
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T07:30:00.000Z",
    history: [],
    historyByType: {},
    latest: { tansho: [{ combination: "1", odds: 1.5 }] },
  });
  expect(fetchMock).toHaveBeenCalledWith(
    "https://sync-realtime-data-hot.kkk4oru.com/api/odds/jra:2026:0529:05:01",
  );
});

it("fetchOddsFromHot accepts payload populated with grouped history and historyByType", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [
          {
            horseNumber: "1",
            points: [
              {
                fetchedAt: "2026-05-29T07:30:00.000Z",
                horseNumber: "1",
                odds: 1.5,
                popularity: 1,
              },
            ],
          },
        ],
        historyByType: {
          tansho: [
            {
              combination: "1",
              points: [
                {
                  combination: "1",
                  fetchedAt: "2026-05-29T07:30:00.000Z",
                  odds: 1.5,
                  rank: 1,
                },
              ],
            },
          ],
        },
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const hot: HotStub = { fetch: fetchMock };
  const result = await fetchOddsFromHot(hot, "jra:2026:0529:05:01");
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-29T07:30:00.000Z",
    history: [
      {
        horseNumber: "1",
        points: [
          {
            fetchedAt: "2026-05-29T07:30:00.000Z",
            horseNumber: "1",
            odds: 1.5,
            popularity: 1,
          },
        ],
      },
    ],
    historyByType: {
      tansho: [
        {
          combination: "1",
          points: [
            {
              combination: "1",
              fetchedAt: "2026-05-29T07:30:00.000Z",
              odds: 1.5,
              rank: 1,
            },
          ],
        },
      ],
    },
    latest: { tansho: [{ combination: "1", odds: 1.5 }] },
  });
});

it("fetchOddsFromHot returns null when history field is not an array", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T07:30:00.000Z",
          history: "not-an-array",
          historyByType: {},
          latest: {},
        }),
        { status: 200 },
      ),
    ),
  );
  const hot: HotStub = { fetch: fetchMock };
  const result = await fetchOddsFromHot(hot, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot returns null when historyByType field is null", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: "2026-05-29T07:30:00.000Z",
          history: [],
          historyByType: null,
          latest: {},
        }),
        { status: 200 },
      ),
    ),
  );
  const hot: HotStub = { fetch: fetchMock };
  const result = await fetchOddsFromHot(hot, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot returns null when fetchedAt has wrong type", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
          fetchedAt: 1234,
          history: [],
          historyByType: {},
          latest: {},
        }),
        { status: 200 },
      ),
    ),
  );
  const hot: HotStub = { fetch: fetchMock };
  const result = await fetchOddsFromHot(hot, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot returns null when hot worker responds with non-2xx", async () => {
  const fetchMock = vi.fn<HotFetch>(async () => Promise.resolve(buildErrorResponse()));
  const hot: HotStub = { fetch: fetchMock };
  const result = await fetchOddsFromHot(hot, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot returns null when hot worker fetch throws", async () => {
  const fetchMock = vi.fn<HotFetch>(async () => {
    throw new Error("boom");
  });
  const hot: HotStub = { fetch: fetchMock };
  const result = await fetchOddsFromHot(hot, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("fetchOddsFromHot returns null when response body fails shape validation", async () => {
  const fetchMock = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response(JSON.stringify({ unexpected: true }), { status: 200 })),
  );
  const hot: HotStub = { fetch: fetchMock };
  const result = await fetchOddsFromHot(hot, "jra:2026:0529:05:01");
  expect(result).toBeNull();
});

it("buildRealtimePayloadFromHot returns degraded payload when odds is null", () => {
  expect(buildRealtimePayloadFromHot("jra:2026:0529:05:01", null)).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("buildRealtimePayloadFromHot returns degraded payload when fetchedAt is null", () => {
  expect(
    buildRealtimePayloadFromHot("jra:2026:0529:05:01", {
      fetchedAt: null,
      history: [],
      historyByType: {},
      latest: {},
    }),
  ).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("buildRealtimePayloadFromHot returns empty arrays for history/horseTrends when hot worker payload is empty", () => {
  expect(
    buildRealtimePayloadFromHot("jra:2026:0529:05:01", {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [],
      historyByType: {},
      latest: { tansho: [{ combination: "1", odds: 1.5 }] },
    }),
  ).toStrictEqual({
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

it("buildRealtimePayloadFromHot passes through grouped history into horseTrends and flattens into history", () => {
  expect(
    buildRealtimePayloadFromHot("jra:2026:0529:05:01", {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [
        {
          horseNumber: "1",
          points: [
            {
              fetchedAt: "2026-05-29T07:25:00.000Z",
              horseNumber: "1",
              odds: 2.1,
              popularity: 2,
            },
            {
              fetchedAt: "2026-05-29T07:30:00.000Z",
              horseNumber: "1",
              odds: 1.8,
              popularity: 1,
            },
          ],
        },
        {
          horseNumber: "2",
          points: [
            {
              fetchedAt: "2026-05-29T07:30:00.000Z",
              horseNumber: "2",
              odds: 3.5,
              popularity: 3,
            },
          ],
        },
      ],
      historyByType: {},
      latest: { tansho: [{ combination: "1", odds: 1.8 }] },
    }),
  ).toStrictEqual({
    horseWeights: null,
    odds: {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [
        {
          fetchedAt: "2026-05-29T07:25:00.000Z",
          horseNumber: "1",
          odds: 2.1,
          popularity: 2,
        },
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseNumber: "1",
          odds: 1.8,
          popularity: 1,
        },
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseNumber: "2",
          odds: 3.5,
          popularity: 3,
        },
      ],
      historyByType: {},
      horseTrends: [
        {
          horseNumber: "1",
          points: [
            {
              fetchedAt: "2026-05-29T07:25:00.000Z",
              horseNumber: "1",
              odds: 2.1,
              popularity: 2,
            },
            {
              fetchedAt: "2026-05-29T07:30:00.000Z",
              horseNumber: "1",
              odds: 1.8,
              popularity: 1,
            },
          ],
        },
        {
          horseNumber: "2",
          points: [
            {
              fetchedAt: "2026-05-29T07:30:00.000Z",
              horseNumber: "2",
              odds: 3.5,
              popularity: 3,
            },
          ],
        },
      ],
      latest: { tansho: [{ combination: "1", odds: 1.8 }] },
      trendsByType: {},
    },
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("buildRealtimePayloadFromHot passes through historyByType into trendsByType and flattens into historyByType", () => {
  expect(
    buildRealtimePayloadFromHot("jra:2026:0529:05:01", {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [],
      historyByType: {
        fukusho: [
          {
            combination: "2",
            points: [
              {
                combination: "2",
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 1.2,
                rank: 1,
              },
            ],
          },
        ],
        tansho: [
          {
            combination: "1",
            points: [
              {
                combination: "1",
                fetchedAt: "2026-05-29T07:25:00.000Z",
                odds: 2.1,
                rank: 2,
              },
              {
                combination: "1",
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 1.8,
                rank: 1,
              },
            ],
          },
        ],
      },
      latest: {},
    }),
  ).toStrictEqual({
    horseWeights: null,
    odds: {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [],
      historyByType: {
        fukusho: [
          {
            combination: "2",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 1.2,
            rank: 1,
          },
        ],
        tansho: [
          {
            combination: "1",
            fetchedAt: "2026-05-29T07:25:00.000Z",
            odds: 2.1,
            rank: 2,
          },
          {
            combination: "1",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 1.8,
            rank: 1,
          },
        ],
      },
      horseTrends: [],
      latest: {},
      trendsByType: {
        fukusho: [
          {
            combination: "2",
            points: [
              {
                combination: "2",
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 1.2,
                rank: 1,
              },
            ],
          },
        ],
        tansho: [
          {
            combination: "1",
            points: [
              {
                combination: "1",
                fetchedAt: "2026-05-29T07:25:00.000Z",
                odds: 2.1,
                rank: 2,
              },
              {
                combination: "1",
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 1.8,
                rank: 1,
              },
            ],
          },
        ],
      },
    },
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("GET returns 400 when source query param is missing", async () => {
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(400);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "invalid source" });
});

it("GET returns 400 when source query param is unknown", async () => {
  const request = new Request(
    "https://example.com/api/races/2026/05/29/05/01/realtime?source=other",
  );
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(400);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "invalid source" });
});

it("GET takes the dev scraper branch when isDevScraperEnabled is true", async () => {
  isDevScraperEnabledMock.mockReturnValue(true);
  buildDevRealtimePayloadMock.mockResolvedValue({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "nar:2026:0529:47:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
  const fetchSpy = vi.fn<AnyMockFn>();
  vi.stubGlobal("fetch", fetchSpy);
  const request = new Request("https://example.com/api/races/2026/05/29/47/01/realtime?source=nar");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "47",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(buildDevRealtimePayloadMock).toHaveBeenCalledWith({
    day: "29",
    keibajoCode: "47",
    month: "05",
    raceNumber: "01",
    source: "nar",
    year: "2026",
  });
  expect(getCloudflareContextMock).not.toHaveBeenCalled();
  expect(fetchSpy).not.toHaveBeenCalled();
});

it("GET production branch calls REALTIME_HOT binding and wraps odds", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(hotFetch).toHaveBeenCalledWith(
    "https://sync-realtime-data-hot.kkk4oru.com/api/odds/jra:2026:0529:05:01",
  );
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
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

it("GET production branch passes hot worker grouped history end-to-end into viewer payload", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [
          {
            horseNumber: "1",
            points: [
              {
                fetchedAt: "2026-05-29T07:30:00.000Z",
                horseNumber: "1",
                odds: 1.5,
                popularity: 1,
              },
            ],
          },
        ],
        historyByType: {
          tansho: [
            {
              combination: "1",
              points: [
                {
                  combination: "1",
                  fetchedAt: "2026-05-29T07:30:00.000Z",
                  odds: 1.5,
                  rank: 1,
                },
              ],
            },
          ],
        },
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    horseWeights: null,
    odds: {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      history: [
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseNumber: "1",
          odds: 1.5,
          popularity: 1,
        },
      ],
      historyByType: {
        tansho: [
          {
            combination: "1",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 1.5,
            rank: 1,
          },
        ],
      },
      horseTrends: [
        {
          horseNumber: "1",
          points: [
            {
              fetchedAt: "2026-05-29T07:30:00.000Z",
              horseNumber: "1",
              odds: 1.5,
              popularity: 1,
            },
          ],
        },
      ],
      latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      trendsByType: {
        tansho: [
          {
            combination: "1",
            points: [
              {
                combination: "1",
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 1.5,
                rank: 1,
              },
            ],
          },
        ],
      },
    },
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("GET production branch returns degraded payload when REALTIME_HOT binding is missing", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: {} });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "jra:2026:0529:05:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("GET production branch returns degraded payload when hot worker is non-2xx", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () => Promise.resolve(buildErrorResponse()));
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/47/01/realtime?source=nar");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "47",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "nar:2026:0529:47:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("GET production branch returns degraded payload when hot worker throws", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () => {
    throw new Error("boom");
  });
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/47/01/realtime?source=nar");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "47",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "nar:2026:0529:47:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("GET production branch overlays horseWeights from REALTIME_DATA when latest endpoint returns 200", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
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
            {
              changeAmount: 4,
              changeSign: "+",
              horseName: "Beta",
              horseNumber: "10",
              weight: 482,
            },
          ],
        }),
        { status: 200 },
      ),
    ),
  );
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_DATA: { fetch: realtimeFetch }, REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
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
        {
          changeAmount: 4,
          changeSign: "+",
          horseName: "Beta",
          horseNumber: "10",
          weight: 482,
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
  expect(realtimeFetch).toHaveBeenCalledTimes(1);
});

it("GET production branch keeps horseWeights null when REALTIME_DATA returns non-200", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const realtimeFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response("not found", { status: 404 })),
  );
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_DATA: { fetch: realtimeFetch }, REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
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

it("GET production branch keeps horseWeights null when REALTIME_DATA returns 200 but malformed JSON", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const realtimeFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response(JSON.stringify({ foo: "bar" }), { status: 200 })),
  );
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_DATA: { fetch: realtimeFetch }, REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
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

it("GET production branch keeps horseWeights null when REALTIME_DATA fetch throws", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const realtimeFetch = vi.fn<HotFetch>(async () => {
    throw new Error("upstream offline");
  });
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_DATA: { fetch: realtimeFetch }, REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
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

interface HorseWeightSnapshotRow {
  horse_number: string;
  horse_name: string | null;
  weight: number | null;
  change_sign: string | null;
  change_amount: number | null;
  fetched_at: string;
}

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

it("GET production branch falls back to REALTIME_DB when REALTIME_DATA returns non-200", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const realtimeFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response("not found", { status: 204 })),
  );
  const realtimeDb = buildD1WithRows([
    {
      change_amount: -2,
      change_sign: "-",
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: "Alpha",
      horse_number: "2",
      weight: 460,
    },
  ]);
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: {
      REALTIME_DATA: { fetch: realtimeFetch },
      REALTIME_DB: realtimeDb,
      REALTIME_HOT: { fetch: hotFetch },
    },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
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

it("GET production branch falls back to REALTIME_DB when REALTIME_DATA service binding is missing", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const realtimeDb = buildD1WithRows([
    {
      change_amount: 4,
      change_sign: "+",
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: "Beta",
      horse_number: "10",
      weight: 482,
    },
    {
      change_amount: -2,
      change_sign: "-",
      fetched_at: "2026-05-29T08:00:00.000Z",
      horse_name: "Alpha",
      horse_number: "2",
      weight: 460,
    },
  ]);
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_DB: realtimeDb, REALTIME_HOT: { fetch: hotFetch } },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
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
        {
          changeAmount: 4,
          changeSign: "+",
          horseName: "Beta",
          horseNumber: "10",
          weight: 482,
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

it("GET production branch returns null horseWeights when both DO returns non-200 and D1 has no rows", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const realtimeFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(new Response("", { status: 204 })),
  );
  const realtimeDb = buildD1WithRows([]);
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: {
      REALTIME_DATA: { fetch: realtimeFetch },
      REALTIME_DB: realtimeDb,
      REALTIME_HOT: { fetch: hotFetch },
    },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
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

it("GET production branch uses DO snapshot directly without consulting REALTIME_DB when DO returns 200", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: { tansho: [{ combination: "1", odds: 1.5 }] },
      }),
    ),
  );
  const realtimeFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      new Response(
        JSON.stringify({
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
        }),
        { status: 200 },
      ),
    ),
  );
  const d1PrepareSpy = vi.fn<(query: string) => void>();
  const realtimeDb: PcKeibaD1Database = {
    batch: emptyBatch,
    exec: noopExec,
    prepare: (query: string): PcKeibaD1PreparedStatement => {
      d1PrepareSpy(query);
      throw new Error("D1 should not be consulted when DO returns a snapshot");
    },
  };
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: {
      REALTIME_DATA: { fetch: realtimeFetch },
      REALTIME_DB: realtimeDb,
      REALTIME_HOT: { fetch: hotFetch },
    },
  });
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({
    horseWeights: {
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
  expect(d1PrepareSpy).not.toHaveBeenCalled();
});

it("GET production branch does not invoke global fetch (legacy upstream is gone)", async () => {
  isDevScraperEnabledMock.mockReturnValue(false);
  const hotFetch = vi.fn<HotFetch>(async () =>
    Promise.resolve(
      buildOkResponseFor({
        fetchedAt: "2026-05-29T07:30:00.000Z",
        history: [],
        historyByType: {},
        latest: {},
      }),
    ),
  );
  getCloudflareContextMock.mockResolvedValue({
    ctx: null,
    env: { REALTIME_HOT: { fetch: hotFetch } },
  });
  const globalFetchSpy = vi.fn<AnyMockFn>();
  vi.stubGlobal("fetch", globalFetchSpy);
  const request = new Request("https://example.com/api/races/2026/05/29/05/01/realtime?source=jra");
  const response = await GET(request, {
    params: Promise.resolve({
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "01",
      year: "2026",
    }),
  });
  expect(response.status).toBe(200);
  expect(globalFetchSpy).not.toHaveBeenCalled();
  expect(hotFetch).toHaveBeenCalledTimes(1);
});
