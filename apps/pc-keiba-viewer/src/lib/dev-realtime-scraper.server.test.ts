// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { resetHistoryStore } from "./dev-realtime-history-store.server";
import { buildDevRealtimePayload, isDevScraperEnabled } from "./dev-realtime-scraper.server";

const RACE_LIST_URL =
  "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=2026%2F05%2F29&k_babaCode=23";
const DEBA_URL =
  "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=1&k_babaCode=23";
const TANSHO_FUKUSHO_URL = "https://www.keiba.go.jp/KeibaWeb/Odds/OddsTanFuku?k=1";

const RACE_LIST_HTML = `
  <a href="DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=1&k_babaCode=23">1</a>
  <a href="DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=2&k_babaCode=23">2</a>
`;

const RACE_DETAIL_HTML = `
  <h4>2026年5月29日（金）　川　崎　第1競走　16:30発走</h4>
  <section class="raceTitle">
    <h3>テストレース</h3>
  </section>
  <nav>
    <div></div><div></div>
    <div>
      <a href="../Odds/OddsTanFuku?k=1">単・複</a>
    </div>
  </nav>
  <tr class="tBorder">
    <td rowspan="5" class="horseNum">1</td>
    <td colspan="3"><a class="horseName">テスト馬A</a></td>
    <td><a class="jockeyName">騎手A<span class="jockeyarea">（川崎）</span></a></td>
  </tr>
  <tr class="tBorder">
    <td rowspan="5" class="horseNum">2</td>
    <td colspan="3"><a class="horseName">テスト馬B</a></td>
    <td><a class="jockeyName">騎手B<span class="jockeyarea">（船橋）</span></a></td>
  </tr>
`;

const TANSHO_FUKUSHO_BODY = `
  <tbody>
    <tr><td></td><td>1</td><td></td><td>1.5</td><td>1.5 - 2.5</td></tr>
    <tr><td></td><td>2</td><td></td><td>3.0</td><td>2.0 - 4.0</td></tr>
  </tbody>
`;

const FIXED_NOW_ISO = "2026-05-29T07:30:00.000Z";

const mockFetchHtml = (htmlByUrl: Record<string, string>): void => {
  vi.stubGlobal(
    "fetch",
    vi.fn((input: string | URL | Request) => {
      const url =
        typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      const value = htmlByUrl[url];
      const body = value ?? "";
      const status = value === undefined ? 404 : 200;
      return Promise.resolve(
        new Response(body, { headers: { "content-type": "text/html" }, status }),
      );
    }),
  );
};

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date(FIXED_NOW_ISO));
  resetHistoryStore();
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  resetHistoryStore();
});

it("isDevScraperEnabled is true when NODE_ENV=development and flag=1", () => {
  vi.stubEnv("NODE_ENV", "development");
  vi.stubEnv("PC_KEIBA_DEV_REALTIME_SCRAPER", "1");
  expect(isDevScraperEnabled()).toBe(true);
});

it("isDevScraperEnabled is false when NODE_ENV=production even with flag=1", () => {
  vi.stubEnv("NODE_ENV", "production");
  vi.stubEnv("PC_KEIBA_DEV_REALTIME_SCRAPER", "1");
  expect(isDevScraperEnabled()).toBe(false);
});

it("isDevScraperEnabled is false when NODE_ENV=development but flag is unset", () => {
  vi.stubEnv("NODE_ENV", "development");
  vi.stubEnv("PC_KEIBA_DEV_REALTIME_SCRAPER", "");
  expect(isDevScraperEnabled()).toBe(false);
});

it("isDevScraperEnabled is false when NODE_ENV=test with flag=1", () => {
  vi.stubEnv("NODE_ENV", "test");
  vi.stubEnv("PC_KEIBA_DEV_REALTIME_SCRAPER", "1");
  expect(isDevScraperEnabled()).toBe(false);
});

it("buildDevRealtimePayload returns NAR scraped payload with fixed odds and entries", async () => {
  mockFetchHtml({
    [RACE_LIST_URL]: RACE_LIST_HTML,
    [DEBA_URL]: RACE_DETAIL_HTML,
    [TANSHO_FUKUSHO_URL]: TANSHO_FUKUSHO_BODY,
  });
  const payload = await buildDevRealtimePayload({
    day: "29",
    keibajoCode: "47",
    month: "05",
    raceNumber: "01",
    source: "nar",
    year: "2026",
  });
  expect(payload).toStrictEqual({
    horseWeights: null,
    odds: {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      historyByType: {
        fukusho: [
          {
            combination: "1",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 2,
            rank: 1,
          },
          {
            combination: "2",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 3,
            rank: 2,
          },
        ],
        tansho: [
          {
            combination: "1",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 1.5,
            rank: 1,
          },
          {
            combination: "2",
            fetchedAt: "2026-05-29T07:30:00.000Z",
            odds: 3,
            rank: 2,
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
        {
          horseNumber: "2",
          points: [
            {
              fetchedAt: "2026-05-29T07:30:00.000Z",
              horseNumber: "2",
              odds: 3,
              popularity: 2,
            },
          ],
        },
      ],
      history: [
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseNumber: "1",
          odds: 1.5,
          popularity: 1,
        },
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseNumber: "2",
          odds: 3,
          popularity: 2,
        },
      ],
      latest: {
        fukusho: [
          { averageOdds: 2, combination: "1", maxOdds: 2.5, minOdds: 1.5, rank: 1 },
          { averageOdds: 3, combination: "2", maxOdds: 4, minOdds: 2, rank: 2 },
        ],
        tansho: [
          { combination: "1", odds: 1.5, rank: 1 },
          { combination: "2", odds: 3, rank: 2 },
        ],
      },
      trendsByType: {
        fukusho: [
          {
            combination: "1",
            points: [
              {
                combination: "1",
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 2,
                rank: 1,
              },
            ],
          },
          {
            combination: "2",
            points: [
              {
                combination: "2",
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 3,
                rank: 2,
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
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 1.5,
                rank: 1,
              },
            ],
          },
          {
            combination: "2",
            points: [
              {
                combination: "2",
                fetchedAt: "2026-05-29T07:30:00.000Z",
                odds: 3,
                rank: 2,
              },
            ],
          },
        ],
      },
    },
    raceEntries: {
      fetchedAt: "2026-05-29T07:30:00.000Z",
      horses: [
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseName: "テスト馬A",
          horseNumber: "1",
          jockeyName: "騎手A",
          status: null,
        },
        {
          fetchedAt: "2026-05-29T07:30:00.000Z",
          horseName: "テスト馬B",
          horseNumber: "2",
          jockeyName: "騎手B",
          status: null,
        },
      ],
    },
    raceKey: "nar:2026:0529:47:01",
    raceResults: null,
    source: {
      babaCode: "23",
      debaUrl:
        "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=1&k_babaCode=23",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "47",
      lastOddsFetchAt: null,
      lastWeightFetchAt: null,
      oddsLinks: {
        fukusho: "https://www.keiba.go.jp/KeibaWeb/Odds/OddsTanFuku?k=1",
        tansho: "https://www.keiba.go.jp/KeibaWeb/Odds/OddsTanFuku?k=1",
      },
      raceBango: "01",
      raceKey: "nar:2026:0529:47:01",
      raceName: "テストレース",
      raceStartAtJst: "2026-05-29T16:30:00+09:00",
      source: "nar",
    },
    trackCondition: null,
  });
});

it("buildDevRealtimePayload returns empty payload when keibajoCode is not in NAR babaCode map", async () => {
  const fetchSpy = vi.fn<() => Promise<Response>>();
  vi.stubGlobal("fetch", fetchSpy);
  const payload = await buildDevRealtimePayload({
    day: "29",
    keibajoCode: "99",
    month: "05",
    raceNumber: "01",
    source: "nar",
    year: "2026",
  });
  expect(payload).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "nar:2026:0529:99:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
  expect(fetchSpy).not.toHaveBeenCalled();
});

it("buildDevRealtimePayload returns empty payload when race list lacks the target raceNumber", async () => {
  mockFetchHtml({
    [RACE_LIST_URL]: `
      <a href="DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=11&k_babaCode=23">11</a>
      <a href="DebaTable?k_raceDate=2026%2F05%2F29&k_raceNo=12&k_babaCode=23">12</a>
    `,
  });
  const payload = await buildDevRealtimePayload({
    day: "29",
    keibajoCode: "47",
    month: "05",
    raceNumber: "01",
    source: "nar",
    year: "2026",
  });
  expect(payload).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "nar:2026:0529:47:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("buildDevRealtimePayload returns empty payload when upstream fetch rejects", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(() => Promise.reject(new Error("network down"))),
  );
  const consoleSpy = vi.spyOn(console, "warn").mockImplementation(() => {
    // suppress noisy log in CI
  });
  const payload = await buildDevRealtimePayload({
    day: "29",
    keibajoCode: "47",
    month: "05",
    raceNumber: "01",
    source: "nar",
    year: "2026",
  });
  expect(payload).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "nar:2026:0529:47:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
  expect(consoleSpy).toHaveBeenCalled();
});

it("buildDevRealtimePayload returns empty payload immediately for JRA without calling fetch", async () => {
  const fetchSpy = vi.fn<() => Promise<Response>>();
  vi.stubGlobal("fetch", fetchSpy);
  const payload = await buildDevRealtimePayload({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "07",
    source: "jra",
    year: "2026",
  });
  expect(payload).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "jra:2026:0529:05:07",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
  expect(fetchSpy).not.toHaveBeenCalled();
});

it("buildDevRealtimePayload accumulates history across two successive polls", async () => {
  mockFetchHtml({
    [RACE_LIST_URL]: RACE_LIST_HTML,
    [DEBA_URL]: RACE_DETAIL_HTML,
    [TANSHO_FUKUSHO_URL]: TANSHO_FUKUSHO_BODY,
  });
  const first = await buildDevRealtimePayload({
    day: "29",
    keibajoCode: "47",
    month: "05",
    raceNumber: "01",
    source: "nar",
    year: "2026",
  });
  expect(first.odds?.history.length).toBe(2);
  vi.setSystemTime(new Date("2026-05-29T07:30:30.000Z"));
  const second = await buildDevRealtimePayload({
    day: "29",
    keibajoCode: "47",
    month: "05",
    raceNumber: "01",
    source: "nar",
    year: "2026",
  });
  expect(second.odds?.fetchedAt).toBe("2026-05-29T07:30:30.000Z");
  expect(second.odds?.history.length).toBe(4);
  expect(second.odds?.horseTrends.length).toBe(2);
  expect(second.odds?.horseTrends[0]?.points.length).toBe(2);
  expect(second.odds?.historyByType?.tansho?.length).toBe(4);
  expect(second.odds?.trendsByType?.tansho?.length).toBe(2);
  expect(second.odds?.trendsByType?.tansho?.[0]?.points.length).toBe(2);
});
