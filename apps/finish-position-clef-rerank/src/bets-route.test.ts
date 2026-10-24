// Run with bun (bunx vitest).
import { afterEach, expect, it, vi } from "vitest";

import {
  oddsSignature,
  parseBudget,
  parseTarget,
  suggestRaceBets,
  viewerPath,
  warmRaceBets,
  type BetEnv,
} from "./bets-route";

const race = {
  ymd: "20261018",
  keibajoCode: "08",
  raceBango: "11",
  budget: 500,
  types: null,
  target: null,
  requireAll: false,
};

const heatmapPath = "/api/races/2026/10/18/08/11/sections/win-rate-heatmap?source=jra";
const realtimePath = "/api/races/2026/10/18/08/11/realtime?source=jra";

const heatmap = {
  runners: [
    {
      umaban: "01",
      wakuban: "1",
      bamei: "アルファ",
      kishumeiRyakusho: "騎手A",
      chokyoshimeiRyakusho: "調教A",
      banushimei: "馬主A",
    },
    {
      umaban: "02",
      wakuban: "2",
      bamei: "ベータ",
      kishumeiRyakusho: "騎手B",
      chokyoshimeiRyakusho: "調教B",
      banushimei: "馬主B",
    },
  ],
};

const realtime = {
  odds: {
    fetchedAt: "2026-10-18T09:00:00+09:00",
    latest: {
      tansho: [
        { combination: "1", odds: 2 },
        { combination: "2", odds: 3 },
      ],
      fukusho: [
        { combination: "1", minOdds: 1.1, maxOdds: 1.4 },
        { combination: "2", minOdds: 1.3, maxOdds: 1.8 },
      ],
    },
  },
};

const clefAnswers = {
  answers: {
    winner: { probabilities: { "1": 0.7, "2": 0.3 } },
    bet_type: { probabilities: { tansho: 0.6, fukusho: 0.4 } },
    bet_style: { probabilities: { box: 1 } },
    axis: { probabilities: { "1": 1 } },
    top3_1: { noul: 0.9 },
    top3_2: { noul: 0.8 },
  },
};

const makeViewer = (pages: Record<string, unknown>): BetEnv["VIEWER"] => ({
  fetch: vi.fn(async (request: Request) => {
    const url = new URL(request.url);
    const page = pages[`${url.pathname}${url.search}`];
    return page === undefined ? new Response("missing", { status: 500 }) : Response.json(page);
  }),
});

const makeEnv = (store: Record<string, string>, pages: Record<string, unknown>): BetEnv => ({
  AI: { run: vi.fn(async () => clefAnswers) },
  DETAIL_SECTION_CACHE_KV: {
    get: vi.fn(async (key: string) => store[key] ?? null),
    put: vi.fn(async () => undefined),
  },
  VIEWER: makeViewer(pages),
});

afterEach(() => {
  vi.useRealTimers();
});

it("parses budgets in 100-yen steps and defaults to 2,000 yen", () => {
  expect(parseBudget(null)).toBe(2000);
  expect(parseBudget("500")).toBe(500);
  expect(parseBudget("1000000")).toBe(1000000);
  expect(parseBudget("150")).toBeNull();
  expect(parseBudget("100")).toBeNull();
  expect(parseBudget("200")).toBe(200);
  expect(parseBudget("0")).toBeNull();
  expect(parseBudget("1000100")).toBeNull();
  expect(parseBudget("abc")).toBeNull();
});

it("builds viewer paths with the race source", () => {
  expect(viewerPath(race, "realtime")).toBe("/api/races/2026/10/18/08/11/realtime?source=jra");
  expect(viewerPath({ ...race, keibajoCode: "44" }, "trends")).toBe(
    "/api/races/2026/10/18/44/11/trends?source=nar",
  );
  expect(viewerPath({ ...race, keibajoCode: "00" }, "trends")).toBe(
    "/api/races/2026/10/18/00/11/trends?source=nar",
  );
});

it("asks clef-flash, caches the answers and returns the suggestion", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-18T00:00:00Z"));
  const env = makeEnv(
    {
      "clefrr:v1:result:20261018:08:11": JSON.stringify({
        predictions: [{ horseNumber: 2, rank: 1 }, { horseNumber: 1, rank: 2 }, { bad: true }],
      }),
    },
    { [heatmapPath]: heatmap, [realtimePath]: realtime },
  );
  expect(await suggestRaceBets(env, race)).toStrictEqual({
    status: 200,
    body: {
      raceKey: "20261018:08:11",
      target: null,
      baseGeneratedAt: "",
      oddsFetchedAt: "2026-10-18T09:00:00+09:00",
      generatedAt: "2026-10-18T00:00:00.000Z",
      runners: [
        {
          horseNumber: 1,
          frameNumber: 1,
          horseName: "アルファ",
          jockey: "騎手A",
          trainer: "調教A",
          owner: "馬主A",
          clefWin: 0.7,
          clefTop3: 0.9,
        },
        {
          horseNumber: 2,
          frameNumber: 2,
          horseName: "ベータ",
          jockey: "騎手B",
          trainer: "調教B",
          owner: "馬主B",
          clefWin: 0.3,
          clefTop3: 0.8,
        },
      ],
      suggestion: {
        betType: "tansho",
        style: "single",
        axis: 1,
        legs: [[1]],
        budget: 300,
        totalStake: 300,
        expectedReturn: 350,
        tickets: [
          {
            horses: [1],
            odds: 2,
            maxOdds: null,
            probability: 0.5840829135794368,
            stake: 300,
            payout: 600,
            payoutMax: null,
          },
        ],
      },
      plans: {
        budget: 500,
        totalStake: 500,
        expectedReturn: 350,
        plans: [
          {
            betType: "tansho",
            style: "single",
            axis: 1,
            legs: [[1]],
            budget: 300,
            totalStake: 300,
            expectedReturn: 350,
            tickets: [
              {
                horses: [1],
                odds: 2,
                maxOdds: null,
                probability: 0.5840829135794368,
                stake: 300,
                payout: 600,
                payoutMax: null,
              },
            ],
          },
          {
            betType: "fukusho",
            style: "single",
            axis: 1,
            legs: [[1]],
            budget: 200,
            totalStake: 200,
            expectedReturn: 0,
            tickets: [
              {
                horses: [1],
                odds: 1.1,
                maxOdds: 1.4,
                probability: 0,
                stake: 200,
                payout: 220,
                payoutMax: 280,
              },
            ],
          },
        ],
      },
    },
  });
  expect(env.DETAIL_SECTION_CACHE_KV.put).toHaveBeenCalledWith(
    "clefbet:v2:answers:20261018:08:11::1-2",
    JSON.stringify(clefAnswers),
    { expirationTtl: 259200 },
  );
  expect(vi.mocked(env.DETAIL_SECTION_CACHE_KV.put).mock.calls[1]?.[0]).toBe(
    "clefbet:v3:result:20261018:08:11:500:t-",
  );
});

it("reuses cached answers and falls back to served ranks", async () => {
  const env = makeEnv(
    {
      "clefbet:v2:answers:20261018:08:11:a:1-2": JSON.stringify(clefAnswers),
      "clefrr:v1:result:20261018:08:11": JSON.stringify({ predictions: "none" }),
      "pred:fp:v1:20261018:08:11": JSON.stringify([
        {
          horseNumber: "1",
          modelVersion: "base",
          predictedFinishNorm: 0,
          predictionGeneratedAt: "a",
        },
        {
          horseNumber: "2",
          modelVersion: "base",
          predictedFinishNorm: 1,
          predictionGeneratedAt: "a",
        },
      ]),
    },
    { [heatmapPath]: heatmap, [realtimePath]: realtime },
  );
  const outcome = await suggestRaceBets(env, race);
  expect(outcome.status).toBe(200);
  expect(env.AI.run).toHaveBeenCalledTimes(0);
  expect(env.DETAIL_SECTION_CACHE_KV.put).toHaveBeenCalledTimes(1);
});

it("returns 404 before odds or runners are published", async () => {
  const env = makeEnv({ "clefrr:v1:result:20261018:08:11": "{broken" }, { [heatmapPath]: heatmap });
  expect(await suggestRaceBets(env, race)).toStrictEqual({
    status: 404,
    body: { error: "runners or odds are not available yet" },
  });
  const noRunners = makeEnv({}, { [realtimePath]: realtime });
  expect((await suggestRaceBets(noRunners, race)).status).toBe(404);
  expect(env.AI.run).toHaveBeenCalledTimes(0);
});

it("returns 502 for a malformed clef-flash answer without caching it", async () => {
  const env = makeEnv({}, { [heatmapPath]: heatmap, [realtimePath]: realtime });
  vi.mocked(env.AI.run).mockResolvedValue({ answers: {} });
  expect(await suggestRaceBets(env, race)).toStrictEqual({
    status: 502,
    body: { error: "clef-flash answer was malformed" },
  });
  expect(env.DETAIL_SECTION_CACHE_KV.put).toHaveBeenCalledTimes(0);
});

it("returns 404 when no planned ticket has odds", async () => {
  const env = makeEnv(
    {},
    {
      [heatmapPath]: heatmap,
      [realtimePath]: {
        odds: { fetchedAt: "t", latest: { tansho: [{ combination: "1", odds: 2 }] } },
      },
    },
  );
  vi.mocked(env.AI.run).mockResolvedValue({
    answers: { ...clefAnswers.answers, axis: { probabilities: { "2": 1 } } },
  });
  expect(await suggestRaceBets(env, race)).toStrictEqual({
    status: 404,
    body: { error: "no ticket has published odds" },
  });
});

it("reports a missing clef win probability as null", async () => {
  const env = makeEnv({}, { [heatmapPath]: heatmap, [realtimePath]: realtime });
  vi.mocked(env.AI.run).mockResolvedValue({
    answers: { ...clefAnswers.answers, winner: { probabilities: { "1": 1 } } },
  });
  const outcome = await suggestRaceBets(env, race);
  const body = outcome.body;
  expect(
    typeof body === "object" && body !== null && "runners" in body ? body.runners : null,
  ).toStrictEqual([
    {
      horseNumber: 1,
      frameNumber: 1,
      horseName: "アルファ",
      jockey: "騎手A",
      trainer: "調教A",
      owner: "馬主A",
      clefWin: 1,
      clefTop3: 0.9,
    },
    {
      horseNumber: 2,
      frameNumber: 2,
      horseName: "ベータ",
      jockey: "騎手B",
      trainer: "調教B",
      owner: "馬主B",
      clefWin: null,
      clefTop3: 0.8,
    },
  ]);
});

const cachedBody = {
  raceKey: "20261018:08:11",
  baseGeneratedAt: "",
  oddsFetchedAt: "2026-10-18T09:00:00+09:00",
  generatedAt: "2026-10-18T00:00:00.000Z",
  runners: [],
  suggestion: { betType: "tansho" },
};

it("serves a cached suggestion for the same odds snapshot without clef or heavy sections", async () => {
  const env = makeEnv(
    { "clefbet:v3:result:20261018:08:11:500:t-": JSON.stringify(cachedBody) },
    { [heatmapPath]: heatmap, [realtimePath]: realtime },
  );
  expect(await suggestRaceBets(env, race)).toStrictEqual({ status: 200, body: cachedBody });
  expect(env.AI.run).toHaveBeenCalledTimes(0);
  expect(env.VIEWER.fetch).toHaveBeenCalledTimes(1);
});

it("regenerates when the cached suggestion is for older odds or another prediction", async () => {
  const staleOdds = makeEnv(
    {
      "clefbet:v3:result:20261018:08:11:500:t-": JSON.stringify({
        ...cachedBody,
        oddsFetchedAt: "old",
      }),
    },
    { [heatmapPath]: heatmap, [realtimePath]: realtime },
  );
  expect((await suggestRaceBets(staleOdds, race)).status).toBe(200);
  expect(staleOdds.AI.run).toHaveBeenCalledTimes(1);
  const staleBase = makeEnv(
    {
      "clefbet:v3:result:20261018:08:11:500:t-": JSON.stringify({
        ...cachedBody,
        baseGeneratedAt: "old",
      }),
    },
    { [heatmapPath]: heatmap, [realtimePath]: realtime },
  );
  expect((await suggestRaceBets(staleBase, race)).status).toBe(200);
  const notResult = makeEnv(
    { "clefbet:v3:result:20261018:08:11:500:t-": JSON.stringify({ oddsFetchedAt: "x" }) },
    { [heatmapPath]: heatmap, [realtimePath]: realtime },
  );
  expect((await suggestRaceBets(notResult, race)).status).toBe(200);
});

it("signs odds by the order of the six shortest win odds", () => {
  expect(
    oddsSignature({
      odds: {
        latest: {
          tansho: [
            { combination: "3", odds: 9 },
            { combination: "1" },
            { combination: "2", odds: 2 },
            { combination: "4", odds: 5 },
            { combination: "5", odds: 7 },
            { combination: "6", odds: 11 },
            { combination: "7", odds: 30 },
          ],
        },
      },
    }),
  ).toBe("2-4-5-3-6-7");
  expect(oddsSignature(null)).toBe("");
});

const basePrediction = JSON.stringify([
  { horseNumber: "1", modelVersion: "base", predictedFinishNorm: 0, predictionGeneratedAt: "g1" },
  { horseNumber: "2", modelVersion: "base", predictedFinishNorm: 1, predictionGeneratedAt: "g1" },
]);
const warmRace = { ymd: "20261018", keibajoCode: "08", raceBango: "11" };

it("warms the default budget once per served prediction generation", async () => {
  const env = makeEnv(
    { "pred:fp:v1:20261018:08:11": basePrediction },
    {
      "/api/races/2026/10/18/08/11/sections/win-rate-heatmap?source=jra": heatmap,
      "/api/races/2026/10/18/08/11/realtime?source=jra": realtime,
    },
  );
  expect(await warmRaceBets(env, warmRace)).toBe(404);
  expect(env.DETAIL_SECTION_CACHE_KV.put).toHaveBeenLastCalledWith(
    "clefbet:v3:warmed:20261018:08:11",
    "g1",
    { expirationTtl: 259200 },
  );
  expect(vi.mocked(env.DETAIL_SECTION_CACHE_KV.put).mock.calls[1]?.[0]).toBe(
    "clefbet:v3:result:20261018:08:11:2000:t10000",
  );
});

it("skips the warm without a prediction or when it was already warmed", async () => {
  const noBase = makeEnv({}, {});
  expect(await warmRaceBets(noBase, warmRace)).toBe(304);
  const warmed = makeEnv(
    { "pred:fp:v1:20261018:08:11": basePrediction, "clefbet:v3:warmed:20261018:08:11": "g1" },
    {},
  );
  expect(await warmRaceBets(warmed, warmRace)).toBe(304);
  expect(warmed.VIEWER.fetch).toHaveBeenCalledTimes(0);
});

it("does not mark a race warmed when the suggestion is unavailable", async () => {
  const env = makeEnv({ "pred:fp:v1:20261018:08:11": basePrediction }, {});
  expect(await warmRaceBets(env, warmRace)).toBe(404);
  expect(env.DETAIL_SECTION_CACHE_KV.put).toHaveBeenCalledTimes(0);
});

it("caches a bet-type selection under its own result key", async () => {
  const env = makeEnv({}, { [heatmapPath]: heatmap, [realtimePath]: realtime });
  vi.mocked(env.AI.run).mockResolvedValue(clefAnswers);
  const outcome = await suggestRaceBets(env, { ...race, types: new Set(["tansho"]) });
  expect(outcome.status).toBe(200);
  expect(vi.mocked(env.DETAIL_SECTION_CACHE_KV.put).mock.calls[1]?.[0]).toBe(
    "clefbet:v3:result:20261018:08:11:500:t-:tansho",
  );
});

it("parses the target profit in whole yen and defaults to 10,000 yen", () => {
  expect(parseTarget(null)).toBe(10000);
  expect(parseTarget("0")).toBe(0);
  expect(parseTarget("12345")).toBe(12345);
  expect(parseTarget("10000000")).toBe(10000000);
  expect(parseTarget("10000001")).toBeNull();
  expect(parseTarget("-1")).toBeNull();
  expect(parseTarget("1.5")).toBeNull();
  expect(parseTarget("abc")).toBeNull();
});

it("reports and caches a target profit that no ticket can reach", async () => {
  const env = makeEnv({}, { [heatmapPath]: heatmap, [realtimePath]: realtime });
  expect(await suggestRaceBets(env, { ...race, target: 100000 })).toStrictEqual({
    status: 404,
    body: {
      error: "no ticket reaches the target profit within the budget",
      reason: "target-unreachable",
    },
  });
  expect(env.DETAIL_SECTION_CACHE_KV.put).toHaveBeenLastCalledWith(
    "clefbet:v3:result:20261018:08:11:500:t100000",
    JSON.stringify({
      error: "no ticket reaches the target profit within the budget",
      reason: "target-unreachable",
      oddsFetchedAt: "2026-10-18T09:00:00+09:00",
      baseGeneratedAt: "",
    }),
    { expirationTtl: 259200 },
  );
});

it("serves a cached unreachable target for the same odds snapshot", async () => {
  const env = makeEnv(
    {
      "clefbet:v3:result:20261018:08:11:500:t100000": JSON.stringify({
        error: "no ticket reaches the target profit within the budget",
        reason: "target-unreachable",
        oddsFetchedAt: "2026-10-18T09:00:00+09:00",
        baseGeneratedAt: "",
      }),
    },
    { [realtimePath]: realtime },
  );
  expect(await suggestRaceBets(env, { ...race, target: 100000 })).toStrictEqual({
    status: 404,
    body: {
      error: "no ticket reaches the target profit within the budget",
      reason: "target-unreachable",
    },
  });
  expect(env.AI.run).toHaveBeenCalledTimes(0);
});

it("caches a required-all suggestion under its own key", async () => {
  const env = makeEnv({}, { [heatmapPath]: heatmap, [realtimePath]: realtime });
  const outcome = await suggestRaceBets(env, { ...race, requireAll: true });
  expect(outcome.status).toBe(200);
  expect(vi.mocked(env.DETAIL_SECTION_CACHE_KV.put).mock.calls.at(-1)?.[0]).toBe(
    "clefbet:v3:result:20261018:08:11:500:t-:all",
  );
});

it("reports and caches required bet types that cannot all reach the target", async () => {
  const env = makeEnv({}, { [heatmapPath]: heatmap, [realtimePath]: realtime });
  expect(await suggestRaceBets(env, { ...race, target: 400, requireAll: true })).toStrictEqual({
    status: 404,
    body: {
      error: "the selected bet types cannot all fit the budget and target",
      reason: "required-types-unfit",
    },
  });
  expect(env.DETAIL_SECTION_CACHE_KV.put).toHaveBeenLastCalledWith(
    "clefbet:v3:result:20261018:08:11:500:t400:all",
    JSON.stringify({
      error: "the selected bet types cannot all fit the budget and target",
      reason: "required-types-unfit",
      oddsFetchedAt: "2026-10-18T09:00:00+09:00",
      baseGeneratedAt: "",
    }),
    { expirationTtl: 259200 },
  );
});

it("serves cached required bet types that cannot all fit", async () => {
  const env = makeEnv(
    {
      "clefbet:v3:result:20261018:08:11:500:t400:all": JSON.stringify({
        reason: "required-types-unfit",
        oddsFetchedAt: "2026-10-18T09:00:00+09:00",
        baseGeneratedAt: "",
      }),
    },
    { [realtimePath]: realtime },
  );
  expect(await suggestRaceBets(env, { ...race, target: 400, requireAll: true })).toStrictEqual({
    status: 404,
    body: {
      error: "the selected bet types cannot all fit the budget and target",
      reason: "required-types-unfit",
    },
  });
});

it("reports required bet types without a fitting plan even with no target", async () => {
  const env = makeEnv(
    {},
    {
      [heatmapPath]: heatmap,
      [realtimePath]: {
        odds: {
          fetchedAt: "t",
          latest: {
            ...realtime.odds.latest,
            "3rentan": [{ combination: "1-2-3", odds: 30 }],
          },
        },
      },
    },
  );
  expect(
    await suggestRaceBets(env, {
      ...race,
      types: new Set(["tansho", "fukusho", "3rentan"]),
      requireAll: true,
    }),
  ).toStrictEqual({
    status: 404,
    body: {
      error: "the selected bet types cannot all fit the budget and target",
      reason: "required-types-unfit",
    },
  });
});
