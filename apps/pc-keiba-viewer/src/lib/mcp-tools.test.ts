// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { indexLiveHorseWeightKg } from "./horse-weight-class";
import { callMcpTool, MCP_TOOL_DEFINITIONS } from "./mcp-tools";
import { buildWinRateHeatmapDisplay } from "./win-rate-heatmap";
import type { WinRateHeatmapSectionPayload } from "./win-rate-heatmap-cache";

const heatmapPayload: WinRateHeatmapSectionPayload = {
  bloodlineRows: [],
  carriedWeightClassStats: [],
  frameStats: [],
  horseResults: [],
  runners: [
    {
      banushimei: "Owner A",
      barei: "4",
      bamei: "Alpha",
      bataiju: null,
      chokyoshimeiRyakusho: "Trainer A",
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      damSireName: null,
      futanJuryo: "570",
      kakuteiChakujun: "00",
      kettoTorokuBango: "2020100001",
      kishumeiRyakusho: "Jockey A",
      kohan3f: null,
      seibetsuCode: "1",
      sireName: null,
      sireSireName: null,
      sohaTime: null,
      tanshoNinkijun: "00",
      tanshoOdds: "0000",
      timeSa: null,
      umaban: "01",
      wakuban: "1",
      zogenFugo: null,
      zogenSa: null,
    },
  ],
  similarRows: [
    {
      category: "jockey",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 40,
      name: "Jockey A",
      quinellaCount: 20,
      quinellaRate: 25,
      showCount: 30,
      showRate: 37.5,
      starts: 80,
      winCount: 16,
      winRate: 20,
    },
  ],
  type: "win-rate-heatmap",
  weightClassStats: [],
};

const jsonFetch =
  (routes: Record<string, unknown>) =>
  async (pathWithQuery: string): Promise<Response> => {
    const value = routes[pathWithQuery];
    if (value === undefined) {
      return new Response("missing", { status: 404 });
    }
    return new Response(JSON.stringify(value), {
      headers: { "content-type": "application/json" },
      status: 200,
    });
  };

const failingFetch = async (): Promise<Response> => new Response("nope", { status: 500 });

it("authenticate reports MCP bearer success without touching human Access", async () => {
  const result = await callMcpTool(
    "authenticate",
    {},
    jsonFetch({ "/api/spec": { openapi: "3.1.0" } }),
  );
  expect(result.isError).toBe(false);
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    accessCoexists: true,
    mcpAuthenticated: true,
    specOk: true,
    specStatus: 200,
  });
});

it("get_json rejects a path that is not allowlisted", async () => {
  const result = await callMcpTool(
    "get_json",
    { path: "/api/internal/race-cache-bust" },
    jsonFetch({}),
  );
  expect(result.isError).toBe(true);
});

it("exposes get_finish_prediction_summary with source required in the MCP schema", () => {
  expect(
    MCP_TOOL_DEFINITIONS.find((definition) => definition.name === "get_finish_prediction_summary"),
  ).toStrictEqual({
    description:
      "Fetch a compact finish-position prediction summary for LLM use. Omits inputs.results and other UI-only history, joins current runner names by normalized horse number, and ranks lower predictedFinishNorm first.",
    inputSchema: {
      additionalProperties: false,
      properties: {
        day: { description: "Calendar day, two digits.", pattern: "^\\d{2}$", type: "string" },
        keibajoCode: { description: "Venue code.", pattern: "^[0-9A-Z]{2}$", type: "string" },
        month: {
          description: "Calendar month, two digits.",
          pattern: "^\\d{2}$",
          type: "string",
        },
        raceNumber: {
          description: "Race number, two digits.",
          pattern: "^\\d{2}$",
          type: "string",
        },
        source: {
          description: "jra or nar race source.",
          enum: ["jra", "nar"],
          type: "string",
        },
        year: {
          description: "Calendar year, four digits.",
          pattern: "^\\d{4}$",
          type: "string",
        },
      },
      required: ["year", "month", "day", "keibajoCode", "raceNumber", "source"],
      type: "object",
    },
    name: "get_finish_prediction_summary",
  });
});

it("get_finish_prediction_summary returns only compact current predictions", async () => {
  const hugeResults = Array.from({ length: 2_000 }, (_, index) => ({
    history: `past-race-${index}-${"x".repeat(80)}`,
  }));
  const payload = {
    bucket: {
      bucketEvaluation: {
        ndcgAt3Avg: 0.8,
        pairScoreAvg: 0.7,
        predictionCount: 500,
        raceCount: 50,
        smallSampleWarning: false,
        top1Accuracy: 0.4,
        top3BoxAccuracy: 0.3,
        top3ExactAccuracy: 0.1,
        top3WinnerCaptureRate: 0.8,
        top5WinnerCaptureRate: 0.95,
      },
      bucketRace: { kyosomeiHondai: "園田5R" },
    },
    inputs: {
      currentDistance: "1400",
      currentKeibajoCode: "50",
      currentRaceDate: "20260827",
      currentSource: "nar",
      currentTrackCode: "24",
      modelPredictionFeatures: [
        {
          confidenceTier: "high",
          horseNumber: "1",
          modelVersion: "nar-model",
          predictedFinishNorm: 0.1,
          predictedScoreStddev: 0.3,
          predictionGeneratedAt: "2026-08-27T01:02:03.000Z",
          showProbability: 0.8,
          winProbability: 0.5,
        },
      ],
      results: hugeResults,
      runners: [
        {
          bamei: "Alpha",
          jockeyNameFull: "Jockey Alpha",
          kishumeiRyakusho: "J. A",
          umaban: "01",
        },
      ],
      sameDayVenueJockeyWins: [{ jockeyName: "Jockey Alpha", winCount: 1 }],
    },
    type: "finish-prediction",
  };
  const result = await callMcpTool(
    "get_finish_prediction_summary",
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
    jsonFetch({
      "/api/races/2026/08/27/50/05/sections/finish-prediction?source=nar": payload,
    }),
  );
  const summaryText = result.content[0]?.text ?? "";

  expect(result.isError).toBe(false);
  expect(JSON.parse(summaryText)).toStrictEqual({
    evaluation: {
      ndcgAt3Avg: 0.8,
      pairScoreAvg: 0.7,
      predictionCount: 500,
      raceCount: 50,
      smallSampleWarning: false,
      top1Accuracy: 0.4,
      top3BoxAccuracy: 0.3,
      top3ExactAccuracy: 0.1,
      top3WinnerCaptureRate: 0.8,
      top5WinnerCaptureRate: 0.95,
    },
    prediction: [
      {
        confidenceTier: "high",
        horseName: "Alpha",
        horseNumber: "01",
        jockeyName: "Jockey Alpha",
        modelVersion: "nar-model",
        predictedFinishNorm: 0.1,
        predictedScoreStddev: 0.3,
        predictionGeneratedAt: "2026-08-27T01:02:03.000Z",
        rank: 1,
        showProbability: 0.8,
        winProbability: 0.5,
      },
    ],
    race: {
      distance: "1400",
      keibajoCode: "50",
      raceDate: "2026-08-27",
      raceName: "園田5R",
      raceNumber: "05",
      source: "nar",
      trackCode: "24",
    },
    sameDayVenueJockeyWins: [{ jockeyName: "Jockey Alpha", winCount: 1 }],
  });
  expect(new TextEncoder().encode(JSON.stringify(payload)).byteLength).toBeGreaterThan(200_000);
  expect(new TextEncoder().encode(summaryText).byteLength).toBeLessThan(2_000);
  expect(summaryText).not.toMatch(/results/u);
  expect(summaryText).not.toMatch(/past-race/u);
});

it("get_finish_prediction_summary applies a bounded AbortSignal to the internal API read", async () => {
  const signals: AbortSignal[] = [];
  const result = await callMcpTool(
    "get_finish_prediction_summary",
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
    async (_pathWithQuery, signal) => {
      if (signal !== undefined) {
        signals.push(signal);
      }
      return new Response(
        JSON.stringify({
          inputs: {
            currentKeibajoCode: "50",
            currentRaceDate: "20260827",
            currentSource: "nar",
            modelPredictionFeatures: [{ horseNumber: "1", predictedFinishNorm: 0.1 }],
            runners: [{ bamei: "Alpha", umaban: "01" }],
          },
          type: "finish-prediction",
        }),
        { status: 200 },
      );
    },
  );

  expect(result.isError).toBe(false);
  expect(signals).toHaveLength(1);
  expect(signals[0]?.aborted).toBe(false);
});

it("get_finish_prediction_summary reports validation errors with stable codes", async () => {
  const invalidSource = await callMcpTool(
    "get_finish_prediction_summary",
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "local",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(invalidSource.content[0]?.text ?? "{}")).toStrictEqual({
    error: { code: "INVALID_SOURCE", message: "source must be either jra or nar." },
  });

  const invalidVenue = await callMcpTool(
    "get_finish_prediction_summary",
    {
      day: "27",
      keibajoCode: "05",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(invalidVenue.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "INVALID_VENUE_CODE",
      message: "keibajoCode 05 is not valid for source nar.",
    },
  });

  const unknownVenue = await callMcpTool(
    "get_finish_prediction_summary",
    {
      day: "27",
      keibajoCode: "99",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(unknownVenue.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "INVALID_VENUE_CODE",
      message: "keibajoCode 99 is not valid for source nar.",
    },
  });

  const invalidRace = await callMcpTool(
    "get_finish_prediction_summary",
    {
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "00",
      source: "nar",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(invalidRace.content[0]?.text ?? "{}")).toStrictEqual({
    error: { code: "INVALID_RACE_NUMBER", message: "raceNumber must be between 01 and 18." },
  });
});

it("get_finish_prediction_summary distinguishes upstream status failures", async () => {
  const args = {
    day: "27",
    keibajoCode: "50",
    month: "08",
    raceNumber: "05",
    source: "nar",
    year: "2026",
  };
  const notFound = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    async () => new Response("missing", { status: 404 }),
  );
  expect(JSON.parse(notFound.content[0]?.text ?? "{}")).toStrictEqual({
    error: { code: "RACE_NOT_FOUND", message: "The requested race was not found." },
  });
  const timeout = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    async () => new Response("timeout", { status: 504 }),
  );
  expect(JSON.parse(timeout.content[0]?.text ?? "{}")).toStrictEqual({
    error: { code: "TIMEOUT", message: "The finish prediction API timed out." },
  });
  const oversized = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    async () => new Response("large", { status: 413 }),
  );
  expect(JSON.parse(oversized.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "RESPONSE_TOO_LARGE",
      message: "The finish prediction API response is too large to process safely.",
    },
  });
  const failed = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    async () => new Response("failed", { status: 500 }),
  );
  expect(JSON.parse(failed.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "UPSTREAM_API_ERROR",
      message: "The finish prediction API failed with status 500.",
    },
  });
});

it("get_finish_prediction_summary distinguishes invalid JSON, timeouts, and request failures", async () => {
  const args = {
    day: "27",
    keibajoCode: "50",
    month: "08",
    raceNumber: "05",
    source: "nar",
    year: "2026",
  };
  const malformed = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    async () => new Response("not-json", { status: 200 }),
  );
  expect(JSON.parse(malformed.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "PREDICTION_PAYLOAD_MALFORMED",
      message: "The finish prediction API returned invalid JSON.",
    },
  });
  const timeout = await callMcpTool("get_finish_prediction_summary", args, async () => {
    throw new DOMException("Timed out", "TimeoutError");
  });
  expect(JSON.parse(timeout.content[0]?.text ?? "{}")).toStrictEqual({
    error: { code: "TIMEOUT", message: "The finish prediction API timed out." },
  });
  const failed = await callMcpTool("get_finish_prediction_summary", args, async () => {
    throw new Error("network down");
  });
  expect(JSON.parse(failed.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "UPSTREAM_API_ERROR",
      message: "The finish prediction API request failed.",
    },
  });
});

it("get_finish_prediction_summary distinguishes unavailable and malformed prediction payloads", async () => {
  const args = {
    day: "27",
    keibajoCode: "50",
    month: "08",
    raceNumber: "05",
    source: "nar",
    year: "2026",
  };
  const unavailable = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    jsonFetch({
      "/api/races/2026/08/27/50/05/sections/finish-prediction?source=nar": {
        inputs: {
          currentKeibajoCode: "50",
          currentRaceDate: "20260827",
          currentSource: "nar",
          modelPredictionFeatures: [],
          runners: [],
        },
        type: "finish-prediction",
      },
    }),
  );
  expect(JSON.parse(unavailable.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "PREDICTION_NOT_AVAILABLE",
      message: "Finish prediction has not been generated for this race.",
    },
  });

  const malformed = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    jsonFetch({
      "/api/races/2026/08/27/50/05/sections/finish-prediction?source=nar": {
        inputs: { modelPredictionFeatures: [], runners: "bad" },
        type: "finish-prediction",
      },
    }),
  );
  expect(JSON.parse(malformed.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "PREDICTION_PAYLOAD_MALFORMED",
      message: "The finish prediction API returned a payload that cannot be summarized safely.",
    },
  });
});

it("get_finish_prediction_summary rejects oversized upstream and compact responses", async () => {
  const args = {
    day: "27",
    keibajoCode: "50",
    month: "08",
    raceNumber: "05",
    source: "nar",
    year: "2026",
  };
  const upstream = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    async () =>
      new Response("large", {
        headers: { "content-length": "16777217" },
        status: 200,
      }),
  );
  expect(JSON.parse(upstream.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "RESPONSE_TOO_LARGE",
      message: "The finish prediction API response is too large to process safely.",
    },
  });

  const longName = "Horse".repeat(14_000);
  const summary = await callMcpTool(
    "get_finish_prediction_summary",
    args,
    jsonFetch({
      "/api/races/2026/08/27/50/05/sections/finish-prediction?source=nar": {
        inputs: {
          currentKeibajoCode: "50",
          currentRaceDate: "20260827",
          currentSource: "nar",
          modelPredictionFeatures: [{ horseNumber: "1", predictedFinishNorm: 0.1 }],
          runners: [{ bamei: longName, umaban: "01" }],
        },
        type: "finish-prediction",
      },
    }),
  );
  expect(JSON.parse(summary.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "RESPONSE_TOO_LARGE",
      message: "The compact finish prediction summary exceeds the MCP response size limit.",
    },
  });
});

it("search_entities validates kind and query", async () => {
  const kind = await callMcpTool("search_entities", { kind: "barn", q: "a" }, jsonFetch({}));
  expect(kind.content[0]?.text).toBe("kind must be horse, jockey, owner, or trainer");
  const query = await callMcpTool("search_entities", { kind: "horse", q: "  " }, jsonFetch({}));
  expect(query.content[0]?.text).toBe("q must be a non-empty search string");
});

it("get_win_rate_heatmap_display uses the same display builder as the table", async () => {
  const result = await callMcpTool(
    "get_win_rate_heatmap_display",
    {
      day: "20",
      keibajoCode: "05",
      month: "08",
      raceNumber: "01",
      year: "2026",
    },
    jsonFetch({
      "/api/races/2026/08/20/05/01/realtime": {
        horseWeights: {
          fetchedAt: "2026-08-20T00:00:00.000Z",
          horses: [{ horseNumber: "1", weight: 480 }],
        },
      },
      "/api/races/2026/08/20/05/01/sections/win-rate-heatmap": heatmapPayload,
    }),
  );
  expect(result.isError).toBe(false);
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual(
    JSON.parse(
      JSON.stringify(
        buildWinRateHeatmapDisplay({
          bloodlineRows: heatmapPayload.bloodlineRows,
          carriedWeightClassStats: heatmapPayload.carriedWeightClassStats,
          frameStats: heatmapPayload.frameStats,
          horseResults: heatmapPayload.horseResults,
          keibajoCode: "05",
          liveWeightKgByHorse: indexLiveHorseWeightKg([{ horseNumber: "1", weight: 480 }]),
          runners: heatmapPayload.runners,
          showStarts: false,
          similarRows: heatmapPayload.similarRows,
          viewMode: "winRate",
          weightClassStats: heatmapPayload.weightClassStats,
        }),
      ),
    ),
  );
});

it("get_win_rate_heatmap_display rejects an invalid viewMode", async () => {
  const result = await callMcpTool(
    "get_win_rate_heatmap_display",
    {
      day: "20",
      keibajoCode: "05",
      month: "08",
      raceNumber: "01",
      viewMode: "rainbow",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(result.content[0]?.text).toBe("viewMode must be winRate, quinellaRate, showRate, or all");
});

it("get_api_spec, list_top_races, get_json, and get_race_section read allowlisted Worker APIs", async () => {
  const spec = await callMcpTool(
    "get_api_spec",
    null,
    jsonFetch({ "/api/spec": { openapi: "3.1.0" } }),
  );
  expect(spec.isError).toBe(false);
  const top = await callMcpTool("list_top_races", undefined, jsonFetch({ "/api/top-races": [] }));
  expect(top.isError).toBe(false);
  const json = await callMcpTool(
    "get_json",
    { path: "/api/spec" },
    jsonFetch({ "/api/spec": { ok: true } }),
  );
  expect(json.isError).toBe(false);
  const section = await callMcpTool(
    "get_race_section",
    {
      day: "20",
      keibajoCode: "05",
      month: "08",
      raceNumber: "01",
      section: "results",
      source: "jra",
      year: "2026",
    },
    jsonFetch({ "/api/races/2026/08/20/05/01/sections/results?source=jra": { type: "results" } }),
  );
  expect(section.isError).toBe(false);
});

it("get_race_section validates the race route and section name", async () => {
  const year = await callMcpTool("get_race_section", { year: "20" }, jsonFetch({}));
  expect(year.content[0]?.text).toBe("year must be a 4-digit calendar year");
  const section = await callMcpTool(
    "get_race_section",
    {
      day: "20",
      keibajoCode: "05",
      month: "08",
      raceNumber: "01",
      section: "nope",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(section.content[0]?.text).toBe("section is not a supported race detail section");
});

it("search_entities fetches the favorites search API", async () => {
  const result = await callMcpTool(
    "search_entities",
    { kind: "horse", q: "Alpha" },
    jsonFetch({ "/api/mypage/favorites/search?kind=horse&q=Alpha": [{ name: "Alpha" }] }),
  );
  expect(result.isError).toBe(false);
});

it("reports Worker API failures for spec, top-races, section, and heatmap", async () => {
  const spec = await callMcpTool("get_api_spec", {}, failingFetch);
  expect(spec.content[0]?.text).toBe("get_api_spec failed with status 500");
  const top = await callMcpTool("list_top_races", {}, failingFetch);
  expect(top.content[0]?.text).toBe("list_top_races failed with status 500");
  const heatmap = await callMcpTool(
    "get_win_rate_heatmap_display",
    {
      day: "20",
      keibajoCode: "05",
      month: "08",
      raceNumber: "01",
      year: "2026",
    },
    failingFetch,
  );
  expect(heatmap.content[0]?.text).toBe("win-rate-heatmap section payload is unavailable");
});

it("parseRaceRoute rejects month, day, venue, race number, and source", async () => {
  const month = await callMcpTool("get_race_section", { year: "2026", month: "8" }, jsonFetch({}));
  expect(month.content[0]?.text).toBe("month must be a 2-digit calendar month");
  const day = await callMcpTool(
    "get_race_section",
    { year: "2026", month: "08", day: "2" },
    jsonFetch({}),
  );
  expect(day.content[0]?.text).toBe("day must be a 2-digit calendar day");
  const venue = await callMcpTool(
    "get_race_section",
    { year: "2026", month: "08", day: "20", keibajoCode: "x" },
    jsonFetch({}),
  );
  expect(venue.content[0]?.text).toBe("keibajoCode must be a 2-character venue code");
  const race = await callMcpTool(
    "get_race_section",
    { year: "2026", month: "08", day: "20", keibajoCode: "05", raceNumber: "1" },
    jsonFetch({}),
  );
  expect(race.content[0]?.text).toBe("raceNumber must be a 2-digit race number");
  const source = await callMcpTool(
    "get_race_section",
    {
      day: "20",
      keibajoCode: "05",
      month: "08",
      raceNumber: "01",
      section: "results",
      source: "local",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(source.content[0]?.text).toBe("source must be jra or nar when provided");
});

it("callMcpTool rejects unknown tools and non-object arguments", async () => {
  const unknown = await callMcpTool("drop_database", {}, jsonFetch({}));
  expect(unknown.content[0]?.text).toBe("Unknown tool: drop_database");
  const args = await callMcpTool("get_api_spec", ["bad"], jsonFetch({}));
  expect(args.content[0]?.text).toBe("Tool arguments must be an object");
});

it("search requires a non-empty query", async () => {
  const empty = await callMcpTool("search", { query: "  " }, jsonFetch({}));
  expect(empty.content[0]?.text).toBe("query must be a non-empty search string");
});

it("search returns ChatGPT id title url results from all entity kinds", async () => {
  const result = await callMcpTool(
    "search",
    { query: "Alpha" },
    jsonFetch({
      "/api/mypage/favorites/search?kind=horse&q=Alpha": {
        results: [{ id: "2020100001", kind: "horse", label: "Alpha", meta: "12走" }],
      },
      "/api/mypage/favorites/search?kind=jockey&q=Alpha": { results: [] },
      "/api/mypage/favorites/search?kind=owner&q=Alpha": { results: [] },
      "/api/mypage/favorites/search?kind=trainer&q=Alpha": {
        results: [{ id: "Trainer A", kind: "trainer", label: "Trainer A" }],
      },
    }),
  );
  expect(result.isError).toBe(false);
  expect(JSON.parse(result.content[0]?.text ?? "")).toStrictEqual({
    results: [
      {
        id: "horse:2020100001",
        title: "Alpha (horse)",
        url: "/horses/2020100001",
      },
      {
        id: "trainer:Trainer A",
        title: "Trainer A (trainer)",
        url: "/trainers/Trainer A",
      },
    ],
  });
});

it("search skips failed kinds and malformed rows", async () => {
  const result = await callMcpTool(
    "search",
    { query: "X" },
    jsonFetch({
      "/api/mypage/favorites/search?kind=jockey&q=X": { results: "bad" },
      "/api/mypage/favorites/search?kind=owner&q=X": { results: [null] },
      "/api/mypage/favorites/search?kind=trainer&q=X": {
        results: [{ id: "T", kind: "barn", label: "Barn" }],
      },
    }),
  );
  expect(JSON.parse(result.content[0]?.text ?? "")).toStrictEqual({
    results: [{ id: "barn:T", title: "Barn (barn)", url: "/barn/T" }],
  });
});

it("fetch loads an allowlisted api path for ChatGPT", async () => {
  const result = await callMcpTool(
    "fetch",
    { id: "/api/spec" },
    jsonFetch({ "/api/spec": { openapi: "3.1.0" } }),
  );
  expect(JSON.parse(result.content[0]?.text ?? "")).toStrictEqual({
    id: "/api/spec",
    metadata: { kind: "api" },
    text: '{"openapi":"3.1.0"}',
    title: "/api/spec",
    url: "/api/spec",
  });
});

it("fetch loads a search result id", async () => {
  const result = await callMcpTool(
    "fetch",
    { id: "horse:2020100001" },
    jsonFetch({
      "/api/mypage/favorites/search?kind=horse&q=2020100001": {
        results: [{ id: "2020100001", kind: "horse", label: "Alpha", meta: "12走" }],
      },
    }),
  );
  expect(JSON.parse(result.content[0]?.text ?? "")).toStrictEqual({
    id: "horse:2020100001",
    metadata: { kind: "horse", meta: "12走" },
    text: '{"id":"2020100001","kind":"horse","label":"Alpha","meta":"12走"}',
    title: "Alpha (horse)",
    url: "/horses/2020100001",
  });
});

it("exposes get_race_entity_recent_results with an entity enum and cursor pagination", () => {
  const definition = MCP_TOOL_DEFINITIONS.find(
    (entry) => entry.name === "get_race_entity_recent_results",
  );
  expect(definition?.inputSchema.properties.entityType).toStrictEqual({
    description: "Entity resolved from the selected target-race runner.",
    enum: ["horse", "jockey", "trainer", "owner"],
    type: "string",
  });
  expect(definition?.inputSchema.properties.cursor).toStrictEqual({
    description: "Opaque nextCursor from the preceding page, or null for the first page.",
    type: ["string", "null"],
  });
  expect(definition?.inputSchema.properties.limit).toStrictEqual({
    description:
      "Page size. The schema maximum is 30; horse is restricted to 20 by the backend, while jockey, trainer, and owner allow 30.",
    maximum: 30,
    minimum: 1,
    type: "integer",
  });
  expect(definition?.inputSchema.required).toStrictEqual([
    "year",
    "month",
    "day",
    "keibajoCode",
    "raceNumber",
    "source",
    "horseNumber",
    "entityType",
  ]);
});

it("get_race_entity_recent_results forwards a bounded R2 Catalog page", async () => {
  const page = {
    entity: { entityId: "2022103916", entityType: "horse" },
    pagination: {
      effectiveLimit: 5,
      hasMore: true,
      nextCursor: "opaque",
      requestedLimit: 5,
      returned: 5,
    },
    results: [{ raceId: "nar:20260820:50:09" }],
  };
  const result = await callMcpTool(
    "get_race_entity_recent_results",
    {
      cursor: null,
      day: "27",
      entityType: "horse",
      horseNumber: "7",
      keibajoCode: "50",
      limit: 5,
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
    jsonFetch({
      "/api/races/2026/08/27/50/05/entity-recent-results?entityType=horse&horseNumber=7&source=nar&limit=5":
        page,
    }),
  );
  expect(result.isError).toBe(false);
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual(page);
});

it("get_race_entity_recent_results preserves stable errors and validates arguments", async () => {
  const invalidEntity = await callMcpTool(
    "get_race_entity_recent_results",
    {
      day: "27",
      entityType: "breeder",
      horseNumber: "7",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(invalidEntity.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      code: "INVALID_ENTITY_TYPE",
      message: "entityType must be horse, jockey, trainer, or owner.",
    },
  });
  const invalidLimit = await callMcpTool(
    "get_race_entity_recent_results",
    {
      day: "27",
      entityType: "horse",
      horseNumber: "7",
      keibajoCode: "50",
      limit: 0,
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(invalidLimit.content[0]?.text ?? "{}")).toStrictEqual({
    error: { code: "INVALID_LIMIT", message: "limit must be a positive integer." },
  });
  const invalidCursor = await callMcpTool(
    "get_race_entity_recent_results",
    {
      cursor: 2,
      day: "27",
      entityType: "horse",
      horseNumber: "7",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      source: "nar",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(invalidCursor.content[0]?.text ?? "{}")).toStrictEqual({
    error: { code: "INVALID_CURSOR", message: "cursor must be a string or null." },
  });
});

it("fetch rejects blank, malformed, missing, and failed ids", async () => {
  const blank = await callMcpTool("fetch", { id: "  " }, jsonFetch({}));
  expect(blank.content[0]?.text).toBe("id must be a non-empty string");
  const malformed = await callMcpTool("fetch", { id: "nocolon" }, jsonFetch({}));
  expect(malformed.content[0]?.text).toBe("id must be kind:id from search, or an /api path");
  const badKind = await callMcpTool("fetch", { id: "barn:1" }, jsonFetch({}));
  expect(badKind.content[0]?.text).toBe("id must be kind:id from search, or an /api path");
  const missing = await callMcpTool(
    "fetch",
    { id: "horse:missing" },
    jsonFetch({
      "/api/mypage/favorites/search?kind=horse&q=missing": { results: [] },
    }),
  );
  expect(missing.content[0]?.text).toBe("fetch id was not found");
  const failedApi = await callMcpTool("fetch", { id: "/api/spec" }, failingFetch);
  expect(failedApi.content[0]?.text).toBe("fetch failed with status 500");
});
