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
        responseCursor: {
          description:
            "Continuation cursor from nextResponseCursor. Repeat the same tool call and concatenate dataChunk values.",
          minimum: 0,
          type: "integer",
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

it("exposes get_daily_finish_predictions for one required JRA or NAR date", () => {
  expect(
    MCP_TOOL_DEFINITIONS.find((definition) => definition.name === "get_daily_finish_predictions"),
  ).toStrictEqual({
    description:
      "Fetch all generated finish-position predictions for one JRA or NAR race day. Returns canonical raceId values, race metadata, ranked runners, model generation timestamps, and unavailable race ids for WIN5 or Triple Uma-tan analysis.",
    inputSchema: {
      additionalProperties: false,
      properties: {
        day: { description: "Calendar day, two digits.", pattern: "^\\d{2}$", type: "string" },
        month: {
          description: "Calendar month, two digits.",
          pattern: "^\\d{2}$",
          type: "string",
        },
        responseCursor: {
          description:
            "Continuation cursor from nextResponseCursor. Repeat the same tool call and concatenate dataChunk values.",
          minimum: 0,
          type: "integer",
        },
        source: {
          description: "jra for WIN5 or nar for Triple Uma-tan.",
          enum: ["jra", "nar"],
          type: "string",
        },
        year: {
          description: "Calendar year, four digits.",
          pattern: "^\\d{4}$",
          type: "string",
        },
      },
      required: ["year", "month", "day", "source"],
      type: "object",
    },
    name: "get_daily_finish_predictions",
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
    async (_pathWithQuery, init) => {
      if (init?.signal !== undefined) {
        signals.push(init.signal);
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
  const chunk = JSON.parse(summary.content[0]?.text ?? "{}");
  expect(summary.isError).toBe(false);
  expect(chunk).toMatchObject({
    complete: false,
    encoding: "json-text",
    nextResponseCursor: 5000,
    responseCursor: 0,
    totalCharacters: 70316,
  });
  expect(chunk.dataChunk).toHaveLength(5000);
});

it("search_entities validates kind and query", async () => {
  const kind = await callMcpTool("search_entities", { kind: "barn", q: "a" }, jsonFetch({}));
  expect(JSON.parse(kind.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "kind must be horse, jockey, owner, or trainer" },
  });
  const query = await callMcpTool("search_entities", { kind: "horse", q: "  " }, jsonFetch({}));
  expect(JSON.parse(query.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "q must be a non-empty search string" },
  });
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
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "viewMode must be winRate, quinellaRate, showRate, or all" },
  });
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

it("get_race_section returns lossless bounded JSON chunks", async () => {
  const first = await callMcpTool(
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
    jsonFetch({
      "/api/races/2026/08/20/05/01/sections/results?source=jra": {
        rows: ["🏇".repeat(5_500)],
      },
    }),
  );
  const firstChunk = JSON.parse(first.content[0]?.text ?? "{}");
  const second = await callMcpTool(
    "get_race_section",
    {
      day: "20",
      keibajoCode: "05",
      month: "08",
      raceNumber: "01",
      responseCursor: 5000,
      section: "results",
      source: "jra",
      year: "2026",
    },
    jsonFetch({
      "/api/races/2026/08/20/05/01/sections/results?source=jra": {
        rows: ["🏇".repeat(5_500)],
      },
    }),
  );
  const secondChunk = JSON.parse(second.content[0]?.text ?? "{}");
  const reconstructed = JSON.parse(`${firstChunk.dataChunk}${secondChunk.dataChunk}`);

  expect(firstChunk).toMatchObject({
    complete: false,
    encoding: "json-text",
    nextResponseCursor: 5000,
    responseCursor: 0,
    totalCharacters: 5513,
  });
  expect(secondChunk).toMatchObject({
    complete: true,
    encoding: "json-text",
    nextResponseCursor: null,
    responseCursor: 5000,
    totalCharacters: 5513,
  });
  expect(reconstructed.rows[0]).toHaveLength(11_000);
  expect(Array.from(reconstructed.rows[0])[0]).toBe("🏇");
  expect(Array.from(reconstructed.rows[0]).at(-1)).toBe("🏇");
});

it("get_latest_odds returns only a bounded page of numeric selection values", async () => {
  const result = await callMcpTool(
    "get_latest_odds",
    {
      day: "29",
      keibajoCode: "07",
      limit: 2,
      month: "08",
      oddsType: "3rentan",
      offset: 1,
      raceNumber: "06",
      source: "jra",
      year: "2026",
    },
    jsonFetch({
      "/api/races/2026/08/29/07/06/realtime?source=jra": {
        odds: {
          fetchedAt: "2026-08-29T05:44:00.000Z",
          history: ["large history is deliberately ignored"],
          latest: {
            "3rentan": [
              { combination: "1-2-3", odds: 12.3, rank: 1 },
              {
                averageOdds: 24.5,
                combination: "1-3-2",
                maxOdds: 25,
                minOdds: 24,
                rank: 2,
              },
              { combination: "2-1-3", odds: 30.1, rank: 3 },
              { combination: "2-3-1", odds: 40.2, rank: 4 },
            ],
          },
          trendsByType: { "3rentan": ["large trends are deliberately ignored"] },
        },
        raceKey: "jra:2026:0829:07:06",
      },
    }),
  );
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    fetchedAt: "2026-08-29T05:44:00.000Z",
    items: [
      {
        averageOdds: 24.5,
        combination: "1-3-2",
        maxOdds: 25,
        minOdds: 24,
        odds: null,
        rank: 2,
      },
      {
        averageOdds: null,
        combination: "2-1-3",
        maxOdds: null,
        minOdds: null,
        odds: 30.1,
        rank: 3,
      },
    ],
    limit: 2,
    nextOffset: 3,
    oddsType: "3rentan",
    offset: 1,
    raceKey: "jra:2026:0829:07:06",
    total: 4,
  });
});

it("get_latest_odds can return one exact betting combination", async () => {
  const result = await callMcpTool(
    "get_latest_odds",
    {
      combination: "2-5",
      day: "29",
      keibajoCode: "04",
      month: "08",
      oddsType: "umaren",
      raceNumber: "08",
      source: "jra",
      year: "2026",
    },
    jsonFetch({
      "/api/races/2026/08/29/04/08/realtime?source=jra": {
        odds: {
          fetchedAt: "2026-08-29T05:45:00.000Z",
          latest: {
            umaren: [
              { combination: "1-2", odds: 8.8, rank: 1 },
              { combination: "2-5", odds: 15.6, rank: 4 },
            ],
          },
        },
        raceKey: "jra:2026:0829:04:08",
      },
    }),
  );
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    fetchedAt: "2026-08-29T05:45:00.000Z",
    items: [
      {
        averageOdds: null,
        combination: "2-5",
        maxOdds: null,
        minOdds: null,
        odds: 15.6,
        rank: 4,
      },
    ],
    limit: 20,
    nextOffset: null,
    oddsType: "umaren",
    offset: 0,
    raceKey: "jra:2026:0829:04:08",
    total: 1,
  });
});

it("get_latest_odds validates source, odds type, offset, and limit as JSON errors", async () => {
  const source = await callMcpTool(
    "get_latest_odds",
    {
      day: "29",
      keibajoCode: "04",
      month: "08",
      oddsType: "tansho",
      raceNumber: "08",
      year: "2026",
    },
    jsonFetch({}),
  );
  const oddsType = await callMcpTool(
    "get_latest_odds",
    {
      day: "29",
      keibajoCode: "04",
      month: "08",
      oddsType: "invalid",
      raceNumber: "08",
      source: "jra",
      year: "2026",
    },
    jsonFetch({}),
  );
  const offset = await callMcpTool(
    "get_latest_odds",
    {
      day: "29",
      keibajoCode: "04",
      month: "08",
      oddsType: "tansho",
      offset: -1,
      raceNumber: "08",
      source: "jra",
      year: "2026",
    },
    jsonFetch({}),
  );
  const limit = await callMcpTool(
    "get_latest_odds",
    {
      day: "29",
      keibajoCode: "04",
      limit: 26,
      month: "08",
      oddsType: "tansho",
      raceNumber: "08",
      source: "jra",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(source.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "source must be jra or nar" },
  });
  expect(JSON.parse(oddsType.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      message:
        "oddsType must be 3renpuku, 3rentan, fukusho, tansho, umaren, umatan, wakuren, or wide",
    },
  });
  expect(JSON.parse(offset.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "offset must be a non-negative integer" },
  });
  expect(JSON.parse(limit.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "limit must be an integer from 1 to 25" },
  });
});

it("get_latest_odds returns an empty JSON page when current odds are unavailable", async () => {
  const result = await callMcpTool(
    "get_latest_odds",
    {
      day: "29",
      keibajoCode: "04",
      month: "08",
      oddsType: "wide",
      raceNumber: "08",
      source: "jra",
      year: "2026",
    },
    jsonFetch({
      "/api/races/2026/08/29/04/08/realtime?source=jra": {
        odds: null,
        raceKey: "jra:2026:0829:04:08",
      },
    }),
  );
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    fetchedAt: null,
    items: [],
    limit: 20,
    nextOffset: null,
    oddsType: "wide",
    offset: 0,
    raceKey: "jra:2026:0829:04:08",
    total: 0,
  });
});

it("get_latest_odds ignores malformed selections and reports upstream JSON errors", async () => {
  const malformed = await callMcpTool(
    "get_latest_odds",
    {
      day: "29",
      keibajoCode: "04",
      month: "08",
      oddsType: "tansho",
      raceNumber: "08",
      source: "jra",
      year: "2026",
    },
    jsonFetch({
      "/api/races/2026/08/29/04/08/realtime?source=jra": {
        odds: {
          fetchedAt: 123,
          latest: {
            tansho: [null, { combination: 1 }, { combination: "2", odds: "4.2", rank: "1" }],
          },
        },
        raceKey: 123,
      },
    }),
  );
  const failed = await callMcpTool(
    "get_latest_odds",
    {
      day: "29",
      keibajoCode: "04",
      month: "08",
      oddsType: "tansho",
      raceNumber: "08",
      source: "jra",
      year: "2026",
    },
    failingFetch,
  );
  expect(JSON.parse(malformed.content[0]?.text ?? "{}")).toStrictEqual({
    fetchedAt: null,
    items: [
      {
        averageOdds: null,
        combination: "2",
        maxOdds: null,
        minOdds: null,
        odds: null,
        rank: null,
      },
    ],
    limit: 20,
    nextOffset: null,
    oddsType: "tansho",
    offset: 0,
    raceKey: null,
    total: 1,
  });
  expect(JSON.parse(failed.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "get_latest_odds failed with status 500" },
  });
});

it("rejects invalid and exhausted response cursors", async () => {
  const invalid = await callMcpTool(
    "get_api_spec",
    { responseCursor: -1 },
    jsonFetch({ "/api/spec": { ok: true } }),
  );
  const exhausted = await callMcpTool(
    "get_api_spec",
    { responseCursor: 20 },
    jsonFetch({ "/api/spec": { ok: true } }),
  );
  expect(JSON.parse(invalid.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "responseCursor must be a non-negative integer" },
  });
  expect(JSON.parse(exhausted.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "responseCursor is outside the serialized JSON response" },
  });
});

it("get_daily_finish_predictions fetches the complete selected day", async () => {
  const result = await callMcpTool(
    "get_daily_finish_predictions",
    { day: "24", month: "05", source: "jra", year: "2026" },
    jsonFetch({
      "/api/finish-predictions/daily?day=24&month=05&source=jra&year=2026": {
        availableRaceCount: 1,
        date: "2026-05-24",
        raceCount: 1,
        races: [{ raceId: "jra:2026:0524:05:11" }],
        source: "jra",
        unavailableRaceIds: [],
      },
    }),
  );
  expect(result.isError).toBe(false);
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    availableRaceCount: 1,
    date: "2026-05-24",
    raceCount: 1,
    races: [{ raceId: "jra:2026:0524:05:11" }],
    source: "jra",
    unavailableRaceIds: [],
  });
});

it("get_daily_finish_predictions validates its date and source", async () => {
  const year = await callMcpTool(
    "get_daily_finish_predictions",
    { day: "24", month: "05", source: "jra", year: "26" },
    jsonFetch({}),
  );
  const month = await callMcpTool(
    "get_daily_finish_predictions",
    { day: "24", month: "5", source: "jra", year: "2026" },
    jsonFetch({}),
  );
  const day = await callMcpTool(
    "get_daily_finish_predictions",
    { day: "2", month: "05", source: "jra", year: "2026" },
    jsonFetch({}),
  );
  const source = await callMcpTool(
    "get_daily_finish_predictions",
    { day: "24", month: "05", source: "overseas", year: "2026" },
    jsonFetch({}),
  );
  expect(JSON.parse(year.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "year must be a 4-digit calendar year" },
  });
  expect(JSON.parse(month.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "month must be a 2-digit calendar month" },
  });
  expect(JSON.parse(day.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "day must be a 2-digit calendar day" },
  });
  expect(JSON.parse(source.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "source must be jra or nar" },
  });
});

it("get_daily_finish_predictions reports its Worker API failure", async () => {
  const result = await callMcpTool(
    "get_daily_finish_predictions",
    { day: "24", month: "05", source: "nar", year: "2026" },
    failingFetch,
  );
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "get_daily_finish_predictions failed with status 500" },
  });
});

it("get_race_section validates the race route and section name", async () => {
  const year = await callMcpTool("get_race_section", { year: "20" }, jsonFetch({}));
  expect(JSON.parse(year.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "year must be a 4-digit calendar year" },
  });
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
  expect(JSON.parse(section.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "section is not a supported race detail section" },
  });
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
  expect(JSON.parse(spec.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "get_api_spec failed with status 500" },
  });
  const top = await callMcpTool("list_top_races", {}, failingFetch);
  expect(JSON.parse(top.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "list_top_races failed with status 500" },
  });
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
  expect(JSON.parse(heatmap.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "win-rate-heatmap section payload is unavailable" },
  });
});

it("parseRaceRoute rejects month, day, venue, race number, and source", async () => {
  const month = await callMcpTool("get_race_section", { year: "2026", month: "8" }, jsonFetch({}));
  expect(JSON.parse(month.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "month must be a 2-digit calendar month" },
  });
  const day = await callMcpTool(
    "get_race_section",
    { year: "2026", month: "08", day: "2" },
    jsonFetch({}),
  );
  expect(JSON.parse(day.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "day must be a 2-digit calendar day" },
  });
  const venue = await callMcpTool(
    "get_race_section",
    { year: "2026", month: "08", day: "20", keibajoCode: "x" },
    jsonFetch({}),
  );
  expect(JSON.parse(venue.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "keibajoCode must be a 2-character venue code" },
  });
  const race = await callMcpTool(
    "get_race_section",
    { year: "2026", month: "08", day: "20", keibajoCode: "05", raceNumber: "1" },
    jsonFetch({}),
  );
  expect(JSON.parse(race.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "raceNumber must be a 2-digit race number" },
  });
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
  expect(JSON.parse(source.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "source must be jra or nar when provided" },
  });
});

it("callMcpTool rejects unknown tools and non-object arguments", async () => {
  const unknown = await callMcpTool("drop_database", {}, jsonFetch({}));
  expect(JSON.parse(unknown.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "Unknown tool: drop_database" },
  });
  const args = await callMcpTool("get_api_spec", ["bad"], jsonFetch({}));
  expect(JSON.parse(args.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "Tool arguments must be an object" },
  });
});

it("search requires a non-empty query", async () => {
  const empty = await callMcpTool("search", { query: "  " }, jsonFetch({}));
  expect(JSON.parse(empty.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "query must be a non-empty search string" },
  });
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

const paddockState = {
  history: [],
  horses: {},
  raceKey: "20260601:05:11",
  updatedAt: "2026-06-01T00:00:00.000Z",
};

it("get_json can read paddock evaluation state", async () => {
  const result = await callMcpTool(
    "get_json",
    { path: "/api/races/2026/06/01/05/11/paddock" },
    jsonFetch({ "/api/races/2026/06/01/05/11/paddock": paddockState }),
  );
  expect(result.isError).toBe(false);
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    history: [],
    horses: {},
    raceKey: "20260601:05:11",
    updatedAt: "2026-06-01T00:00:00.000Z",
  });
});

it("get_paddock_state reads paddock evaluation state", async () => {
  const result = await callMcpTool(
    "get_paddock_state",
    {
      day: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      year: "2026",
    },
    jsonFetch({ "/api/races/2026/06/01/05/11/paddock": paddockState }),
  );
  expect(result.isError).toBe(false);
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    history: [],
    horses: {},
    raceKey: "20260601:05:11",
    updatedAt: "2026-06-01T00:00:00.000Z",
  });
});

it("get_paddock_state validates the race route", async () => {
  const result = await callMcpTool("get_paddock_state", { year: "20" }, jsonFetch({}));
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "year must be a 4-digit calendar year" },
  });
});

it("get_paddock_state reports upstream failure", async () => {
  const result = await callMcpTool(
    "get_paddock_state",
    {
      day: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      year: "2026",
    },
    failingFetch,
  );
  expect(result.isError).toBe(true);
});

it("update_paddock_state posts a score action", async () => {
  const posted: Array<{ body?: string; method?: string; path: string }> = [];
  const result = await callMcpTool(
    "update_paddock_state",
    {
      actionType: "score",
      category: "paddock",
      day: "01",
      delta: 1,
      horseName: "Alpha",
      horseNumber: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      userId: "user-1",
      year: "2026",
    },
    async (path, init) => {
      posted.push({ body: init?.body, method: init?.method, path });
      return new Response(JSON.stringify(paddockState), {
        headers: { "content-type": "application/json" },
        status: 200,
      });
    },
  );
  expect(result.isError).toBe(false);
  expect(posted[0]?.path).toBe("/api/races/2026/06/01/05/11/paddock");
  expect(posted[0]?.method).toBe("POST");
  expect(posted[0]?.body).toBe(
    '{"category":"paddock","delta":1,"horseName":"Alpha","horseNumber":"01","userId":"user-1"}',
  );
});

it("update_paddock_state posts an official-rank action", async () => {
  const posted: Array<{ body?: string }> = [];
  const result = await callMcpTool(
    "update_paddock_state",
    {
      actionType: "official-rank",
      day: "01",
      horseName: "Alpha",
      horseNumber: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      rank: 3,
      year: "2026",
    },
    async (path, init) => {
      posted.push({ body: init?.body });
      return new Response(JSON.stringify(paddockState), {
        headers: { "content-type": "application/json" },
        status: 200,
      });
    },
  );
  expect(result.isError).toBe(false);
  expect(posted[0]?.body).toBe(
    '{"horseName":"Alpha","horseNumber":"01","rank":3,"type":"official-rank"}',
  );
});

it("update_paddock_state posts a null official rank to clear it", async () => {
  const posted: Array<{ body?: string }> = [];
  const result = await callMcpTool(
    "update_paddock_state",
    {
      actionType: "official-rank",
      day: "01",
      horseName: "Alpha",
      horseNumber: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      rank: null,
      year: "2026",
    },
    async (_path, init) => {
      posted.push({ body: init?.body });
      return new Response(JSON.stringify(paddockState), {
        headers: { "content-type": "application/json" },
        status: 200,
      });
    },
  );
  expect(result.isError).toBe(false);
  expect(posted[0]?.body).toBe(
    '{"horseName":"Alpha","horseNumber":"01","rank":null,"type":"official-rank"}',
  );
});

it("update_paddock_state rejects an invalid action", async () => {
  const missingType = await callMcpTool(
    "update_paddock_state",
    {
      day: "01",
      horseName: "Alpha",
      horseNumber: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(missingType.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "actionType must be score or official-rank" },
  });
  const badScore = await callMcpTool(
    "update_paddock_state",
    {
      actionType: "score",
      day: "01",
      horseName: "Alpha",
      horseNumber: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(badScore.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      message: "score requires horseName, horseNumber, category, and delta 1 or -1",
    },
  });
  const badRank = await callMcpTool(
    "update_paddock_state",
    {
      actionType: "official-rank",
      day: "01",
      horseName: "Alpha",
      horseNumber: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      rank: 11,
      year: "2026",
    },
    jsonFetch({}),
  );
  expect(JSON.parse(badRank.content[0]?.text ?? "{}")).toStrictEqual({
    error: {
      message: "official-rank requires horseName, horseNumber, and rank 1-10 or null",
    },
  });
});

it("update_paddock_state validates the race route", async () => {
  const result = await callMcpTool("update_paddock_state", { year: "20" }, jsonFetch({}));
  expect(JSON.parse(result.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "year must be a 4-digit calendar year" },
  });
});

it("update_paddock_state reports malformed JSON from the paddock API", async () => {
  const result = await callMcpTool(
    "update_paddock_state",
    {
      actionType: "score",
      category: "kaeshi",
      day: "01",
      delta: 1,
      horseName: "Alpha",
      horseNumber: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      year: "2026",
    },
    async () => new Response("not-json", { status: 200 }),
  );
  expect(result.isError).toBe(false);
  expect(JSON.parse(result.content[0]?.text ?? '""')).toBe("not-json");
});

it("update_paddock_state reports upstream failure", async () => {
  const result = await callMcpTool(
    "update_paddock_state",
    {
      actionType: "score",
      category: "attention",
      day: "01",
      delta: -1,
      horseName: "Alpha",
      horseNumber: "01",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      year: "2026",
    },
    failingFetch,
  );
  expect(result.isError).toBe(true);
});

it("exposes get_paddock_state and update_paddock_state tools", () => {
  expect(
    MCP_TOOL_DEFINITIONS.find((definition) => definition.name === "get_paddock_state")?.name,
  ).toBe("get_paddock_state");
  expect(
    MCP_TOOL_DEFINITIONS.find((definition) => definition.name === "update_paddock_state")?.name,
  ).toBe("update_paddock_state");
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
  expect(JSON.parse(blank.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "id must be a non-empty string" },
  });
  const malformed = await callMcpTool("fetch", { id: "nocolon" }, jsonFetch({}));
  expect(JSON.parse(malformed.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "id must be kind:id from search, or an /api path" },
  });
  const badKind = await callMcpTool("fetch", { id: "barn:1" }, jsonFetch({}));
  expect(JSON.parse(badKind.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "id must be kind:id from search, or an /api path" },
  });
  const missing = await callMcpTool(
    "fetch",
    { id: "horse:missing" },
    jsonFetch({
      "/api/mypage/favorites/search?kind=horse&q=missing": { results: [] },
    }),
  );
  expect(JSON.parse(missing.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "fetch id was not found" },
  });
  const failedApi = await callMcpTool("fetch", { id: "/api/spec" }, failingFetch);
  expect(JSON.parse(failedApi.content[0]?.text ?? "{}")).toStrictEqual({
    error: { message: "fetch failed with status 500" },
  });
});
