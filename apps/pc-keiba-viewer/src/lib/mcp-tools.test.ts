// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { indexLiveHorseWeightKg } from "./horse-weight-class";
import { callMcpTool } from "./mcp-tools";
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
