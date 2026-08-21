// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { indexLiveHorseWeightKg, type LiveHorseWeight } from "./horse-weight-class";
import { resolveMcpApiPath } from "./mcp-allowlist";
import {
  buildWinRateHeatmapDisplay,
  DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS,
  DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  type WinRateHeatmapViewMode,
} from "./win-rate-heatmap";
import { isWinRateHeatmapSectionPayload } from "./win-rate-heatmap-cache";

export type McpSiteFetch = (pathWithQuery: string) => Promise<Response>;

interface McpJsonSchemaProperty {
  description?: string;
  enum?: readonly string[];
  minLength?: number;
  pattern?: string;
  type: string;
}

interface McpJsonSchemaObject {
  additionalProperties: boolean;
  properties: Record<string, McpJsonSchemaProperty>;
  required: readonly string[];
  type: "object";
}

export interface McpToolDefinition {
  description: string;
  inputSchema: McpJsonSchemaObject;
  name: string;
}

export interface McpTextContent {
  text: string;
  type: "text";
}

export interface McpToolResult {
  content: readonly McpTextContent[];
  isError: boolean;
}

interface FavoriteSearchRow {
  id: string;
  kind: string;
  label: string;
  meta: string;
}

interface ChatgptSearchHit {
  id: string;
  title: string;
  url: string;
}

interface SearchKindRowsInput {
  fetchSite: McpSiteFetch;
  kind: string;
  query: string;
}

const YEAR_PATTERN: string = "^\\d{4}$";
const MONTH_DAY_RACE_PATTERN: string = "^\\d{2}$";
const KEIBAJO_PATTERN: string = "^[0-9A-Z]{2}$";
const SOURCE_JRA: string = "jra";
const SOURCE_NAR: string = "nar";
const VIEW_MODE_LIST: readonly WinRateHeatmapViewMode[] = [
  "all",
  "quinellaRate",
  "showRate",
  "winRate",
];

const isHeatmapViewMode = (value: string): value is WinRateHeatmapViewMode =>
  VIEW_MODE_LIST.some((mode) => mode === value);
const SEARCH_KINDS: ReadonlySet<string> = new Set(["horse", "jockey", "owner", "trainer"]);
const ENTITY_PAGE_PATH: ReadonlyMap<string, string> = new Map([
  ["horse", "/horses/"],
  ["jockey", "/jockeys/"],
  ["owner", "/owners/"],
  ["trainer", "/trainers/"],
]);
const RACE_SECTIONS: ReadonlySet<string> = new Set([
  "ability",
  "bloodline",
  "condition",
  "finish-prediction",
  "overall-score",
  "pace-prediction",
  "premium-data-top",
  "results",
  "similar",
  "time-score",
  "training",
  "win-rate-heatmap",
]);

const EMPTY_SCHEMA: McpJsonSchemaObject = {
  additionalProperties: false,
  properties: {},
  required: [],
  type: "object",
};

const STRING_ARG = (description: string, pattern: string): McpJsonSchemaProperty => ({
  description,
  pattern,
  type: "string",
});

const RACE_ROUTE_PROPERTIES: Record<string, McpJsonSchemaProperty> = {
  day: STRING_ARG("Calendar day, two digits.", MONTH_DAY_RACE_PATTERN),
  keibajoCode: STRING_ARG("Venue code.", KEIBAJO_PATTERN),
  month: STRING_ARG("Calendar month, two digits.", MONTH_DAY_RACE_PATTERN),
  raceNumber: STRING_ARG("Race number, two digits.", MONTH_DAY_RACE_PATTERN),
  source: {
    description: "Optional jra or nar source override.",
    enum: [SOURCE_JRA, SOURCE_NAR],
    type: "string",
  },
  year: STRING_ARG("Calendar year, four digits.", YEAR_PATTERN),
};

export const MCP_TOOL_DEFINITIONS: readonly McpToolDefinition[] = [
  {
    description:
      "Verify MCP bearer auth and that this Worker can read /api/spec (the same spec the site serves). Cloudflare Access for humans is unchanged; this tool runs only after Access and MCP bearer both succeed.",
    inputSchema: EMPTY_SCHEMA,
    name: "authenticate",
  },
  {
    description: "Fetch /api/spec from this Worker — the OpenAPI document the site publishes.",
    inputSchema: EMPTY_SCHEMA,
    name: "get_api_spec",
  },
  {
    description: "Fetch /api/top-races from this Worker.",
    inputSchema: EMPTY_SCHEMA,
    name: "list_top_races",
  },
  {
    description: "Search horses, jockeys, owners, or trainers via /api/mypage/favorites/search.",
    inputSchema: {
      additionalProperties: false,
      properties: {
        kind: {
          description: "Entity kind to search.",
          enum: ["horse", "jockey", "owner", "trainer"],
          type: "string",
        },
        q: { description: "Search string.", minLength: 1, type: "string" },
      },
      required: ["kind", "q"],
      type: "object",
    },
    name: "search_entities",
  },
  {
    description: "GET an allowlisted /api path on this Worker (same handlers the browser uses).",
    inputSchema: {
      additionalProperties: false,
      properties: { path: STRING_ARG("Path beginning with /api/.", "^/") },
      required: ["path"],
      type: "object",
    },
    name: "get_json",
  },
  {
    description: "Fetch a race-detail section JSON payload — the same payload React hydrates.",
    inputSchema: {
      additionalProperties: false,
      properties: {
        ...RACE_ROUTE_PROPERTIES,
        section: { description: "Detail section id.", type: "string" },
      },
      required: ["year", "month", "day", "keibajoCode", "raceNumber", "section"],
      type: "object",
    },
    name: "get_race_section",
  },
  {
    description:
      "Build the win-rate heatmap display model with the same buildWinRateHeatmapDisplay function the on-screen table uses. Defaults match first paint (勝率, レース数 off).",
    inputSchema: {
      additionalProperties: false,
      properties: {
        ...RACE_ROUTE_PROPERTIES,
        showStarts: { description: "When true, include (n) start labels.", type: "boolean" },
        viewMode: {
          description: "Heatmap view mode.",
          enum: ["winRate", "quinellaRate", "showRate", "all"],
          type: "string",
        },
      },
      required: ["year", "month", "day", "keibajoCode", "raceNumber"],
      type: "object",
    },
    name: "get_win_rate_heatmap_display",
  },
  {
    description:
      "ChatGPT search tool. Searches horses, jockeys, owners, and trainers. Returns id/title/url results.",
    inputSchema: {
      additionalProperties: false,
      properties: { query: { description: "Search string.", minLength: 1, type: "string" } },
      required: ["query"],
      type: "object",
    },
    name: "search",
  },
  {
    description:
      "ChatGPT fetch tool. Loads a search result id (kind:id) or an allowlisted /api path.",
    inputSchema: {
      additionalProperties: false,
      properties: {
        id: { description: "Result id from search, or an /api path.", type: "string" },
      },
      required: ["id"],
      type: "object",
    },
    name: "fetch",
  },
];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const readString = (args: Record<string, unknown>, key: string): string | null => {
  const value = args[key];
  return typeof value === "string" ? value : null;
};

const matches = (value: string, pattern: string): boolean => new RegExp(pattern, "u").test(value);

const errorResult = (message: string): McpToolResult => ({
  content: [{ text: message, type: "text" }],
  isError: true,
});

const okJson = (value: unknown): McpToolResult => ({
  content: [{ text: JSON.stringify(value), type: "text" }],
  isError: false,
});

interface RaceRoute {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: string | null;
  year: string;
}

const parseRaceRoute = (args: Record<string, unknown>): RaceRoute | string => {
  const year = readString(args, "year");
  const month = readString(args, "month");
  const day = readString(args, "day");
  const keibajoCode = readString(args, "keibajoCode");
  const raceNumber = readString(args, "raceNumber");
  if (year === null || !matches(year, YEAR_PATTERN)) {
    return "year must be a 4-digit calendar year";
  }
  if (month === null || !matches(month, MONTH_DAY_RACE_PATTERN)) {
    return "month must be a 2-digit calendar month";
  }
  if (day === null || !matches(day, MONTH_DAY_RACE_PATTERN)) {
    return "day must be a 2-digit calendar day";
  }
  if (keibajoCode === null || !matches(keibajoCode, KEIBAJO_PATTERN)) {
    return "keibajoCode must be a 2-character venue code";
  }
  if (raceNumber === null || !matches(raceNumber, MONTH_DAY_RACE_PATTERN)) {
    return "raceNumber must be a 2-digit race number";
  }
  const sourceRaw = args.source;
  if (sourceRaw === undefined) {
    return { day, keibajoCode, month, raceNumber, source: null, year };
  }
  if (sourceRaw !== SOURCE_JRA && sourceRaw !== SOURCE_NAR) {
    return "source must be jra or nar when provided";
  }
  return { day, keibajoCode, month, raceNumber, source: sourceRaw, year };
};

const raceApiPath = (route: RaceRoute, suffix: string): string => {
  const base = `/api/races/${route.year}/${route.month}/${route.day}/${route.keibajoCode}/${route.raceNumber}/${suffix}`;
  return route.source === null ? base : `${base}?source=${route.source}`;
};

const fetchSiteJson = async (
  fetchSite: McpSiteFetch,
  pathWithQuery: string,
): Promise<{ ok: boolean; status: number; value: unknown }> => {
  const allowed = resolveMcpApiPath(pathWithQuery);
  if (allowed === null) {
    return { ok: false, status: 403, value: "Path is not allowlisted for MCP reads" };
  }
  const response = await fetchSite(allowed);
  const text = await response.text();
  try {
    return { ok: response.ok, status: response.status, value: JSON.parse(text) };
  } catch {
    return { ok: response.ok, status: response.status, value: text };
  }
};

const readLiveWeights = (realtime: unknown): Map<string, number> => {
  if (!isRecord(realtime)) {
    return new Map();
  }
  const horseWeights = realtime.horseWeights;
  if (!isRecord(horseWeights) || !Array.isArray(horseWeights.horses)) {
    return new Map();
  }
  const horses: LiveHorseWeight[] = horseWeights.horses.flatMap((entry) => {
    if (!isRecord(entry) || typeof entry.horseNumber !== "string") {
      return [];
    }
    const weight = entry.weight;
    return [
      {
        horseNumber: entry.horseNumber,
        weight: typeof weight === "number" ? weight : null,
      },
    ];
  });
  return indexLiveHorseWeightKg(horses);
};

export const callMcpTool = async (
  name: string,
  rawArgs: unknown,
  fetchSite: McpSiteFetch,
): Promise<McpToolResult> => {
  const args = rawArgs === undefined || rawArgs === null ? {} : rawArgs;
  if (!isRecord(args)) {
    return errorResult("Tool arguments must be an object");
  }
  if (name === "authenticate" || name === "get_api_spec") {
    const fetched = await fetchSiteJson(fetchSite, "/api/spec");
    if (name === "authenticate") {
      return okJson({
        accessCoexists: true,
        mcpAuthenticated: true,
        specOk: fetched.ok,
        specStatus: fetched.status,
      });
    }
    if (!fetched.ok) {
      return errorResult(`get_api_spec failed with status ${fetched.status}`);
    }
    return okJson(fetched.value);
  }
  if (name === "list_top_races") {
    const fetched = await fetchSiteJson(fetchSite, "/api/top-races");
    if (!fetched.ok) {
      return errorResult(`list_top_races failed with status ${fetched.status}`);
    }
    return okJson(fetched.value);
  }
  if (name === "search_entities") {
    const kind = readString(args, "kind");
    const q = readString(args, "q");
    if (kind === null || !SEARCH_KINDS.has(kind)) {
      return errorResult("kind must be horse, jockey, owner, or trainer");
    }
    if (q === null || q.trim().length === 0) {
      return errorResult("q must be a non-empty search string");
    }
    const path = `/api/mypage/favorites/search?kind=${encodeURIComponent(kind)}&q=${encodeURIComponent(q)}`;
    const fetched = await fetchSiteJson(fetchSite, path);
    if (!fetched.ok) {
      return errorResult(`search_entities failed with status ${fetched.status}`);
    }
    return okJson(fetched.value);
  }
  if (name === "get_json") {
    const path = readString(args, "path");
    if (path === null) {
      return errorResult("path must start with /");
    }
    const fetched = await fetchSiteJson(fetchSite, path);
    if (!fetched.ok) {
      return {
        content: [{ text: JSON.stringify(fetched.value), type: "text" }],
        isError: true,
      };
    }
    return okJson(fetched.value);
  }
  if (name === "get_race_section") {
    const parsed = parseRaceRoute(args);
    if (typeof parsed === "string") {
      return errorResult(parsed);
    }
    const section = readString(args, "section");
    if (section === null || !RACE_SECTIONS.has(section)) {
      return errorResult("section is not a supported race detail section");
    }
    const fetched = await fetchSiteJson(fetchSite, raceApiPath(parsed, `sections/${section}`));
    if (!fetched.ok) {
      return errorResult(`get_race_section failed with status ${fetched.status}`);
    }
    return okJson(fetched.value);
  }
  if (name === "get_win_rate_heatmap_display") {
    const parsed = parseRaceRoute(args);
    if (typeof parsed === "string") {
      return errorResult(parsed);
    }
    const viewModeRaw = args.viewMode;
    if (
      viewModeRaw !== undefined &&
      (typeof viewModeRaw !== "string" || !isHeatmapViewMode(viewModeRaw))
    ) {
      return errorResult("viewMode must be winRate, quinellaRate, showRate, or all");
    }
    const viewMode: WinRateHeatmapViewMode =
      typeof viewModeRaw === "string" && isHeatmapViewMode(viewModeRaw)
        ? viewModeRaw
        : DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE;
    const showStarts =
      typeof args.showStarts === "boolean" ? args.showStarts : DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS;
    const section = await fetchSiteJson(
      fetchSite,
      raceApiPath(parsed, "sections/win-rate-heatmap"),
    );
    if (!section.ok || !isWinRateHeatmapSectionPayload(section.value)) {
      return errorResult("win-rate-heatmap section payload is unavailable");
    }
    const realtime = await fetchSiteJson(fetchSite, raceApiPath(parsed, "realtime"));
    const display = buildWinRateHeatmapDisplay({
      bloodlineRows: section.value.bloodlineRows,
      carriedWeightClassStats: section.value.carriedWeightClassStats,
      frameStats: section.value.frameStats,
      horseResults: section.value.horseResults,
      keibajoCode: parsed.keibajoCode,
      liveWeightKgByHorse: readLiveWeights(realtime.value),
      runners: section.value.runners,
      showStarts,
      similarRows: section.value.similarRows,
      viewMode,
      weightClassStats: section.value.weightClassStats,
    });
    return okJson(display);
  }
  if (name === "search") {
    return searchForChatgpt(args, fetchSite);
  }
  if (name === "fetch") {
    return fetchForChatgpt(args, fetchSite);
  }
  return errorResult(`Unknown tool: ${name}`);
};

const readFavoriteRows = (payload: unknown): FavoriteSearchRow[] => {
  if (!isRecord(payload) || !Array.isArray(payload.results)) {
    return [];
  }
  return payload.results.flatMap((entry) => {
    if (!isRecord(entry)) {
      return [];
    }
    const id = entry.id;
    const kind = entry.kind;
    const label = entry.label;
    const meta = entry.meta;
    if (typeof id !== "string" || typeof kind !== "string" || typeof label !== "string") {
      return [];
    }
    return [
      {
        id,
        kind,
        label,
        meta: typeof meta === "string" ? meta : "",
      },
    ];
  });
};

const toChatgptHit = (row: FavoriteSearchRow): ChatgptSearchHit => {
  const prefix = ENTITY_PAGE_PATH.get(row.kind);
  const path = prefix === undefined ? `/${row.kind}/${row.id}` : `${prefix}${row.id}`;
  return {
    id: `${row.kind}:${row.id}`,
    title: `${row.label} (${row.kind})`,
    url: path,
  };
};

const searchKindRows = async (input: SearchKindRowsInput): Promise<FavoriteSearchRow[]> => {
  const path = `/api/mypage/favorites/search?kind=${encodeURIComponent(input.kind)}&q=${encodeURIComponent(input.query)}`;
  const fetched = await fetchSiteJson(input.fetchSite, path);
  if (!fetched.ok) {
    return [];
  }
  return readFavoriteRows(fetched.value);
};

const searchForChatgpt = async (
  args: Record<string, unknown>,
  fetchSite: McpSiteFetch,
): Promise<McpToolResult> => {
  const query = readString(args, "query");
  if (query === null || query.trim().length === 0) {
    return errorResult("query must be a non-empty search string");
  }
  const horseRows = await searchKindRows({ fetchSite, kind: "horse", query });
  const jockeyRows = await searchKindRows({ fetchSite, kind: "jockey", query });
  const ownerRows = await searchKindRows({ fetchSite, kind: "owner", query });
  const trainerRows = await searchKindRows({ fetchSite, kind: "trainer", query });
  const results = [...horseRows, ...jockeyRows, ...ownerRows, ...trainerRows].map(toChatgptHit);
  return okJson({ results });
};

const fetchForChatgpt = async (
  args: Record<string, unknown>,
  fetchSite: McpSiteFetch,
): Promise<McpToolResult> => {
  const id = readString(args, "id");
  if (id === null || id.trim().length === 0) {
    return errorResult("id must be a non-empty string");
  }
  if (id.startsWith("/api/")) {
    const fetched = await fetchSiteJson(fetchSite, id);
    if (!fetched.ok) {
      return errorResult(`fetch failed with status ${fetched.status}`);
    }
    return okJson({
      id,
      metadata: { kind: "api" },
      text: JSON.stringify(fetched.value),
      title: id,
      url: id,
    });
  }
  const separator = id.indexOf(":");
  if (separator < 1) {
    return errorResult("id must be kind:id from search, or an /api path");
  }
  const kind = id.slice(0, separator);
  const entityId = id.slice(separator + 1);
  if (!SEARCH_KINDS.has(kind) || entityId.length === 0) {
    return errorResult("id must be kind:id from search, or an /api path");
  }
  const rows = await searchKindRows({ fetchSite, kind, query: entityId });
  const row = rows.find((entry) => entry.id === entityId);
  if (row === undefined) {
    return errorResult("fetch id was not found");
  }
  const hit = toChatgptHit(row);
  return okJson({
    id: hit.id,
    metadata: { kind: row.kind, meta: row.meta },
    text: JSON.stringify(row),
    title: hit.title,
    url: hit.url,
  });
};
