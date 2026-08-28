// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { KEIBAJO_NAMES } from "./codes";
import { indexLiveHorseWeightKg, type LiveHorseWeight } from "./horse-weight-class";
import { resolveMcpApiPath, resolveMcpPaddockWritePath } from "./mcp-allowlist";
import {
  buildFinishPredictionSummary,
  createFinishPredictionSummaryError,
  type FinishPredictionSummaryError,
  type FinishPredictionSummaryRoute,
} from "./mcp-finish-prediction-summary";
import { isPaddockAction, type PaddockAction } from "./paddock";
import { inferRaceSourceFromKeibajoCode } from "./runner-format";
import {
  buildWinRateHeatmapDisplay,
  DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS,
  DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  type WinRateHeatmapViewMode,
} from "./win-rate-heatmap";
import { isWinRateHeatmapSectionPayload } from "./win-rate-heatmap-cache";

export interface McpSiteFetchInit {
  body?: string;
  method?: string;
  signal?: AbortSignal;
}

export type McpSiteFetch = (pathWithQuery: string, init?: McpSiteFetchInit) => Promise<Response>;

interface McpJsonSchemaProperty {
  description?: string;
  enum?: readonly (number | string | null)[];
  maximum?: number;
  minimum?: number;
  minLength?: number;
  pattern?: string;
  type: string | readonly string[];
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

interface FetchSiteJsonWriteParams {
  body: string;
  fetchSite: McpSiteFetch;
  pathWithQuery: string;
}

interface BoundedResponseText {
  byteLength: number;
  status: "ok";
  text: string;
}

interface OversizedResponseText {
  status: "too-large";
}

type ReadBoundedResponseResult = BoundedResponseText | OversizedResponseText;

type FinishPredictionRouteParseResult =
  | { error: FinishPredictionSummaryError; status: "error" }
  | { route: FinishPredictionSummaryRoute; status: "ok" };

const YEAR_PATTERN: string = "^\\d{4}$";
const MONTH_DAY_RACE_PATTERN: string = "^\\d{2}$";
const KEIBAJO_PATTERN: string = "^[0-9A-Z]{2}$";
const SOURCE_JRA: string = "jra";
const SOURCE_NAR: string = "nar";
const MAX_FINISH_PREDICTION_UPSTREAM_BYTES: number = 16 * 1024 * 1024;
const MAX_FINISH_PREDICTION_SUMMARY_BYTES: number = 64 * 1024;
const FINISH_PREDICTION_TIMEOUT_MS: number = 15_000;
const RACE_ENTITY_TIMEOUT_MS: number = 50_000;
const MAX_RACE_NUMBER: number = 18;
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
const PADDOCK_ACTION_SCORE: string = "score";
const PADDOCK_ACTION_OFFICIAL_RANK: string = "official-rank";
const PADDOCK_METRICS: readonly string[] = ["attention", "kaeshi", "paddock", "preference"];
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
    description: "jra or nar race source.",
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
    description:
      "GET paddock evaluation state for a race (horse scores, official ranks, history). Same JSON the paddock page loads.",
    inputSchema: {
      additionalProperties: false,
      properties: RACE_ROUTE_PROPERTIES,
      required: ["year", "month", "day", "keibajoCode", "raceNumber"],
      type: "object",
    },
    name: "get_paddock_state",
  },
  {
    description:
      "POST a paddock evaluation update for one horse. actionType score increments or decrements paddock, kaeshi, attention, or preference. actionType official-rank sets or clears the 1-10 official paddock rank. Same handler as the paddock page.",
    inputSchema: {
      additionalProperties: false,
      properties: {
        ...RACE_ROUTE_PROPERTIES,
        actionType: {
          description: "score for metric counts, official-rank for the 1-10 official rank.",
          enum: [PADDOCK_ACTION_OFFICIAL_RANK, PADDOCK_ACTION_SCORE],
          type: "string",
        },
        category: {
          description: "Score metric. Required when actionType is score.",
          enum: PADDOCK_METRICS,
          type: "string",
        },
        delta: {
          description: "Score step. Required when actionType is score.",
          enum: [-1, 1],
          type: "integer",
        },
        horseName: {
          description: "Horse name shown on the paddock page.",
          minLength: 1,
          type: "string",
        },
        horseNumber: STRING_ARG("Horse number, one or two digits.", "^\\d{1,2}$"),
        rank: {
          description:
            "Official paddock rank 1-10, or null to clear. Required when actionType is official-rank.",
          maximum: 10,
          minimum: 1,
          type: ["integer", "null"],
        },
        userId: {
          description:
            "Optional editor id recorded on score history (1-128 letters, digits, _ or -).",
          minLength: 1,
          type: "string",
        },
      },
      required: [
        "year",
        "month",
        "day",
        "keibajoCode",
        "raceNumber",
        "horseNumber",
        "horseName",
        "actionType",
      ],
      type: "object",
    },
    name: "update_paddock_state",
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
      "Fetch a compact finish-position prediction summary for LLM use. Omits inputs.results and other UI-only history, joins current runner names by normalized horse number, and ranks lower predictedFinishNorm first.",
    inputSchema: {
      additionalProperties: false,
      properties: RACE_ROUTE_PROPERTIES,
      required: ["year", "month", "day", "keibajoCode", "raceNumber", "source"],
      type: "object",
    },
    name: "get_finish_prediction_summary",
  },
  {
    description:
      "Fetch bounded, point-in-time recent results for the selected runner's horse, current jockey, current trainer, or current owner. Uses canonical IDs and opaque cursor pagination backed by R2 Catalog.",
    inputSchema: {
      additionalProperties: false,
      properties: {
        ...RACE_ROUTE_PROPERTIES,
        cursor: {
          description: "Opaque nextCursor from the preceding page, or null for the first page.",
          type: ["string", "null"],
        },
        entityType: {
          description: "Entity resolved from the selected target-race runner.",
          enum: ["horse", "jockey", "trainer", "owner"],
          type: "string",
        },
        horseNumber: STRING_ARG("Target-race horse number, one or two digits.", "^\\d{1,2}$"),
        limit: {
          description:
            "Page size. The schema maximum is 30; horse is restricted to 20 by the backend, while jockey, trainer, and owner allow 30.",
          maximum: 30,
          minimum: 1,
          type: "integer",
        },
      },
      required: [
        "year",
        "month",
        "day",
        "keibajoCode",
        "raceNumber",
        "source",
        "horseNumber",
        "entityType",
      ],
      type: "object",
    },
    name: "get_race_entity_recent_results",
  },
  {
    description:
      "Build the win-rate heatmap display model with the same buildWinRateHeatmapDisplay function the on-screen table uses. Defaults match first paint (勝率, レース数 off).",
    inputSchema: {
      additionalProperties: false,
      properties: {
        ...RACE_ROUTE_PROPERTIES,
        showStarts: {
          description:
            "When true, show (n) start counts on heatmap cells. Tooltips always include start counts.",
          type: "boolean",
        },
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

const finishPredictionErrorResult = (error: FinishPredictionSummaryError): McpToolResult => ({
  content: [{ text: JSON.stringify({ error }), type: "text" }],
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

const parseFinishPredictionSummaryRoute = (
  args: Record<string, unknown>,
): FinishPredictionRouteParseResult => {
  const source = readString(args, "source");
  if (source !== SOURCE_JRA && source !== SOURCE_NAR) {
    return {
      error: createFinishPredictionSummaryError(
        "INVALID_SOURCE",
        "source must be either jra or nar.",
      ),
      status: "error",
    };
  }
  const parsed = parseRaceRoute(args);
  if (typeof parsed === "string") {
    const code = parsed.startsWith("keibajoCode")
      ? "INVALID_VENUE_CODE"
      : parsed.startsWith("raceNumber")
        ? "INVALID_RACE_NUMBER"
        : "INVALID_ARGUMENT";
    return {
      error: createFinishPredictionSummaryError(code, parsed),
      status: "error",
    };
  }
  const inferredSource = inferRaceSourceFromKeibajoCode(parsed.keibajoCode);
  if (!Object.hasOwn(KEIBAJO_NAMES, parsed.keibajoCode) || inferredSource !== source) {
    return {
      error: createFinishPredictionSummaryError(
        "INVALID_VENUE_CODE",
        `keibajoCode ${parsed.keibajoCode} is not valid for source ${source}.`,
      ),
      status: "error",
    };
  }
  const raceNumber = Number(parsed.raceNumber);
  if (!Number.isSafeInteger(raceNumber) || raceNumber < 1 || raceNumber > MAX_RACE_NUMBER) {
    return {
      error: createFinishPredictionSummaryError(
        "INVALID_RACE_NUMBER",
        `raceNumber must be between 01 and ${MAX_RACE_NUMBER}.`,
      ),
      status: "error",
    };
  }
  return {
    route: {
      day: parsed.day,
      keibajoCode: parsed.keibajoCode,
      month: parsed.month,
      raceNumber: parsed.raceNumber,
      source,
      year: parsed.year,
    },
    status: "ok",
  };
};

const readResponseBodyChunks = async (
  reader: ReadableStreamDefaultReader<Uint8Array>,
  decoder: TextDecoder,
  chunks: readonly string[],
  byteLength: number,
): Promise<ReadBoundedResponseResult> => {
  const next = await reader.read();
  if (next.done) {
    return {
      byteLength,
      status: "ok",
      text: [...chunks, decoder.decode()].join(""),
    };
  }
  const nextByteLength = byteLength + next.value.byteLength;
  if (nextByteLength > MAX_FINISH_PREDICTION_UPSTREAM_BYTES) {
    await reader.cancel();
    return { status: "too-large" };
  }
  return readResponseBodyChunks(
    reader,
    decoder,
    [...chunks, decoder.decode(next.value, { stream: true })],
    nextByteLength,
  );
};

const readBoundedResponseText = async (response: Response): Promise<ReadBoundedResponseResult> => {
  const contentLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > MAX_FINISH_PREDICTION_UPSTREAM_BYTES) {
    await response.body?.cancel();
    return { status: "too-large" };
  }
  if (response.body === null) {
    return { byteLength: 0, status: "ok", text: "" };
  }
  return readResponseBodyChunks(response.body.getReader(), new TextDecoder(), [], 0);
};

const isTimeoutError = (error: unknown): boolean => {
  if (!(error instanceof Error)) {
    return false;
  }
  return (
    error.name === "AbortError" ||
    error.name === "TimeoutError" ||
    error.message.toLowerCase().includes("timed out") ||
    error.message.toLowerCase().includes("timeout")
  );
};

const finishPredictionUpstreamError = (status: number): FinishPredictionSummaryError => {
  if (status === 404) {
    return createFinishPredictionSummaryError(
      "RACE_NOT_FOUND",
      "The requested race was not found.",
    );
  }
  if (status === 408 || status === 504) {
    return createFinishPredictionSummaryError("TIMEOUT", "The finish prediction API timed out.");
  }
  if (status === 413) {
    return createFinishPredictionSummaryError(
      "RESPONSE_TOO_LARGE",
      "The finish prediction API response is too large to process safely.",
    );
  }
  return createFinishPredictionSummaryError(
    "UPSTREAM_API_ERROR",
    `The finish prediction API failed with status ${status}.`,
  );
};

const getFinishPredictionSummary = async (
  args: Record<string, unknown>,
  fetchSite: McpSiteFetch,
): Promise<McpToolResult> => {
  const parsed = parseFinishPredictionSummaryRoute(args);
  if (parsed.status === "error") {
    return finishPredictionErrorResult(parsed.error);
  }
  const path = raceApiPath(parsed.route, "sections/finish-prediction");
  const allowed = resolveMcpApiPath(path);
  if (allowed === null) {
    return finishPredictionErrorResult(
      createFinishPredictionSummaryError(
        "UPSTREAM_API_ERROR",
        "The finish prediction API path is not allowlisted for MCP reads.",
      ),
    );
  }
  try {
    const response = await fetchSite(allowed, {
      signal: AbortSignal.timeout(FINISH_PREDICTION_TIMEOUT_MS),
    });
    if (!response.ok) {
      await response.body?.cancel();
      return finishPredictionErrorResult(finishPredictionUpstreamError(response.status));
    }
    const body = await readBoundedResponseText(response);
    if (body.status === "too-large") {
      return finishPredictionErrorResult(
        createFinishPredictionSummaryError(
          "RESPONSE_TOO_LARGE",
          "The finish prediction API response is too large to process safely.",
        ),
      );
    }
    try {
      const payload: unknown = JSON.parse(body.text);
      const built = buildFinishPredictionSummary(payload, parsed.route);
      if (built.status === "error") {
        return finishPredictionErrorResult(built.error);
      }
      const summaryText = JSON.stringify(built.summary);
      const summaryBytes = new TextEncoder().encode(summaryText).byteLength;
      return summaryBytes <= MAX_FINISH_PREDICTION_SUMMARY_BYTES
        ? { content: [{ text: summaryText, type: "text" }], isError: false }
        : finishPredictionErrorResult(
            createFinishPredictionSummaryError(
              "RESPONSE_TOO_LARGE",
              "The compact finish prediction summary exceeds the MCP response size limit.",
            ),
          );
    } catch {
      return finishPredictionErrorResult(
        createFinishPredictionSummaryError(
          "PREDICTION_PAYLOAD_MALFORMED",
          "The finish prediction API returned invalid JSON.",
        ),
      );
    }
  } catch (error) {
    return finishPredictionErrorResult(
      isTimeoutError(error)
        ? createFinishPredictionSummaryError("TIMEOUT", "The finish prediction API timed out.")
        : createFinishPredictionSummaryError(
            "UPSTREAM_API_ERROR",
            "The finish prediction API request failed.",
          ),
    );
  }
};

const getRaceEntityRecentResults = async (
  args: Record<string, unknown>,
  fetchSite: McpSiteFetch,
): Promise<McpToolResult> => {
  const parsed = parseRaceRoute(args);
  if (typeof parsed === "string") {
    return errorResult(JSON.stringify({ error: { code: "RACE_NOT_FOUND", message: parsed } }));
  }
  const horseNumber = readString(args, "horseNumber");
  const entityType = readString(args, "entityType");
  if (horseNumber === null || !/^\d{1,2}$/u.test(horseNumber)) {
    return errorResult(
      JSON.stringify({
        error: { code: "RUNNER_NOT_FOUND", message: "horseNumber must contain one or two digits." },
      }),
    );
  }
  if (
    entityType !== "horse" &&
    entityType !== "jockey" &&
    entityType !== "trainer" &&
    entityType !== "owner"
  ) {
    return errorResult(
      JSON.stringify({
        error: {
          code: "INVALID_ENTITY_TYPE",
          message: "entityType must be horse, jockey, trainer, or owner.",
        },
      }),
    );
  }
  const limit = args.limit;
  if (limit !== undefined && (typeof limit !== "number" || !Number.isInteger(limit) || limit < 1)) {
    return errorResult(
      JSON.stringify({
        error: { code: "INVALID_LIMIT", message: "limit must be a positive integer." },
      }),
    );
  }
  const cursor = args.cursor;
  if (cursor !== undefined && cursor !== null && typeof cursor !== "string") {
    return errorResult(
      JSON.stringify({
        error: { code: "INVALID_CURSOR", message: "cursor must be a string or null." },
      }),
    );
  }
  const query = new URLSearchParams({
    entityType,
    horseNumber,
    source: parsed.source ?? "",
  });
  if (limit !== undefined) query.set("limit", String(limit));
  if (typeof cursor === "string") query.set("cursor", cursor);
  const path = raceApiPath(parsed, "entity-recent-results").split("?")[0];
  try {
    const response = await fetchSite(`${path}?${query.toString()}`, {
      signal: AbortSignal.timeout(RACE_ENTITY_TIMEOUT_MS),
    });
    const text = await response.text();
    try {
      const value: unknown = JSON.parse(text);
      return response.ok
        ? okJson(value)
        : { content: [{ text: JSON.stringify(value), type: "text" }], isError: true };
    } catch {
      return errorResult(
        JSON.stringify({
          error: {
            code: "MALFORMED_HISTORY_DATA",
            message: "The history API returned invalid JSON.",
          },
        }),
      );
    }
  } catch (error) {
    return errorResult(
      JSON.stringify({
        error: {
          code: isTimeoutError(error) ? "TIMEOUT" : "UPSTREAM_ERROR",
          message: isTimeoutError(error)
            ? "The history request timed out."
            : "The history request failed.",
        },
      }),
    );
  }
};

const parseJsonResponse = async (
  response: Response,
): Promise<{ ok: boolean; status: number; value: unknown }> => {
  const text = await response.text();
  try {
    return { ok: response.ok, status: response.status, value: JSON.parse(text) };
  } catch {
    return { ok: response.ok, status: response.status, value: text };
  }
};

const fetchSiteJson = async (
  fetchSite: McpSiteFetch,
  pathWithQuery: string,
): Promise<{ ok: boolean; status: number; value: unknown }> => {
  const allowed = resolveMcpApiPath(pathWithQuery);
  if (allowed === null) {
    return { ok: false, status: 403, value: "Path is not allowlisted for MCP reads" };
  }
  return parseJsonResponse(await fetchSite(allowed));
};

const postSiteJson = async (
  input: FetchSiteJsonWriteParams,
): Promise<{ ok: boolean; status: number; value: unknown }> => {
  const allowed = resolveMcpPaddockWritePath(input.pathWithQuery);
  if (allowed === null) {
    return { ok: false, status: 403, value: "Path is not allowlisted for MCP writes" };
  }
  const response = await input.fetchSite(allowed, { body: input.body, method: "POST" });
  return parseJsonResponse(response);
};

const buildPaddockAction = (args: Record<string, unknown>): PaddockAction | string => {
  const actionType = readString(args, "actionType");
  if (actionType === PADDOCK_ACTION_OFFICIAL_RANK) {
    const candidate = {
      horseName: args.horseName,
      horseNumber: args.horseNumber,
      rank: args.rank,
      type: PADDOCK_ACTION_OFFICIAL_RANK,
    };
    return isPaddockAction(candidate)
      ? candidate
      : "official-rank requires horseName, horseNumber, and rank 1-10 or null";
  }
  if (actionType !== PADDOCK_ACTION_SCORE) {
    return "actionType must be score or official-rank";
  }
  const userId = args.userId;
  const candidate =
    typeof userId === "string"
      ? {
          category: args.category,
          delta: args.delta,
          horseName: args.horseName,
          horseNumber: args.horseNumber,
          userId,
        }
      : {
          category: args.category,
          delta: args.delta,
          horseName: args.horseName,
          horseNumber: args.horseNumber,
        };
  return isPaddockAction(candidate)
    ? candidate
    : "score requires horseName, horseNumber, category, and delta 1 or -1";
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
  if (name === "get_paddock_state") {
    const parsed = parseRaceRoute(args);
    if (typeof parsed === "string") {
      return errorResult(parsed);
    }
    const fetched = await fetchSiteJson(fetchSite, raceApiPath(parsed, "paddock"));
    if (!fetched.ok) {
      return {
        content: [{ text: JSON.stringify(fetched.value), type: "text" }],
        isError: true,
      };
    }
    return okJson(fetched.value);
  }
  if (name === "update_paddock_state") {
    const parsed = parseRaceRoute(args);
    if (typeof parsed === "string") {
      return errorResult(parsed);
    }
    const action = buildPaddockAction(args);
    if (typeof action === "string") {
      return errorResult(action);
    }
    const fetched = await postSiteJson({
      body: JSON.stringify(action),
      fetchSite,
      pathWithQuery: raceApiPath(parsed, "paddock"),
    });
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
  if (name === "get_finish_prediction_summary") {
    return getFinishPredictionSummary(args, fetchSite);
  }
  if (name === "get_race_entity_recent_results") {
    return getRaceEntityRecentResults(args, fetchSite);
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
