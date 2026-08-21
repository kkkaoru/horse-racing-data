// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { MCP_TOOL_DEFINITIONS, callMcpTool, type McpSiteFetch } from "./mcp-tools";

const PROTOCOL_2024: string = "2024-11-05";
const PROTOCOL_2025_03: string = "2025-03-26";
const PROTOCOL_2025_06: string = "2025-06-18";
const PROTOCOL_2026: string = "2026-07-28";
const SUPPORTED_PROTOCOLS: ReadonlySet<string> = new Set([
  PROTOCOL_2024,
  PROTOCOL_2025_03,
  PROTOCOL_2025_06,
  PROTOCOL_2026,
]);
const DEFAULT_PROTOCOL: string = PROTOCOL_2025_03;
const JSONRPC_VERSION: string = "2.0";
const PARSE_ERROR: number = -32700;
const INVALID_REQUEST: number = -32600;
const METHOD_NOT_FOUND: number = -32601;
const INVALID_PARAMS: number = -32602;
const INTERNAL_ERROR: number = -32603;
const SERVER_NAME: string = "pc-keiba-viewer";
const SERVER_VERSION: string = "1.0.0";

interface JsonRpcRequest {
  id: string | number | null;
  jsonrpc: string;
  method: string;
  params: unknown;
}

interface JsonRpcErrorBody {
  error: { code: number; message: string };
  id: string | number | null;
  jsonrpc: string;
}

interface JsonRpcResultBody {
  id: string | number | null;
  jsonrpc: string;
  result: unknown;
}

export type JsonRpcResponseBody = JsonRpcErrorBody | JsonRpcResultBody;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isRpcId = (value: unknown): value is string | number | null =>
  value === null || typeof value === "string" || typeof value === "number";

export const parseJsonRpcRequest = (value: unknown): JsonRpcRequest | string => {
  if (!isRecord(value)) {
    return "JSON-RPC body must be an object";
  }
  if (value.jsonrpc !== JSONRPC_VERSION) {
    return "jsonrpc must be 2.0";
  }
  if (typeof value.method !== "string" || value.method.length === 0) {
    return "method must be a non-empty string";
  }
  if (value.id !== undefined && !isRpcId(value.id)) {
    return "id must be string, number, or null";
  }
  return {
    id: value.id === undefined ? null : value.id,
    jsonrpc: JSONRPC_VERSION,
    method: value.method,
    params: value.params,
  };
};

const errorBody = (
  id: string | number | null,
  code: number,
  message: string,
): JsonRpcErrorBody => ({
  error: { code, message },
  id,
  jsonrpc: JSONRPC_VERSION,
});

const resultBody = (id: string | number | null, result: unknown): JsonRpcResultBody => ({
  id,
  jsonrpc: JSONRPC_VERSION,
  result,
});

const resolveProtocolVersion = (params: unknown): string => {
  if (!isRecord(params)) {
    return DEFAULT_PROTOCOL;
  }
  const requested = params.protocolVersion;
  if (typeof requested !== "string" || !SUPPORTED_PROTOCOLS.has(requested)) {
    return DEFAULT_PROTOCOL;
  }
  return requested;
};

const toolCallName = (params: unknown): string | null => {
  if (!isRecord(params)) {
    return null;
  }
  if (typeof params.name !== "string" || params.name.length === 0) {
    return null;
  }
  return params.name;
};

const toolCallArguments = (params: unknown): unknown => {
  if (!isRecord(params)) {
    return {};
  }
  return params.arguments;
};

export const handleJsonRpc = async (
  request: JsonRpcRequest,
  fetchSite: McpSiteFetch,
): Promise<JsonRpcResponseBody | null> => {
  if (request.id === null && request.method.startsWith("notifications/")) {
    return null;
  }
  if (request.method === "initialize") {
    return resultBody(request.id, {
      capabilities: { tools: {} },
      protocolVersion: resolveProtocolVersion(request.params),
      serverInfo: { name: SERVER_NAME, version: SERVER_VERSION },
    });
  }
  if (request.method === "ping") {
    return resultBody(request.id, {});
  }
  if (request.method === "tools/list") {
    return resultBody(request.id, { tools: MCP_TOOL_DEFINITIONS });
  }
  if (request.method === "tools/call") {
    const name = toolCallName(request.params);
    if (name === null) {
      return errorBody(request.id, INVALID_PARAMS, "tools/call requires params.name");
    }
    const toolResult = await callMcpTool(name, toolCallArguments(request.params), fetchSite);
    return resultBody(request.id, toolResult);
  }
  return errorBody(request.id, METHOD_NOT_FOUND, `Method not found: ${request.method}`);
};

export const handleMcpPostBody = async (
  rawBody: string,
  fetchSite: McpSiteFetch,
): Promise<{ body: string | null; status: number }> => {
  if (rawBody.trim().length === 0) {
    return { body: JSON.stringify(errorBody(null, PARSE_ERROR, "Parse error")), status: 200 };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawBody);
  } catch {
    return { body: JSON.stringify(errorBody(null, PARSE_ERROR, "Parse error")), status: 200 };
  }
  if (Array.isArray(parsed)) {
    return {
      body: JSON.stringify(errorBody(null, INVALID_REQUEST, "Batched JSON-RPC is not supported")),
      status: 200,
    };
  }
  const request = parseJsonRpcRequest(parsed);
  if (typeof request === "string") {
    return {
      body: JSON.stringify(errorBody(null, INVALID_REQUEST, request)),
      status: 200,
    };
  }
  try {
    const response = await handleJsonRpc(request, fetchSite);
    if (response === null) {
      return { body: null, status: 202 };
    }
    return { body: JSON.stringify(response), status: 200 };
  } catch {
    return {
      body: JSON.stringify(errorBody(request.id, INTERNAL_ERROR, "Internal error")),
      status: 200,
    };
  }
};
