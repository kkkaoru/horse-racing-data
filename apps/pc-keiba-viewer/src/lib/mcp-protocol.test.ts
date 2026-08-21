// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { handleMcpPostBody, parseJsonRpcRequest } from "./mcp-protocol";

const okFetch = async (): Promise<Response> =>
  new Response("{}", { headers: { "content-type": "application/json" }, status: 200 });

it("parseJsonRpcRequest rejects invalid envelopes", () => {
  expect(parseJsonRpcRequest("x")).toBe("JSON-RPC body must be an object");
  expect(parseJsonRpcRequest({ jsonrpc: "1.0", method: "ping" })).toBe("jsonrpc must be 2.0");
  expect(parseJsonRpcRequest({ jsonrpc: "2.0", method: "" })).toBe(
    "method must be a non-empty string",
  );
  expect(parseJsonRpcRequest({ id: true, jsonrpc: "2.0", method: "ping" })).toBe(
    "id must be string, number, or null",
  );
});

it("handleMcpPostBody returns parse errors and rejects batches", async () => {
  const empty = await handleMcpPostBody("", okFetch);
  expect(JSON.parse(empty.body ?? "{}")).toStrictEqual({
    error: { code: -32700, message: "Parse error" },
    id: null,
    jsonrpc: "2.0",
  });
  const invalid = await handleMcpPostBody("{", okFetch);
  expect(JSON.parse(invalid.body ?? "{}")).toStrictEqual({
    error: { code: -32700, message: "Parse error" },
    id: null,
    jsonrpc: "2.0",
  });
  const batch = await handleMcpPostBody("[]", okFetch);
  expect(JSON.parse(batch.body ?? "{}")).toStrictEqual({
    error: { code: -32600, message: "Batched JSON-RPC is not supported" },
    id: null,
    jsonrpc: "2.0",
  });
});

it("initialize, ping, and unknown methods work", async () => {
  const initialized = await handleMcpPostBody(
    JSON.stringify({
      id: 1,
      jsonrpc: "2.0",
      method: "initialize",
      params: { protocolVersion: "2025-06-18" },
    }),
    okFetch,
  );
  expect(JSON.parse(initialized.body ?? "{}")).toStrictEqual({
    id: 1,
    jsonrpc: "2.0",
    result: {
      capabilities: { tools: {} },
      protocolVersion: "2025-06-18",
      serverInfo: { name: "pc-keiba-viewer", version: "1.0.0" },
    },
  });
  const ping = await handleMcpPostBody('{"id":0,"jsonrpc":"2.0","method":"ping"}', okFetch);
  expect(JSON.parse(ping.body ?? "{}")).toStrictEqual({ id: 0, jsonrpc: "2.0", result: {} });
  const unknown = await handleMcpPostBody('{"id":5,"jsonrpc":"2.0","method":"boom"}', okFetch);
  expect(JSON.parse(unknown.body ?? "{}")).toStrictEqual({
    error: { code: -32601, message: "Method not found: boom" },
    id: 5,
    jsonrpc: "2.0",
  });
});

it("initialize defaults an unknown protocol version", async () => {
  const result = await handleMcpPostBody(
    JSON.stringify({
      id: 1,
      jsonrpc: "2.0",
      method: "initialize",
      params: { protocolVersion: "1999-01-01" },
    }),
    okFetch,
  );
  expect(JSON.parse(result.body ?? "{}")).toStrictEqual({
    id: 1,
    jsonrpc: "2.0",
    result: {
      capabilities: { tools: {} },
      protocolVersion: "2025-03-26",
      serverInfo: { name: "pc-keiba-viewer", version: "1.0.0" },
    },
  });
});

it("tools/call requires a name", async () => {
  const missing = await handleMcpPostBody(
    '{"id":3,"jsonrpc":"2.0","method":"tools/call","params":{}}',
    okFetch,
  );
  expect(JSON.parse(missing.body ?? "{}")).toStrictEqual({
    error: { code: -32602, message: "tools/call requires params.name" },
    id: 3,
    jsonrpc: "2.0",
  });
});

it("maps fetch failures to an internal error", async () => {
  const result = await handleMcpPostBody(
    JSON.stringify({
      id: 6,
      jsonrpc: "2.0",
      method: "tools/call",
      params: { arguments: {}, name: "get_api_spec" },
    }),
    async () => {
      throw new Error("network down");
    },
  );
  expect(JSON.parse(result.body ?? "{}")).toStrictEqual({
    error: { code: -32603, message: "Internal error" },
    id: 6,
    jsonrpc: "2.0",
  });
});
