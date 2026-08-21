// Run with: bun run test
import { expect, it } from "vitest";

import {
  encodeStdioMessage,
  extractStdioMessages,
  forwardMcpJsonRpc,
  loadMcpProxyConfig,
  type McpProxyConfig,
} from "./mcp-stdio-proxy";

const CONFIG: McpProxyConfig = {
  accessClientId: "access-id",
  accessClientSecret: "access-secret",
  mcpAuthToken: "mcp-token",
  mcpUrl: "https://viewer.example.test/mcp",
};

it("loadMcpProxyConfig requires url, bearer, and Access credentials", () => {
  expect(loadMcpProxyConfig({})).toBe(
    "PC_KEIBA_VIEWER_MCP_URL is required (absolute https URL ending with /mcp)",
  );
  expect(loadMcpProxyConfig({ PC_KEIBA_VIEWER_MCP_URL: "https://viewer.example.test/mcp" })).toBe(
    "MCP_AUTH_TOKEN is required",
  );
  expect(
    loadMcpProxyConfig({
      MCP_AUTH_TOKEN: "mcp-token",
      PC_KEIBA_VIEWER_MCP_URL: "https://viewer.example.test/mcp",
    }),
  ).toBe("PC_KEIBA_ACCESS_CLIENT_ID is required");
  expect(
    loadMcpProxyConfig({
      MCP_AUTH_TOKEN: "mcp-token",
      PC_KEIBA_ACCESS_CLIENT_ID: "access-id",
      PC_KEIBA_VIEWER_MCP_URL: "https://viewer.example.test/mcp",
    }),
  ).toBe("PC_KEIBA_ACCESS_CLIENT_SECRET is required");
});

it("loadMcpProxyConfig rejects non-https, userinfo, fragments, and non-/mcp paths", () => {
  const base = {
    MCP_AUTH_TOKEN: "mcp-token",
    PC_KEIBA_ACCESS_CLIENT_ID: "access-id",
    PC_KEIBA_ACCESS_CLIENT_SECRET: "access-secret",
  };
  expect(loadMcpProxyConfig({ ...base, PC_KEIBA_VIEWER_MCP_URL: "not a url" })).toBe(
    "PC_KEIBA_VIEWER_MCP_URL is not a valid URL",
  );
  expect(
    loadMcpProxyConfig({ ...base, PC_KEIBA_VIEWER_MCP_URL: "http://viewer.example.test/mcp" }),
  ).toBe("PC_KEIBA_VIEWER_MCP_URL must be https");
  expect(
    loadMcpProxyConfig({
      ...base,
      PC_KEIBA_VIEWER_MCP_URL: "https://user:pass@viewer.example.test/mcp",
    }),
  ).toBe("PC_KEIBA_VIEWER_MCP_URL must not include userinfo");
  expect(
    loadMcpProxyConfig({ ...base, PC_KEIBA_VIEWER_MCP_URL: "https://viewer.example.test/mcp#x" }),
  ).toBe("PC_KEIBA_VIEWER_MCP_URL must not include a fragment");
  expect(
    loadMcpProxyConfig({ ...base, PC_KEIBA_VIEWER_MCP_URL: "https://viewer.example.test/api" }),
  ).toBe("PC_KEIBA_VIEWER_MCP_URL path must end with /mcp");
});

it("loadMcpProxyConfig accepts a https /mcp URL", () => {
  expect(
    loadMcpProxyConfig({
      MCP_AUTH_TOKEN: "mcp-token",
      PC_KEIBA_ACCESS_CLIENT_ID: "access-id",
      PC_KEIBA_ACCESS_CLIENT_SECRET: "access-secret",
      PC_KEIBA_VIEWER_MCP_URL: "https://viewer.example.test/mcp",
    }),
  ).toStrictEqual(CONFIG);
});

it("extractStdioMessages reads newline-delimited JSON", () => {
  expect(extractStdioMessages('\n{"id":1}\n\n{"id":2}\n')).toStrictEqual({
    messages: ['{"id":1}', '{"id":2}'],
    rest: "",
  });
  expect(extractStdioMessages('{"id":1}\n{"id":2}\n')).toStrictEqual({
    messages: ['{"id":1}', '{"id":2}'],
    rest: "",
  });
  expect(extractStdioMessages('{"id":1}\n{"id":')).toStrictEqual({
    messages: ['{"id":1}'],
    rest: '{"id":',
  });
});

it("extractStdioMessages reads Content-Length framed messages", () => {
  const body = '{"id":1}';
  const framed = encodeStdioMessage(body);
  expect(extractStdioMessages(framed)).toStrictEqual({
    messages: [body],
    rest: "",
  });
  expect(extractStdioMessages(framed.slice(0, 10))).toStrictEqual({
    messages: [],
    rest: framed.slice(0, 10),
  });
});

it("extractStdioMessages skips an invalid Content-Length header", () => {
  expect(extractStdioMessages("Content-Length: no\r\n\r\n")).toStrictEqual({
    messages: [],
    rest: "",
  });
});

it("extractStdioMessages waits for a complete Content-Length header", () => {
  expect(extractStdioMessages("Content-Length: 4")).toStrictEqual({
    messages: [],
    rest: "Content-Length: 4",
  });
});

it("extractStdioMessages waits for a complete Content-Length body", () => {
  expect(extractStdioMessages('Content-Length: 50\r\n\r\n{"id":1}')).toStrictEqual({
    messages: [],
    rest: 'Content-Length: 50\r\n\r\n{"id":1}',
  });
});

it("forwardMcpJsonRpc posts Access and Bearer headers", async () => {
  const reply = await forwardMcpJsonRpc(
    CONFIG,
    '{"id":1,"jsonrpc":"2.0","method":"ping"}',
    async (input, init) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      expect(url).toBe("https://viewer.example.test/mcp");
      const headers = new Headers(init?.headers);
      expect(headers.get("Authorization")).toBe("Bearer mcp-token");
      expect(headers.get("CF-Access-Client-Id")).toBe("access-id");
      expect(headers.get("CF-Access-Client-Secret")).toBe("access-secret");
      return new Response('{"id":1,"jsonrpc":"2.0","result":{}}', { status: 200 });
    },
  );
  expect(reply).toBe('{"id":1,"jsonrpc":"2.0","result":{}}');
});

it("forwardMcpJsonRpc returns null for 202 and empty bodies", async () => {
  const accepted = await forwardMcpJsonRpc(
    CONFIG,
    "{}",
    async () => new Response(null, { status: 202 }),
  );
  expect(accepted).toBe(null);
  const empty = await forwardMcpJsonRpc(
    CONFIG,
    "{}",
    async () => new Response("  ", { status: 200 }),
  );
  expect(empty).toBe(null);
});

it("forwardMcpJsonRpc maps HTTP errors to JSON-RPC errors", async () => {
  const reply = await forwardMcpJsonRpc(
    CONFIG,
    "{}",
    async () => new Response("nope", { status: 401 }),
  );
  expect(JSON.parse(reply ?? "{}")).toStrictEqual({
    error: { code: -32000, message: "Remote MCP HTTP 401" },
    id: null,
    jsonrpc: "2.0",
  });
});
