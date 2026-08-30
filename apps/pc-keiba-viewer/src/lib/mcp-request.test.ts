// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { signHs256Jwt } from "./mcp-oauth-jwt";
import { handlePcKeibaMcpRequest, readMcpAuthToken, readMcpOauthSigningKey } from "./mcp-request";

const TOKEN: string = "mcp-token";
const NOW_SECONDS: number = 1_700_000_000;
const SIGNING_KEY: string = "oauth-signing-key-for-tests";

const fetchSite = async (): Promise<Response> =>
  new Response("{}", { headers: { "content-type": "application/json" }, status: 200 });

const handle = (input: {
  fetchSite: typeof fetchSite;
  mcpAuthToken: string;
  request: Request;
}): ReturnType<typeof handlePcKeibaMcpRequest> =>
  handlePcKeibaMcpRequest({
    fetchSite: input.fetchSite,
    mcpAuthToken: input.mcpAuthToken,
    nowSeconds: NOW_SECONDS,
    oauthSigningKey: SIGNING_KEY,
    request: input.request,
  });

it("ignores non-MCP paths so Access-protected pages keep their existing fetch path", async () => {
  const response = await handle({
    fetchSite,
    mcpAuthToken: TOKEN,
    request: new Request("https://viewer.example.test/races/2026/08/20"),
  });
  expect(response).toBe(null);
});

it("answers OPTIONS on /mcp without a bearer token", async () => {
  const response = await handle({
    fetchSite,
    mcpAuthToken: TOKEN,
    request: new Request("https://viewer.example.test/mcp", { method: "OPTIONS" }),
  });
  expect(response?.status).toBe(204);
  expect(response?.headers.get("Access-Control-Allow-Origin")).toBe("*");
  expect(response?.headers.get("Access-Control-Allow-Methods")).toBe("GET, POST, OPTIONS");
  expect(response?.headers.get("Access-Control-Allow-Headers")).toBe(
    "Authorization, Content-Type, Accept, MCP-Protocol-Version, Mcp-Method, Mcp-Name, CF-Access-Client-Id, CF-Access-Client-Secret",
  );
});

it("rejects /mcp without a bearer token even when Access already passed", async () => {
  const response = await handle({
    fetchSite,
    mcpAuthToken: TOKEN,
    request: new Request("https://viewer.example.test/mcp", { method: "POST", body: "{}" }),
  });
  expect(response?.status).toBe(401);
  expect(await response?.json()).toStrictEqual({ error: { message: "Unauthorized" } });
});

it("rejects GET /mcp after bearer auth", async () => {
  const response = await handle({
    fetchSite,
    mcpAuthToken: TOKEN,
    request: new Request("https://viewer.example.test/mcp", {
      headers: { Authorization: "Bearer mcp-token" },
    }),
  });
  expect(response?.status).toBe(405);
  expect(await response?.json()).toStrictEqual({ error: { message: "Method Not Allowed" } });
});

it("rejects PUT /mcp after bearer auth", async () => {
  const response = await handle({
    fetchSite,
    mcpAuthToken: TOKEN,
    request: new Request("https://viewer.example.test/mcp", {
      headers: { Authorization: "Bearer mcp-token" },
      method: "PUT",
    }),
  });
  expect(response?.status).toBe(405);
  expect(await response?.json()).toStrictEqual({ error: { message: "Method Not Allowed" } });
});

it("handles tools/list over POST /mcp with bearer auth", async () => {
  const response = await handle({
    fetchSite,
    mcpAuthToken: TOKEN,
    request: new Request("https://viewer.example.test/mcp", {
      body: JSON.stringify({ id: 1, jsonrpc: "2.0", method: "tools/list" }),
      headers: { Authorization: "Bearer mcp-token", "Content-Type": "application/json" },
      method: "POST",
    }),
  });
  expect(response?.status).toBe(200);
});

it("returns 202 for MCP notifications", async () => {
  const response = await handle({
    fetchSite,
    mcpAuthToken: TOKEN,
    request: new Request("https://viewer.example.test/mcp", {
      body: JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }),
      headers: { Authorization: "Bearer mcp-token" },
      method: "POST",
    }),
  });
  expect(response?.status).toBe(202);
});

it("readMcpAuthToken reads a string secret and ignores other shapes", () => {
  expect(readMcpAuthToken({})).toBe("");
  expect(readMcpAuthToken({ MCP_AUTH_TOKEN: "secret" })).toBe("secret");
  expect(readMcpAuthToken({ MCP_AUTH_TOKEN: undefined })).toBe("");
  const numericTokenEnv: CloudflareEnv = {};
  Object.defineProperty(numericTokenEnv, "MCP_AUTH_TOKEN", { value: 1 });
  expect(readMcpAuthToken(numericTokenEnv)).toBe("");
});

it("readMcpOauthSigningKey reads a string secret and ignores other shapes", () => {
  expect(readMcpOauthSigningKey({})).toBe("");
  expect(readMcpOauthSigningKey({ MCP_OAUTH_SIGNING_KEY: "signing" })).toBe("signing");
  expect(readMcpOauthSigningKey({ MCP_OAUTH_SIGNING_KEY: undefined })).toBe("");
  const numericKeyEnv: CloudflareEnv = {};
  Object.defineProperty(numericKeyEnv, "MCP_OAUTH_SIGNING_KEY", { value: 1 });
  expect(readMcpOauthSigningKey(numericKeyEnv)).toBe("");
});

it("authorizes /mcp with a static MCP_AUTH_TOKEN while an OAuth signing key is also configured", async () => {
  const response = await handle({
    fetchSite,
    mcpAuthToken: TOKEN,
    request: new Request("https://viewer.example.test/mcp", {
      body: JSON.stringify({ id: 1, jsonrpc: "2.0", method: "tools/list" }),
      headers: { Authorization: "Bearer mcp-token", "Content-Type": "application/json" },
      method: "POST",
    }),
  });
  expect(response?.status).toBe(200);
});

it("authorizes /mcp with an OAuth JWT when the static token is empty", async () => {
  const jwt = await signHs256Jwt(
    {
      aud: "https://viewer.example.test/mcp",
      client_id: "client-1",
      exp: NOW_SECONDS + 3_600,
      iat: NOW_SECONDS,
      iss: "https://viewer.example.test",
      scope: "mcp",
      sub: "user@example.test",
    },
    SIGNING_KEY,
  );
  const response = await handlePcKeibaMcpRequest({
    fetchSite,
    mcpAuthToken: "",
    nowSeconds: NOW_SECONDS,
    oauthSigningKey: SIGNING_KEY,
    request: new Request("https://viewer.example.test/mcp", {
      body: JSON.stringify({ id: 2, jsonrpc: "2.0", method: "tools/list" }),
      headers: { Authorization: `Bearer ${jwt}`, "Content-Type": "application/json" },
      method: "POST",
    }),
  });
  expect(response?.status).toBe(200);
});

it("authorizes /mcp with an OAuth JWT when the static token does not match", async () => {
  const jwt = await signHs256Jwt(
    {
      aud: "https://viewer.example.test/mcp",
      client_id: "client-1",
      exp: NOW_SECONDS + 3_600,
      iat: NOW_SECONDS,
      iss: "https://viewer.example.test",
      scope: "mcp",
      sub: "user@example.test",
    },
    SIGNING_KEY,
  );
  const response = await handlePcKeibaMcpRequest({
    fetchSite,
    mcpAuthToken: TOKEN,
    nowSeconds: NOW_SECONDS,
    oauthSigningKey: SIGNING_KEY,
    request: new Request("https://viewer.example.test/mcp", {
      body: JSON.stringify({ id: 3, jsonrpc: "2.0", method: "tools/list" }),
      headers: { Authorization: `Bearer ${jwt}`, "Content-Type": "application/json" },
      method: "POST",
    }),
  });
  expect(response?.status).toBe(200);
});

it("rejects /mcp when the OAuth signing key is blank and the static token does not match", async () => {
  const response = await handlePcKeibaMcpRequest({
    fetchSite,
    mcpAuthToken: TOKEN,
    nowSeconds: NOW_SECONDS,
    oauthSigningKey: "   ",
    request: new Request("https://viewer.example.test/mcp", {
      body: "{}",
      headers: { Authorization: "Bearer other-token" },
      method: "POST",
    }),
  });
  expect(response?.status).toBe(401);
});

it("rejects GET /mcp after OAuth JWT auth", async () => {
  const jwt = await signHs256Jwt(
    {
      aud: "https://viewer.example.test/mcp",
      client_id: "client-1",
      exp: NOW_SECONDS + 3_600,
      iat: NOW_SECONDS,
      iss: "https://viewer.example.test",
      scope: "mcp",
      sub: "user@example.test",
    },
    SIGNING_KEY,
  );
  const response = await handlePcKeibaMcpRequest({
    fetchSite,
    mcpAuthToken: "",
    nowSeconds: NOW_SECONDS,
    oauthSigningKey: SIGNING_KEY,
    request: new Request("https://viewer.example.test/mcp", {
      headers: { Authorization: `Bearer ${jwt}` },
    }),
  });
  expect(response?.status).toBe(405);
});
