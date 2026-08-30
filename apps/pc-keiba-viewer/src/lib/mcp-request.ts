// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
// Human pages stay behind Cloudflare Access. /mcp accepts either the static
// Worker secret MCP_AUTH_TOKEN or an OAuth access token issued after consent.

import { isMcpAuthorized, mcpUnauthorizedResponse, parseBearerToken } from "./mcp-auth";
import { verifyHs256Jwt } from "./mcp-oauth-jwt";
import { mcpResourceUrl, originFromRequestUrl } from "./mcp-oauth-origin";
import { handleMcpPostBody } from "./mcp-protocol";
import type { McpSiteFetch } from "./mcp-tools";

export const PC_KEIBA_MCP_PATH: string = "/mcp";

const CORS_ALLOW_HEADERS: string =
  "Authorization, Content-Type, Accept, MCP-Protocol-Version, Mcp-Method, Mcp-Name, CF-Access-Client-Id, CF-Access-Client-Secret";
const CORS_ALLOW_METHODS: string = "GET, POST, OPTIONS";
const CORS_ALLOW_ORIGIN: string = "*";
const METHOD_GET: string = "GET";
const METHOD_POST: string = "POST";
const METHOD_OPTIONS: string = "OPTIONS";
const METHOD_NOT_ALLOWED: number = 405;

interface HandlePcKeibaMcpRequestInput {
  fetchSite: McpSiteFetch;
  mcpAuthToken: string;
  nowSeconds: number;
  oauthSigningKey: string;
  request: Request;
}

const corsHeaders = (): Record<string, string> => ({
  "Access-Control-Allow-Headers": CORS_ALLOW_HEADERS,
  "Access-Control-Allow-Methods": CORS_ALLOW_METHODS,
  "Access-Control-Allow-Origin": CORS_ALLOW_ORIGIN,
});

const jsonHeaders = (): Record<string, string> => ({
  ...corsHeaders(),
  "Content-Type": "application/json; charset=utf-8",
});

export const handlePcKeibaMcpRequest = async (
  input: HandlePcKeibaMcpRequestInput,
): Promise<Response | null> => {
  if (new URL(input.request.url).pathname !== PC_KEIBA_MCP_PATH) {
    return null;
  }
  if (input.request.method === METHOD_OPTIONS) {
    return new Response(null, { headers: corsHeaders(), status: 204 });
  }
  const origin = originFromRequestUrl(input.request.url);
  const staticOk = isMcpAuthorized(input.request, input.mcpAuthToken);
  const bearer = parseBearerToken(input.request.headers.get("Authorization"));
  const oauthClaims =
    staticOk || bearer === null || input.oauthSigningKey.trim().length === 0
      ? null
      : await verifyHs256Jwt(
          bearer,
          input.oauthSigningKey,
          origin,
          mcpResourceUrl(origin),
          input.nowSeconds,
        );
  if (!staticOk && oauthClaims === null) {
    return mcpUnauthorizedResponse(origin);
  }
  if (input.request.method === METHOD_GET) {
    return new Response(JSON.stringify({ error: { message: "Method Not Allowed" } }), {
      headers: { ...jsonHeaders(), Allow: METHOD_POST },
      status: METHOD_NOT_ALLOWED,
    });
  }
  if (input.request.method !== METHOD_POST) {
    return new Response(JSON.stringify({ error: { message: "Method Not Allowed" } }), {
      headers: { ...jsonHeaders(), Allow: METHOD_POST },
      status: METHOD_NOT_ALLOWED,
    });
  }
  const result = await handleMcpPostBody(await input.request.text(), input.fetchSite);
  if (result.body === null) {
    return new Response(null, { headers: corsHeaders(), status: result.status });
  }
  return new Response(result.body, { headers: jsonHeaders(), status: result.status });
};

export const readMcpAuthToken = (env: CloudflareEnv): string => {
  if (!("MCP_AUTH_TOKEN" in env)) {
    return "";
  }
  const token = Reflect.get(env, "MCP_AUTH_TOKEN");
  return typeof token === "string" ? token : "";
};

export const readMcpOauthSigningKey = (env: CloudflareEnv): string => {
  if (!("MCP_OAUTH_SIGNING_KEY" in env)) {
    return "";
  }
  const key = Reflect.get(env, "MCP_OAUTH_SIGNING_KEY");
  return typeof key === "string" ? key : "";
};
