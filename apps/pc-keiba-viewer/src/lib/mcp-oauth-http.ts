// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { registerPublicClient, resolveClient } from "./mcp-oauth-clients";
import { renderMcpConsentPage, renderMcpLoginRequiredPage } from "./mcp-oauth-consent-html";
import { readAccessUserSubject } from "./mcp-oauth-identity";
import { signHs256Jwt } from "./mcp-oauth-jwt";
import {
  buildAuthorizationServerMetadata,
  buildProtectedResourceMetadata,
  MCP_OAUTH_SCOPE,
} from "./mcp-oauth-metadata";
import { mcpResourceUrl, normalizeMcpResource, originFromRequestUrl } from "./mcp-oauth-origin";
import { isS256Method, verifyPkceS256 } from "./mcp-oauth-pkce";
import { randomToken } from "./mcp-oauth-random";
import { redirectUrisInclude } from "./mcp-oauth-redirect";
import type { McpOauthStore } from "./mcp-oauth-store";

const CODE_TTL_SECONDS: number = 600;
const ACCESS_TTL_SECONDS: number = 3600;
const REFRESH_TTL_SECONDS: number = 60 * 60 * 24 * 30;
const AUTH_REQUEST_TTL_SECONDS: number = 600;
const JSON_CONTENT_TYPE: string = "application/json; charset=utf-8";
const HTML_CONTENT_TYPE: string = "text/html; charset=utf-8";
const CORS_ALLOW_HEADERS: string = "Authorization, Content-Type, MCP-Protocol-Version";
const CORS_ALLOW_METHODS: string = "GET, POST, OPTIONS";
const CORS_ALLOW_ORIGIN: string = "*";

export interface McpOauthHttpInput {
  fetchImpl: typeof fetch;
  nowSeconds: number;
  request: Request;
  signingKey: string;
  store: McpOauthStore;
}

interface AuthorizationCodeRecord {
  client_id: string;
  code_challenge: string;
  redirect_uri: string;
  resource: string;
  scope: string;
  sub: string;
}

interface RefreshTokenRecord {
  client_id: string;
  resource: string;
  scope: string;
  sub: string;
}

interface PendingAuthorization {
  client_id: string;
  client_name: string;
  code_challenge: string;
  redirect_uri: string;
  resource: string;
  state: string | null;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const corsHeaders = (): Record<string, string> => ({
  "Access-Control-Allow-Headers": CORS_ALLOW_HEADERS,
  "Access-Control-Allow-Methods": CORS_ALLOW_METHODS,
  "Access-Control-Allow-Origin": CORS_ALLOW_ORIGIN,
});

const jsonResponse = (body: unknown, status: number): Response =>
  new Response(JSON.stringify(body), {
    headers: {
      ...corsHeaders(),
      "Cache-Control": "no-store",
      "Content-Type": JSON_CONTENT_TYPE,
    },
    status,
  });

const optionsResponse = (): Response => new Response(null, { headers: corsHeaders(), status: 204 });

const htmlResponse = (body: string, status: number): Response =>
  new Response(body, { headers: { "Content-Type": HTML_CONTENT_TYPE }, status });

const oauthErrorRedirect = (
  redirectUri: string,
  error: string,
  state: string | null,
  iss: string,
): Response => {
  const url = new URL(redirectUri);
  url.searchParams.set("error", error);
  url.searchParams.set("iss", iss);
  if (state !== null) {
    url.searchParams.set("state", state);
  }
  return Response.redirect(url.toString(), 302);
};

const parseJsonBody = async (request: Request): Promise<unknown> => {
  const contentType = request.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return request.json();
  }
  return null;
};

const parseFormBody = async (request: Request): Promise<URLSearchParams> => {
  const text = await request.text();
  return new URLSearchParams(text);
};

const readPendingAuthorization = (raw: string): PendingAuthorization | null => {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) {
      return null;
    }
    const clientId = parsed.client_id;
    const clientName = parsed.client_name;
    const challenge = parsed.code_challenge;
    const redirectUri = parsed.redirect_uri;
    const resource = parsed.resource;
    const state = parsed.state;
    if (
      typeof clientId !== "string" ||
      typeof clientName !== "string" ||
      typeof challenge !== "string" ||
      typeof redirectUri !== "string" ||
      typeof resource !== "string"
    ) {
      return null;
    }
    return {
      client_id: clientId,
      client_name: clientName,
      code_challenge: challenge,
      redirect_uri: redirectUri,
      resource,
      state: typeof state === "string" ? state : null,
    };
  } catch {
    return null;
  }
};

const handleAuthorizeGet = async (input: McpOauthHttpInput): Promise<Response> => {
  const origin = originFromRequestUrl(input.request.url);
  const url = new URL(input.request.url);
  const clientId = url.searchParams.get("client_id");
  const redirectUri = url.searchParams.get("redirect_uri");
  const responseType = url.searchParams.get("response_type");
  const codeChallenge = url.searchParams.get("code_challenge");
  const method = url.searchParams.get("code_challenge_method");
  const resource = normalizeMcpResource(
    origin,
    url.searchParams.get("resource") ?? mcpResourceUrl(origin),
  );
  const state = url.searchParams.get("state");
  if (
    clientId === null ||
    redirectUri === null ||
    responseType !== "code" ||
    codeChallenge === null
  ) {
    return jsonResponse({ error: "invalid_request" }, 400);
  }
  if (!isS256Method(method)) {
    return jsonResponse({ error: "invalid_request", error_description: "PKCE S256 required" }, 400);
  }
  if (resource === null) {
    return jsonResponse({ error: "invalid_target" }, 400);
  }
  const client = await resolveClient(input.store, clientId, input.fetchImpl);
  if (client === null || !redirectUrisInclude(client.redirect_uris, redirectUri)) {
    return jsonResponse({ error: "invalid_client" }, 400);
  }
  const subject = readAccessUserSubject(input.request);
  if (subject === null) {
    return htmlResponse(renderMcpLoginRequiredPage(), 401);
  }
  const requestId = randomToken();
  const pending: PendingAuthorization = {
    client_id: client.client_id,
    client_name: client.client_name,
    code_challenge: codeChallenge,
    redirect_uri: redirectUri,
    resource,
    state,
  };
  await input.store.put(
    `mcp-oauth:authz:${requestId}`,
    JSON.stringify(pending),
    AUTH_REQUEST_TTL_SECONDS,
  );
  return htmlResponse(
    renderMcpConsentPage({ clientName: client.client_name, requestId, subject }),
    200,
  );
};

const handleAuthorizePost = async (input: McpOauthHttpInput): Promise<Response> => {
  const origin = originFromRequestUrl(input.request.url);
  const form = await parseFormBody(input.request);
  const requestId = form.get("request_id");
  const decision = form.get("decision");
  if (requestId === null) {
    return jsonResponse({ error: "invalid_request" }, 400);
  }
  const raw = await input.store.get(`mcp-oauth:authz:${requestId}`);
  if (raw === null) {
    return jsonResponse({ error: "invalid_request" }, 400);
  }
  await input.store.delete(`mcp-oauth:authz:${requestId}`);
  const pending = readPendingAuthorization(raw);
  if (pending === null) {
    return jsonResponse({ error: "invalid_request" }, 400);
  }
  if (decision !== "allow") {
    return oauthErrorRedirect(pending.redirect_uri, "access_denied", pending.state, origin);
  }
  const subject = readAccessUserSubject(input.request);
  if (subject === null) {
    return htmlResponse(renderMcpLoginRequiredPage(), 401);
  }
  const code = randomToken();
  const record: AuthorizationCodeRecord = {
    client_id: pending.client_id,
    code_challenge: pending.code_challenge,
    redirect_uri: pending.redirect_uri,
    resource: pending.resource,
    scope: MCP_OAUTH_SCOPE,
    sub: subject,
  };
  await input.store.put(`mcp-oauth:code:${code}`, JSON.stringify(record), CODE_TTL_SECONDS);
  const redirect = new URL(pending.redirect_uri);
  redirect.searchParams.set("code", code);
  redirect.searchParams.set("iss", origin);
  if (pending.state !== null) {
    redirect.searchParams.set("state", pending.state);
  }
  return Response.redirect(redirect.toString(), 302);
};

const parseCodeRecord = (raw: string): AuthorizationCodeRecord | null => {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) {
      return null;
    }
    const clientId = parsed.client_id;
    const challenge = parsed.code_challenge;
    const redirectUri = parsed.redirect_uri;
    const resource = parsed.resource;
    const scope = parsed.scope;
    const sub = parsed.sub;
    if (
      typeof clientId !== "string" ||
      typeof challenge !== "string" ||
      typeof redirectUri !== "string" ||
      typeof resource !== "string" ||
      typeof scope !== "string" ||
      typeof sub !== "string"
    ) {
      return null;
    }
    return {
      client_id: clientId,
      code_challenge: challenge,
      redirect_uri: redirectUri,
      resource,
      scope,
      sub,
    };
  } catch {
    return null;
  }
};

const parseRefreshRecord = (raw: string): RefreshTokenRecord | null => {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) {
      return null;
    }
    const clientId = parsed.client_id;
    const resource = parsed.resource;
    const scope = parsed.scope;
    const sub = parsed.sub;
    if (
      typeof clientId !== "string" ||
      typeof resource !== "string" ||
      typeof scope !== "string" ||
      typeof sub !== "string"
    ) {
      return null;
    }
    return { client_id: clientId, resource, scope, sub };
  } catch {
    return null;
  }
};

const issueTokens = async (
  input: McpOauthHttpInput,
  origin: string,
  record: RefreshTokenRecord,
): Promise<Response> => {
  const accessToken = await signHs256Jwt(
    {
      aud: record.resource,
      client_id: record.client_id,
      exp: input.nowSeconds + ACCESS_TTL_SECONDS,
      iat: input.nowSeconds,
      iss: origin,
      scope: record.scope,
      sub: record.sub,
    },
    input.signingKey,
  );
  const refreshToken = randomToken();
  await input.store.put(
    `mcp-oauth:rt:${refreshToken}`,
    JSON.stringify(record),
    REFRESH_TTL_SECONDS,
  );
  return jsonResponse(
    {
      access_token: accessToken,
      expires_in: ACCESS_TTL_SECONDS,
      refresh_token: refreshToken,
      scope: record.scope,
      token_type: "Bearer",
    },
    200,
  );
};

const handleToken = async (input: McpOauthHttpInput): Promise<Response> => {
  const origin = originFromRequestUrl(input.request.url);
  const form = await parseFormBody(input.request);
  const grantType = form.get("grant_type");
  const clientId = form.get("client_id");
  const resource = normalizeMcpResource(origin, form.get("resource") ?? mcpResourceUrl(origin));
  if (clientId === null || resource === null) {
    return jsonResponse({ error: "invalid_request" }, 400);
  }
  if (grantType === "refresh_token") {
    const refreshToken = form.get("refresh_token");
    if (refreshToken === null) {
      return jsonResponse({ error: "invalid_request" }, 400);
    }
    const raw = await input.store.get(`mcp-oauth:rt:${refreshToken}`);
    if (raw === null) {
      return jsonResponse({ error: "invalid_grant" }, 400);
    }
    const record = parseRefreshRecord(raw);
    if (record === null || record.client_id !== clientId || record.resource !== resource) {
      return jsonResponse({ error: "invalid_grant" }, 400);
    }
    await input.store.delete(`mcp-oauth:rt:${refreshToken}`);
    return issueTokens(input, origin, record);
  }
  if (grantType !== "authorization_code") {
    return jsonResponse({ error: "unsupported_grant_type" }, 400);
  }
  const code = form.get("code");
  const redirectUri = form.get("redirect_uri");
  const verifier = form.get("code_verifier");
  if (code === null || redirectUri === null || verifier === null) {
    return jsonResponse({ error: "invalid_request" }, 400);
  }
  const raw = await input.store.get(`mcp-oauth:code:${code}`);
  if (raw === null) {
    return jsonResponse({ error: "invalid_grant" }, 400);
  }
  await input.store.delete(`mcp-oauth:code:${code}`);
  const record = parseCodeRecord(raw);
  if (
    record === null ||
    record.client_id !== clientId ||
    record.redirect_uri !== redirectUri ||
    record.resource !== resource
  ) {
    return jsonResponse({ error: "invalid_grant" }, 400);
  }
  if (!(await verifyPkceS256(verifier, record.code_challenge))) {
    return jsonResponse({ error: "invalid_grant" }, 400);
  }
  return issueTokens(input, origin, {
    client_id: record.client_id,
    resource: record.resource,
    scope: record.scope,
    sub: record.sub,
  });
};

const handleRegister = async (input: McpOauthHttpInput): Promise<Response> => {
  const body = await parseJsonBody(input.request);
  const registered = await registerPublicClient(input.store, body);
  if (typeof registered === "string") {
    return jsonResponse({ error: registered }, 400);
  }
  return jsonResponse(
    {
      client_id: registered.client_id,
      client_name: registered.client_name,
      grant_types: registered.grant_types,
      redirect_uris: registered.redirect_uris,
      token_endpoint_auth_method: registered.token_endpoint_auth_method,
    },
    201,
  );
};

export const handleMcpOauthHttp = async (input: McpOauthHttpInput): Promise<Response | null> => {
  const url = new URL(input.request.url);
  const pathname = url.pathname;
  const origin = originFromRequestUrl(input.request.url);
  if (
    pathname === "/.well-known/oauth-protected-resource" ||
    pathname === "/.well-known/oauth-protected-resource/mcp"
  ) {
    if (input.request.method === "OPTIONS") {
      return optionsResponse();
    }
    return jsonResponse(buildProtectedResourceMetadata(origin), 200);
  }
  if (
    pathname === "/.well-known/oauth-authorization-server" ||
    pathname === "/.well-known/openid-configuration"
  ) {
    if (input.request.method === "OPTIONS") {
      return optionsResponse();
    }
    return jsonResponse(buildAuthorizationServerMetadata(origin), 200);
  }
  if (pathname === "/oauth/register") {
    if (input.request.method === "OPTIONS") {
      return optionsResponse();
    }
    if (input.request.method !== "POST") {
      return jsonResponse({ error: "invalid_request" }, 405);
    }
    return handleRegister(input);
  }
  if (pathname === "/oauth/token") {
    if (input.request.method === "OPTIONS") {
      return optionsResponse();
    }
    if (input.request.method !== "POST") {
      return jsonResponse({ error: "invalid_request" }, 405);
    }
    return handleToken(input);
  }
  if (pathname === "/oauth/authorize") {
    if (input.request.method === "GET") {
      return handleAuthorizeGet(input);
    }
    if (input.request.method === "POST") {
      return handleAuthorizePost(input);
    }
    return jsonResponse({ error: "invalid_request" }, 405);
  }
  return null;
};
