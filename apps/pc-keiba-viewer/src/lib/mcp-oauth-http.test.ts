// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { saveRegisteredClient } from "./mcp-oauth-clients";
import { handleMcpOauthHttp } from "./mcp-oauth-http";
import { sha256Base64Url } from "./mcp-oauth-pkce";
import { createMemoryOauthStore } from "./mcp-oauth-store";

const ORIGIN: string = "https://viewer.example.test";
const SIGNING_KEY: string = "oauth-signing-key-for-http-tests";
const NOW: number = 1_700_000_000;
const VERIFIER: string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ012";

const handle = (request: Request, fetchImpl: typeof fetch = fetch) =>
  handleMcpOauthHttp({
    fetchImpl,
    nowSeconds: NOW,
    request,
    signingKey: SIGNING_KEY,
    store: createMemoryOauthStore(),
  });

it("serves protected resource and authorization server metadata", async () => {
  const store = createMemoryOauthStore();
  const prm = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/.well-known/oauth-protected-resource`),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(prm?.status).toBe(200);
  const as = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/.well-known/oauth-authorization-server`),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(as?.status).toBe(200);
  const body: unknown = await as?.json();
  expect(JSON.parse(JSON.stringify(body))).toStrictEqual({
    authorization_endpoint: "https://viewer.example.test/oauth/authorize",
    authorization_response_iss_parameter_supported: true,
    client_id_metadata_document_supported: true,
    code_challenge_methods_supported: ["S256"],
    grant_types_supported: ["authorization_code", "refresh_token"],
    issuer: "https://viewer.example.test",
    registration_endpoint: "https://viewer.example.test/oauth/register",
    response_types_supported: ["code"],
    scopes_supported: ["mcp", "offline_access"],
    token_endpoint: "https://viewer.example.test/oauth/token",
    token_endpoint_auth_methods_supported: ["none"],
  });
});

it("registers a public client, consents, and issues tokens", async () => {
  const store = createMemoryOauthStore();
  const register = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`, {
      body: JSON.stringify({
        client_name: "Test Agent",
        redirect_uris: ["http://127.0.0.1:3456/callback"],
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(register?.status).toBe(201);
  const registered: unknown = await register?.json();
  const clientId =
    typeof registered === "object" && registered !== null && "client_id" in registered
      ? registered.client_id
      : null;
  expect(typeof clientId).toBe("string");
  if (typeof clientId !== "string") {
    return;
  }
  const challenge = await sha256Base64Url(VERIFIER);
  const authorize = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=${clientId}&redirect_uri=${encodeURIComponent("http://127.0.0.1:3456/callback")}&code_challenge=${challenge}&code_challenge_method=S256&resource=${encodeURIComponent(`${ORIGIN}/mcp`)}&state=xyz`,
      { headers: { "Cf-Access-Authenticated-User-Email": "user@example.test" } },
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(authorize?.status).toBe(200);
  const html = await authorize?.text();
  const requestIdMatch = html?.match(/name="request_id" value="([^"]+)"/);
  const requestId = requestIdMatch?.[1];
  expect(typeof requestId).toBe("string");
  if (typeof requestId !== "string") {
    return;
  }
  const consent = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: `request_id=${encodeURIComponent(requestId)}&decision=allow`,
      headers: {
        "Cf-Access-Authenticated-User-Email": "user@example.test",
        "Content-Type": "application/x-www-form-urlencoded",
      },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(consent?.status).toBe(302);
  const location = consent?.headers.get("Location") ?? "";
  const code = new URL(location).searchParams.get("code");
  expect(typeof code).toBe("string");
  if (code === null) {
    return;
  }
  const token = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=${encodeURIComponent(code)}&redirect_uri=${encodeURIComponent("http://127.0.0.1:3456/callback")}&client_id=${encodeURIComponent(clientId)}&code_verifier=${VERIFIER}&resource=${encodeURIComponent(`${ORIGIN}/mcp`)}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(token?.status).toBe(200);
  const issued: unknown = await token?.json();
  expect(
    typeof issued === "object" &&
      issued !== null &&
      "access_token" in issued &&
      "refresh_token" in issued,
  ).toBe(true);
});

it("returns null for unrelated paths", async () => {
  const response = await handle(new Request(`${ORIGIN}/races`));
  expect(response).toBe(null);
});

it("requires Access identity on the consent GET", async () => {
  const store = createMemoryOauthStore();
  const register = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`, {
      body: JSON.stringify({ redirect_uris: ["http://127.0.0.1/cb"] }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  const registered: unknown = await register?.json();
  const clientId =
    typeof registered === "object" && registered !== null && "client_id" in registered
      ? String(registered.client_id)
      : "";
  const challenge = await sha256Base64Url(VERIFIER);
  const authorize = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=${clientId}&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&code_challenge=${challenge}&code_challenge_method=S256`,
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(authorize?.status).toBe(401);
});

it("answers OPTIONS on discovery and token endpoints", async () => {
  const store = createMemoryOauthStore();
  const options = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, { method: "OPTIONS" }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(options?.status).toBe(204);
  expect(options?.headers.get("Access-Control-Allow-Origin")).toBe("*");
});

it("serves OpenID discovery and path-suffixed protected resource metadata", async () => {
  const store = createMemoryOauthStore();
  const oidc = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/.well-known/openid-configuration`),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(oidc?.status).toBe(200);
  const prm = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/.well-known/oauth-protected-resource/mcp`),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(prm?.status).toBe(200);
  const asOptions = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/.well-known/oauth-authorization-server`, {
      method: "OPTIONS",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(asOptions?.status).toBe(204);
  const prmOptions = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/.well-known/oauth-protected-resource`, { method: "OPTIONS" }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(prmOptions?.status).toBe(204);
});

it("rejects authorize requests that omit PKCE S256 or use a bad resource", async () => {
  const store = createMemoryOauthStore();
  const register = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`, {
      body: JSON.stringify({ redirect_uris: ["http://127.0.0.1/cb"] }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  const registered: unknown = await register?.json();
  const clientId =
    typeof registered === "object" && registered !== null && "client_id" in registered
      ? String(registered.client_id)
      : "";
  const missing = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(missing?.status).toBe(400);
  const challenge = await sha256Base64Url(VERIFIER);
  const plain = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=${clientId}&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&code_challenge=${challenge}&code_challenge_method=plain`,
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(plain?.status).toBe(400);
  const badResource = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=${clientId}&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&code_challenge=${challenge}&code_challenge_method=S256&resource=${encodeURIComponent("https://other.example.test/mcp")}`,
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(badResource?.status).toBe(400);
  const unknownClient = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=missing&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&code_challenge=${challenge}&code_challenge_method=S256`,
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(unknownClient?.status).toBe(400);
});

it("denies consent and includes iss on the error redirect", async () => {
  const store = createMemoryOauthStore();
  const register = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`, {
      body: JSON.stringify({ redirect_uris: ["http://127.0.0.1/cb"] }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  const registered: unknown = await register?.json();
  const clientId =
    typeof registered === "object" && registered !== null && "client_id" in registered
      ? String(registered.client_id)
      : "";
  const challenge = await sha256Base64Url(VERIFIER);
  const authorize = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=${clientId}&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&code_challenge=${challenge}&code_challenge_method=S256&state=st1`,
      { headers: { "Cf-Access-Authenticated-User-Email": "user@example.test" } },
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  const html = await authorize?.text();
  const requestIdMatch = html?.match(/name="request_id" value="([^"]+)"/);
  const requestId = requestIdMatch?.[1] ?? "";
  const deny = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: `request_id=${encodeURIComponent(requestId)}&decision=deny`,
      headers: {
        "Cf-Access-Authenticated-User-Email": "user@example.test",
        "Content-Type": "application/x-www-form-urlencoded",
      },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(deny?.status).toBe(302);
  const location = new URL(deny?.headers.get("Location") ?? "https://invalid.example.test/");
  expect(location.searchParams.get("error")).toBe("access_denied");
  expect(location.searchParams.get("iss")).toBe("https://viewer.example.test");
  expect(location.searchParams.get("state")).toBe("st1");
});

it("rejects consent posts that are missing, stale, or unauthenticated", async () => {
  const store = createMemoryOauthStore();
  const missing = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: "decision=allow",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(missing?.status).toBe(400);
  const stale = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: "request_id=missing-id&decision=allow",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(stale?.status).toBe(400);
  await store.put("mcp-oauth:authz:bad-json", "{", 600);
  const corrupt = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: "request_id=bad-json&decision=allow",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(corrupt?.status).toBe(400);
  await store.put(
    "mcp-oauth:authz:no-access",
    JSON.stringify({
      client_id: "c",
      client_name: "n",
      code_challenge: "ch",
      redirect_uri: "http://127.0.0.1/cb",
      resource: `${ORIGIN}/mcp`,
      state: null,
    }),
    600,
  );
  const noAccess = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: "request_id=no-access&decision=allow",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(noAccess?.status).toBe(401);
});

it("rejects token grants with a bad PKCE verifier, missing code, or wrong resource", async () => {
  const store = createMemoryOauthStore();
  const register = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`, {
      body: JSON.stringify({ redirect_uris: ["http://127.0.0.1/cb"] }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  const registered: unknown = await register?.json();
  const clientId =
    typeof registered === "object" && registered !== null && "client_id" in registered
      ? String(registered.client_id)
      : "";
  const challenge = await sha256Base64Url(VERIFIER);
  const authorize = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=${clientId}&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&code_challenge=${challenge}&code_challenge_method=S256`,
      { headers: { "Cf-Access-Authenticated-User-Email": "user@example.test" } },
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  const html = await authorize?.text();
  const requestIdMatch = html?.match(/name="request_id" value="([^"]+)"/);
  const requestId = requestIdMatch?.[1] ?? "";
  const consent = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: `request_id=${encodeURIComponent(requestId)}&decision=allow`,
      headers: {
        "Cf-Access-Authenticated-User-Email": "user@example.test",
        "Content-Type": "application/x-www-form-urlencoded",
      },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  const code = new URL(
    consent?.headers.get("Location") ?? "https://invalid.example.test/",
  ).searchParams.get("code");
  expect(typeof code).toBe("string");
  if (code === null) {
    return;
  }
  const missingVerifier = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=${encodeURIComponent(code)}&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=${encodeURIComponent(clientId)}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(missingVerifier?.status).toBe(400);
  const badVerifier = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=${encodeURIComponent(code)}&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=${encodeURIComponent(clientId)}&code_verifier=${"b".repeat(43)}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(badVerifier?.status).toBe(400);
  const reused = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=${encodeURIComponent(code)}&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=${encodeURIComponent(clientId)}&code_verifier=${VERIFIER}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(reused?.status).toBe(400);
});

it("rejects a token request whose resource does not match the authorization code", async () => {
  const store = createMemoryOauthStore();
  await store.put(
    "mcp-oauth:code:res-mismatch",
    JSON.stringify({
      client_id: "client-1",
      code_challenge: await sha256Base64Url(VERIFIER),
      redirect_uri: "http://127.0.0.1/cb",
      resource: "https://other.example.test/mcp",
      scope: "mcp",
      sub: "user@example.test",
    }),
    60,
  );
  const wrongResource = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=res-mismatch&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=client-1&code_verifier=${VERIFIER}&resource=${encodeURIComponent(`${ORIGIN}/mcp`)}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(wrongResource?.status).toBe(400);
});

it("rotates refresh tokens and rejects a reused refresh token", async () => {
  const store = createMemoryOauthStore();
  await store.put(
    "mcp-oauth:rt:refresh-1",
    JSON.stringify({
      client_id: "client-1",
      resource: `${ORIGIN}/mcp`,
      scope: "mcp",
      sub: "user@example.test",
    }),
    60,
  );
  const first = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=refresh_token&refresh_token=refresh-1&client_id=client-1&resource=${encodeURIComponent(`${ORIGIN}/mcp`)}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(first?.status).toBe(200);
  const issued: unknown = await first?.json();
  const refreshToken =
    typeof issued === "object" && issued !== null && "refresh_token" in issued
      ? String(issued.refresh_token)
      : "";
  const reused = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=refresh_token&refresh_token=refresh-1&client_id=client-1`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(reused?.status).toBe(400);
  const second = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(refreshToken)}&client_id=client-1`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(second?.status).toBe(200);
});

it("rejects unsupported grants, missing refresh tokens, and non-POST token methods", async () => {
  const store = createMemoryOauthStore();
  const unsupported = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: "grant_type=client_credentials&client_id=c",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(unsupported?.status).toBe(400);
  const missingRefresh = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: "grant_type=refresh_token&client_id=c",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(missingRefresh?.status).toBe(400);
  const getToken = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(getToken?.status).toBe(405);
  const missingClient = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: "grant_type=authorization_code",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(missingClient?.status).toBe(400);
});

it("treats a register POST without Content-Type as invalid client metadata", async () => {
  const store = createMemoryOauthStore();
  const response = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`, {
      body: JSON.stringify({ redirect_uris: ["http://127.0.0.1/cb"] }),
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(response?.status).toBe(400);
  expect(await response?.json()).toStrictEqual({ error: "invalid_client_metadata" });
});

it("rejects invalid DCR bodies and non-POST registration", async () => {
  const store = createMemoryOauthStore();
  const getRegister = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(getRegister?.status).toBe(405);
  const registerOptions = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`, { method: "OPTIONS" }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(registerOptions?.status).toBe(204);
  const notJson = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/register`, {
      body: "redirect_uris=http://127.0.0.1/cb",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(notJson?.status).toBe(400);
  const authorizePut = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, { method: "PUT" }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(authorizePut?.status).toBe(405);
});

it("rejects token exchange when the stored code JSON is corrupt or the client mismatches", async () => {
  const store = createMemoryOauthStore();
  await store.put("mcp-oauth:code:bad", "{", 60);
  const corrupt = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=bad&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=c&code_verifier=${VERIFIER}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(corrupt?.status).toBe(400);
  await store.put(
    "mcp-oauth:rt:wrong-client",
    JSON.stringify({
      client_id: "other",
      resource: `${ORIGIN}/mcp`,
      scope: "mcp",
      sub: "user@example.test",
    }),
    60,
  );
  const wrongClient = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: "grant_type=refresh_token&refresh_token=wrong-client&client_id=client-1",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(wrongClient?.status).toBe(400);
  await store.put("mcp-oauth:rt:bad", "[", 60);
  const badRefresh = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: "grant_type=refresh_token&refresh_token=bad&client_id=c",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(badRefresh?.status).toBe(400);
  const missingCode = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=missing&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=c&code_verifier=${VERIFIER}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(missingCode?.status).toBe(400);
});

it("ignores a null state on deny and rejects pending JSON that is not a record", async () => {
  const store = createMemoryOauthStore();
  await store.put("mcp-oauth:authz:array", "[]", 60);
  const arrayPending = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: "request_id=array&decision=deny",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(arrayPending?.status).toBe(400);
  await store.put(
    "mcp-oauth:authz:no-state",
    JSON.stringify({
      client_id: "c",
      client_name: "n",
      code_challenge: "ch",
      redirect_uri: "http://127.0.0.1/cb",
      resource: `${ORIGIN}/mcp`,
    }),
    60,
  );
  const deny = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: "request_id=no-state&decision=deny",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(deny?.status).toBe(302);
  const location = new URL(deny?.headers.get("Location") ?? "https://invalid.example.test/");
  expect(location.searchParams.get("state")).toBe(null);
});

it("answers OPTIONS on OpenID and path-suffixed protected resource metadata", async () => {
  const store = createMemoryOauthStore();
  const oidc = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/.well-known/openid-configuration`, { method: "OPTIONS" }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(oidc?.status).toBe(204);
  const prm = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/.well-known/oauth-protected-resource/mcp`, {
      method: "OPTIONS",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(prm?.status).toBe(204);
});

it("rejects a registered client whose redirect_uri is not listed", async () => {
  const store = createMemoryOauthStore();
  await saveRegisteredClient(store, {
    client_id: "public-client",
    client_name: "Test Agent",
    grant_types: ["authorization_code", "refresh_token"],
    redirect_uris: ["http://127.0.0.1:3456/callback"],
    token_endpoint_auth_method: "none",
  });
  const challenge = await sha256Base64Url(VERIFIER);
  const response = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=public-client&redirect_uri=${encodeURIComponent("http://127.0.0.1:9/other")}&code_challenge=${challenge}&code_challenge_method=S256`,
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(response?.status).toBe(400);
  expect(await response?.json()).toStrictEqual({ error: "invalid_client" });
});

it("authorizes a CIMD client_id after a successful metadata fetch", async () => {
  const store = createMemoryOauthStore();
  const challenge = await sha256Base64Url(VERIFIER);
  const response = await handleMcpOauthHttp({
    fetchImpl: async () =>
      new Response(
        JSON.stringify({
          client_id: "https://agents.example.test/client.json",
          client_name: "CIMD Agent",
          grant_types: ["authorization_code"],
          redirect_uris: ["http://127.0.0.1:3456/callback"],
          token_endpoint_auth_method: "none",
        }),
        { status: 200 },
      ),
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=${encodeURIComponent("https://agents.example.test/client.json")}&redirect_uri=${encodeURIComponent("http://127.0.0.1:3456/callback")}&code_challenge=${challenge}&code_challenge_method=S256`,
      { headers: { "Cf-Access-Authenticated-User-Email": "user@example.test" } },
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(response?.status).toBe(200);
  const html = await response?.text();
  expect(html?.indexOf("CIMD Agent") === -1).toBe(false);
});

it("rejects a CIMD client_id when metadata fetch fails", async () => {
  const store = createMemoryOauthStore();
  const challenge = await sha256Base64Url(VERIFIER);
  const response = await handleMcpOauthHttp({
    fetchImpl: async () => new Response("missing", { status: 404 }),
    nowSeconds: NOW,
    request: new Request(
      `${ORIGIN}/oauth/authorize?response_type=code&client_id=${encodeURIComponent("https://agents.example.test/client.json")}&redirect_uri=${encodeURIComponent("http://127.0.0.1:3456/callback")}&code_challenge=${challenge}&code_challenge_method=S256`,
    ),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(response?.status).toBe(400);
  expect(await response?.json()).toStrictEqual({ error: "invalid_client" });
});

it("rejects pending authorization JSON that is missing required strings", async () => {
  const store = createMemoryOauthStore();
  await store.put(
    "mcp-oauth:authz:partial",
    JSON.stringify({
      client_id: "public-client",
      client_name: "Test Agent",
      code_challenge: "challenge",
      redirect_uri: "http://127.0.0.1:3456/callback",
    }),
    600,
  );
  const response = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: "request_id=partial&decision=allow",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(response?.status).toBe(400);
  expect(await response?.json()).toStrictEqual({ error: "invalid_request" });
});

it("issues a code without state when pending state is not a string", async () => {
  const store = createMemoryOauthStore();
  await store.put(
    "mcp-oauth:authz:allow-nostate",
    JSON.stringify({
      client_id: "public-client",
      client_name: "Test Agent",
      code_challenge: "challenge",
      redirect_uri: "http://127.0.0.1:3456/callback",
      resource: "https://viewer.example.test/mcp",
      state: 1,
    }),
    600,
  );
  const response = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/authorize`, {
      body: "request_id=allow-nostate&decision=allow",
      headers: {
        "Cf-Access-Authenticated-User-Email": "user@example.test",
        "Content-Type": "application/x-www-form-urlencoded",
      },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(response?.status).toBe(302);
  const location = response?.headers.get("Location");
  if (location === null || location === undefined) {
    throw new Error("expected Location");
  }
  expect(new URL(location).searchParams.get("state")).toBe(null);
});

it("rejects authorization codes whose client, redirect, or JSON fields do not match", async () => {
  const store = createMemoryOauthStore();
  await store.put("mcp-oauth:code:array", "[]", 60);
  const arrayRecord = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=array&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=client-1&code_verifier=${VERIFIER}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(arrayRecord?.status).toBe(400);
  await store.put(
    "mcp-oauth:code:partial",
    JSON.stringify({
      client_id: "client-1",
      code_challenge: "challenge",
      redirect_uri: "http://127.0.0.1/cb",
      resource: "https://viewer.example.test/mcp",
    }),
    60,
  );
  const partial = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=partial&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=client-1&code_verifier=${VERIFIER}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(partial?.status).toBe(400);
  const challenge = await sha256Base64Url(VERIFIER);
  await store.put(
    "mcp-oauth:code:other-client",
    JSON.stringify({
      client_id: "other-client",
      code_challenge: challenge,
      redirect_uri: "http://127.0.0.1/cb",
      resource: "https://viewer.example.test/mcp",
      scope: "mcp",
      sub: "user@example.test",
    }),
    60,
  );
  const mismatchedClient = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=other-client&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=client-1&code_verifier=${VERIFIER}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(mismatchedClient?.status).toBe(400);
  await store.put(
    "mcp-oauth:code:other-redirect",
    JSON.stringify({
      client_id: "client-1",
      code_challenge: challenge,
      redirect_uri: "http://127.0.0.1:9/other",
      resource: "https://viewer.example.test/mcp",
      scope: "mcp",
      sub: "user@example.test",
    }),
    60,
  );
  const mismatchedRedirect = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=authorization_code&code=other-redirect&redirect_uri=${encodeURIComponent("http://127.0.0.1/cb")}&client_id=client-1&code_verifier=${VERIFIER}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(mismatchedRedirect?.status).toBe(400);
});

it("rejects a refresh token whose resource does not match the request", async () => {
  const store = createMemoryOauthStore();
  await store.put(
    "mcp-oauth:rt:resource-mismatch",
    JSON.stringify({
      client_id: "client-1",
      resource: "https://other.example.test/mcp",
      scope: "mcp",
      sub: "user@example.test",
    }),
    60,
  );
  const response = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: `grant_type=refresh_token&refresh_token=resource-mismatch&client_id=client-1&resource=${encodeURIComponent(`${ORIGIN}/mcp`)}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(response?.status).toBe(400);
  expect(await response?.json()).toStrictEqual({ error: "invalid_grant" });
});

it("rejects a refresh token record that is missing required strings", async () => {
  const store = createMemoryOauthStore();
  await store.put("mcp-oauth:rt:array", "[]", 60);
  const arrayRecord = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: "grant_type=refresh_token&refresh_token=array&client_id=client-1",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(arrayRecord?.status).toBe(400);
  await store.put(
    "mcp-oauth:rt:partial",
    JSON.stringify({
      client_id: "client-1",
      resource: "https://viewer.example.test/mcp",
    }),
    60,
  );
  const response = await handleMcpOauthHttp({
    fetchImpl: fetch,
    nowSeconds: NOW,
    request: new Request(`${ORIGIN}/oauth/token`, {
      body: "grant_type=refresh_token&refresh_token=partial&client_id=client-1",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      method: "POST",
    }),
    signingKey: SIGNING_KEY,
    store,
  });
  expect(response?.status).toBe(400);
  expect(await response?.json()).toStrictEqual({ error: "invalid_grant" });
});
