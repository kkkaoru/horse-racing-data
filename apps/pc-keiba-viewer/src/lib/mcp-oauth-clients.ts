// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { randomToken } from "./mcp-oauth-random";
import { isAllowedOAuthRedirectUri } from "./mcp-oauth-redirect";
import type { McpOauthStore } from "./mcp-oauth-store";

export interface RegisteredOAuthClient {
  client_id: string;
  client_name: string;
  grant_types: readonly string[];
  redirect_uris: readonly string[];
  token_endpoint_auth_method: string;
}

const CLIENT_TTL_SECONDS: number = 60 * 60 * 24 * 90;
const CLIENT_KEY_PREFIX: string = "mcp-oauth:client:";
const DEFAULT_CLIENT_NAME: string = "MCP client";
const DEFAULT_GRANT_TYPES: readonly string[] = ["authorization_code", "refresh_token"];
const PUBLIC_TOKEN_AUTH_METHOD: string = "none";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const clientStorageKey = (clientId: string): string => `${CLIENT_KEY_PREFIX}${clientId}`;

export const parseRegisteredClient = (raw: string): RegisteredOAuthClient | null => {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) {
      return null;
    }
    const clientId = parsed.client_id;
    const clientName = parsed.client_name;
    const method = parsed.token_endpoint_auth_method;
    const redirectUris = parsed.redirect_uris;
    const grantTypes = parsed.grant_types;
    if (typeof clientId !== "string" || clientId.length === 0) {
      return null;
    }
    if (typeof clientName !== "string") {
      return null;
    }
    if (typeof method !== "string") {
      return null;
    }
    if (!Array.isArray(redirectUris) || !redirectUris.every((uri) => typeof uri === "string")) {
      return null;
    }
    if (!Array.isArray(grantTypes) || !grantTypes.every((grant) => typeof grant === "string")) {
      return null;
    }
    return {
      client_id: clientId,
      client_name: clientName,
      grant_types: grantTypes,
      redirect_uris: redirectUris,
      token_endpoint_auth_method: method,
    };
  } catch {
    return null;
  }
};

export const loadRegisteredClient = async (
  store: McpOauthStore,
  clientId: string,
): Promise<RegisteredOAuthClient | null> => {
  const raw = await store.get(clientStorageKey(clientId));
  return raw === null ? null : parseRegisteredClient(raw);
};

export const saveRegisteredClient = async (
  store: McpOauthStore,
  client: RegisteredOAuthClient,
): Promise<void> => {
  await store.put(clientStorageKey(client.client_id), JSON.stringify(client), CLIENT_TTL_SECONDS);
};

export const registerPublicClient = async (
  store: McpOauthStore,
  body: unknown,
): Promise<RegisteredOAuthClient | string> => {
  if (!isRecord(body)) {
    return "invalid_client_metadata";
  }
  const redirectUrisRaw = body.redirect_uris;
  if (!Array.isArray(redirectUrisRaw) || redirectUrisRaw.length === 0) {
    return "invalid_redirect_uri";
  }
  const redirectUris = redirectUrisRaw.filter((uri): uri is string => typeof uri === "string");
  if (redirectUris.length !== redirectUrisRaw.length) {
    return "invalid_redirect_uri";
  }
  if (redirectUris.some((uri) => !isAllowedOAuthRedirectUri(uri))) {
    return "invalid_redirect_uri";
  }
  const clientName = typeof body.client_name === "string" ? body.client_name : DEFAULT_CLIENT_NAME;
  const client: RegisteredOAuthClient = {
    client_id: randomToken(),
    client_name: clientName,
    grant_types: DEFAULT_GRANT_TYPES,
    redirect_uris: redirectUris,
    token_endpoint_auth_method: PUBLIC_TOKEN_AUTH_METHOD,
  };
  await saveRegisteredClient(store, client);
  return client;
};

const isHttpsClientId = (clientId: string): boolean => {
  try {
    const url = new URL(clientId);
    return url.protocol === "https:" && url.pathname.length > 1;
  } catch {
    return false;
  }
};

export const resolveClient = async (
  store: McpOauthStore,
  clientId: string,
  fetchImpl: typeof fetch,
): Promise<RegisteredOAuthClient | null> => {
  const stored = await loadRegisteredClient(store, clientId);
  if (stored !== null) {
    return stored;
  }
  if (!isHttpsClientId(clientId)) {
    return null;
  }
  const response = await fetchImpl(clientId, { method: "GET", redirect: "manual" });
  if (!response.ok) {
    return null;
  }
  try {
    const parsed: unknown = await response.json();
    const client = parseClientIdMetadataDocument(parsed, clientId);
    if (client === null) {
      return null;
    }
    if (client.redirect_uris.some((uri) => !isAllowedOAuthRedirectUri(uri))) {
      return null;
    }
    await saveRegisteredClient(store, client);
    return client;
  } catch {
    return null;
  }
};

export const parseClientIdMetadataDocument = (
  body: unknown,
  expectedClientId: string,
): RegisteredOAuthClient | null => {
  if (!isRecord(body)) {
    return null;
  }
  if (body.client_id !== expectedClientId) {
    return null;
  }
  const redirectUrisRaw = body.redirect_uris;
  if (!Array.isArray(redirectUrisRaw) || redirectUrisRaw.length === 0) {
    return null;
  }
  const redirectUris = redirectUrisRaw.filter((uri): uri is string => typeof uri === "string");
  if (redirectUris.length !== redirectUrisRaw.length) {
    return null;
  }
  const grantTypesRaw = body.grant_types;
  const grantTypes = Array.isArray(grantTypesRaw)
    ? grantTypesRaw.filter((grant): grant is string => typeof grant === "string")
    : [...DEFAULT_GRANT_TYPES];
  if (grantTypes.length === 0) {
    return null;
  }
  const clientName = typeof body.client_name === "string" ? body.client_name : DEFAULT_CLIENT_NAME;
  const method =
    typeof body.token_endpoint_auth_method === "string"
      ? body.token_endpoint_auth_method
      : PUBLIC_TOKEN_AUTH_METHOD;
  return {
    client_id: expectedClientId,
    client_name: clientName,
    grant_types: grantTypes,
    redirect_uris: redirectUris,
    token_endpoint_auth_method: method,
  };
};
