// Run with:
//   CLOUDFLARE_API_TOKEN=... bun run src/scripts/configure-access-dev-service-token.ts

const ACCOUNT_ID = "78109ec18c7c85b194b19fb32e3bb149";
const APP_NAME = "PC Keiba Viewer";
const TOKEN_NAME = "pc-keiba-viewer-local-dev";
const POLICY_NAME = "Local dev service token";

interface AccessApp {
  id: string;
  name: string;
}

interface AccessPolicy {
  id: string;
  name: string;
  precedence?: number;
}

interface ServiceToken {
  client_id: string;
  client_secret?: string;
  id: string;
  name: string;
}

const getApiToken = (): string => {
  const token = process.env.CLOUDFLARE_API_TOKEN?.trim();
  if (!token) {
    throw new Error("CLOUDFLARE_API_TOKEN is required.");
  }
  return token;
};

interface CloudflareResponseEnvelope {
  errors?: unknown[];
  result: unknown;
  success: boolean;
}

const buildHeaders = (init?: RequestInit): Headers => {
  const headers = new Headers(init?.headers);
  headers.set("Authorization", `Bearer ${getApiToken()}`);
  headers.set("content-type", "application/json");
  return headers;
};

const cloudflareRequest = async (
  path: string,
  init?: RequestInit,
): Promise<CloudflareResponseEnvelope> => {
  const response = await fetch(`https://api.cloudflare.com/client/v4${path}`, {
    ...init,
    headers: buildHeaders(init),
  });
  const payload: CloudflareResponseEnvelope = await response.json();
  if (!payload.success) {
    throw new Error(`Cloudflare API ${path} failed: ${JSON.stringify(payload.errors)}`);
  }
  return payload;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isAccessApp = (value: unknown): value is AccessApp =>
  isRecord(value) && typeof value.id === "string" && typeof value.name === "string";

const isAccessPolicy = (value: unknown): value is AccessPolicy => {
  if (!isRecord(value)) return false;
  if (typeof value.id !== "string" || typeof value.name !== "string") return false;
  return value.precedence === undefined || typeof value.precedence === "number";
};

const isServiceToken = (value: unknown): value is ServiceToken => {
  if (!isRecord(value)) return false;
  if (typeof value.client_id !== "string" || typeof value.id !== "string") return false;
  if (typeof value.name !== "string") return false;
  return value.client_secret === undefined || typeof value.client_secret === "string";
};

const filterAccessApps = (value: unknown): AccessApp[] =>
  Array.isArray(value) ? value.filter(isAccessApp) : [];

const filterServiceTokens = (value: unknown): ServiceToken[] =>
  Array.isArray(value) ? value.filter(isServiceToken) : [];

const filterAccessPolicies = (value: unknown): AccessPolicy[] =>
  Array.isArray(value) ? value.filter(isAccessPolicy) : [];

const findAccessApp = async (): Promise<AccessApp> => {
  const { result } = await cloudflareRequest(`/accounts/${ACCOUNT_ID}/access/apps`);
  const app = filterAccessApps(result).find((entry) => entry.name === APP_NAME);
  if (!app) {
    throw new Error(`Access application "${APP_NAME}" was not found.`);
  }
  return app;
};

const findServiceToken = async (): Promise<ServiceToken | null> => {
  const { result } = await cloudflareRequest(`/accounts/${ACCOUNT_ID}/access/service_tokens`);
  return filterServiceTokens(result).find((entry) => entry.name === TOKEN_NAME) ?? null;
};

const createServiceToken = async (): Promise<ServiceToken> => {
  const { result } = await cloudflareRequest(`/accounts/${ACCOUNT_ID}/access/service_tokens`, {
    body: JSON.stringify({ duration: "8760h", name: TOKEN_NAME }),
    method: "POST",
  });
  if (!isServiceToken(result)) {
    throw new Error("Cloudflare API returned an invalid service token payload.");
  }
  return result;
};

const listPolicies = async (appId: string): Promise<AccessPolicy[]> => {
  const { result } = await cloudflareRequest(
    `/accounts/${ACCOUNT_ID}/access/apps/${appId}/policies`,
  );
  return filterAccessPolicies(result);
};

const createServiceTokenPolicy = async (
  appId: string,
  tokenId: string,
  precedence: number,
): Promise<void> => {
  await cloudflareRequest(`/accounts/${ACCOUNT_ID}/access/apps/${appId}/policies`, {
    body: JSON.stringify({
      decision: "non_identity",
      include: [{ service_token: { token_id: tokenId } }],
      name: POLICY_NAME,
      precedence,
    }),
    method: "POST",
  });
};

const getNextPolicyPrecedence = (policies: AccessPolicy[]): number => {
  const maxPrecedence = policies.reduce((max, policy) => Math.max(max, policy.precedence ?? 0), 0);
  return maxPrecedence + 1;
};

const main = async (): Promise<void> => {
  const app = await findAccessApp();
  let token = await findServiceToken();
  let createdSecret = false;

  if (!token) {
    token = await createServiceToken();
    createdSecret = true;
    console.log(`[access-dev] created service token "${TOKEN_NAME}"`);
  } else {
    console.log(`[access-dev] reusing service token "${TOKEN_NAME}" (${token.id})`);
  }

  const policies = await listPolicies(app.id);
  const hasPolicy = policies.some((policy) => policy.name === POLICY_NAME);
  if (!hasPolicy) {
    await createServiceTokenPolicy(app.id, token.id, getNextPolicyPrecedence(policies));
    console.log(`[access-dev] created Access policy "${POLICY_NAME}"`);
  } else {
    console.log(`[access-dev] Access policy "${POLICY_NAME}" already exists`);
  }

  console.log("");
  console.log("Add the following to apps/pc-keiba-viewer/.env.local:");
  console.log(`PC_KEIBA_ACCESS_CLIENT_ID=${token.client_id}`);
  if (token.client_secret) {
    console.log(`PC_KEIBA_ACCESS_CLIENT_SECRET=${token.client_secret}`);
  } else if (!createdSecret) {
    console.log(
      "# PC_KEIBA_ACCESS_CLIENT_SECRET=<existing secret for this token; recreate token if lost>",
    );
  }
  console.log("PC_KEIBA_PRODUCTION_API_PROXY=1");
  console.log("NEXT_PUBLIC_PC_KEIBA_PRODUCTION_API_PROXY=1");
};

await main();
