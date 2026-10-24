// Run with bun. Verifies a Cloudflare Access JWT (the CF_Authorization cookie) the way the
// Access edge would, for requests that reached the Worker through a bypass app where Access
// itself does not check the viewer.

export interface AccessJwtConfig {
  aud: string;
  fetchImpl: typeof fetch;
  nowMs: number;
  teamDomain: string;
}

interface AccessJwk {
  e: string;
  kid: string;
  kty: string;
  n: string;
}

interface CachedKeys {
  expiresAt: number;
  keys: AccessJwk[];
}

const ACCESS_COOKIE_PATTERN: RegExp = /(?:^|;\s*)CF_Authorization=([^;]+)/u;
const CERTS_TTL_MS: number = 3_600_000;
const BASE64_BLOCK: number = 4;
const SIGNATURE_ALGORITHM: RsaHashedImportParams = {
  hash: "SHA-256",
  name: "RSASSA-PKCS1-v1_5",
};
// Access rotates its signing keys rarely; one fetch per isolate per hour is enough.
const certsCache: Map<string, CachedKeys> = new Map();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isJwk = (value: unknown): value is AccessJwk =>
  isRecord(value) &&
  typeof value.kid === "string" &&
  typeof value.kty === "string" &&
  typeof value.n === "string" &&
  typeof value.e === "string";

const base64UrlDecode = (value: string): Uint8Array<ArrayBuffer> => {
  const base64 = value.replaceAll("-", "+").replaceAll("_", "/");
  const padded = base64.padEnd(Math.ceil(base64.length / BASE64_BLOCK) * BASE64_BLOCK, "=");
  return Uint8Array.from(atob(padded), (char) => char.charCodeAt(0));
};

const decodeJson = (part: string): unknown =>
  JSON.parse(new TextDecoder().decode(base64UrlDecode(part)));

export const readAccessCookie = (cookieHeader: string | null): string | null =>
  ACCESS_COOKIE_PATTERN.exec(cookieHeader ?? "")?.[1] ?? null;

const loadKeys = async (config: AccessJwtConfig): Promise<AccessJwk[]> => {
  const cached = certsCache.get(config.teamDomain);
  if (cached !== undefined && cached.expiresAt > config.nowMs) return cached.keys;
  const response = await config.fetchImpl(`https://${config.teamDomain}/cdn-cgi/access/certs`);
  const body: unknown = response.ok ? await response.json() : null;
  const keys = isRecord(body) && Array.isArray(body.keys) ? body.keys.filter(isJwk) : [];
  // A failed fetch is not cached, so the next request retries it.
  if (keys.length > 0)
    certsCache.set(config.teamDomain, { expiresAt: config.nowMs + CERTS_TTL_MS, keys });
  return keys;
};

const hasValidClaims = (claims: unknown, config: AccessJwtConfig): boolean =>
  isRecord(claims) &&
  (Array.isArray(claims.aud) ? claims.aud.includes(config.aud) : claims.aud === config.aud) &&
  typeof claims.exp === "number" &&
  claims.exp * 1000 > config.nowMs &&
  claims.iss === `https://${config.teamDomain}`;

export const verifyAccessJwt = async (token: string, config: AccessJwtConfig): Promise<boolean> => {
  const [header, payload, signature] = token.split(".");
  if (header === undefined || payload === undefined || signature === undefined) return false;
  try {
    const head = decodeJson(header);
    if (!isRecord(head) || head.alg !== "RS256" || !hasValidClaims(decodeJson(payload), config))
      return false;
    const jwk = (await loadKeys(config)).find((key) => key.kid === head.kid);
    if (jwk === undefined) return false;
    const key = await crypto.subtle.importKey(
      "jwk",
      { alg: "RS256", e: jwk.e, ext: true, kty: jwk.kty, n: jwk.n },
      SIGNATURE_ALGORITHM,
      false,
      ["verify"],
    );
    return await crypto.subtle.verify(
      SIGNATURE_ALGORITHM.name,
      key,
      base64UrlDecode(signature),
      new TextEncoder().encode(`${header}.${payload}`),
    );
  } catch {
    // Malformed tokens and keys are simply not a signed-in viewer.
    return false;
  }
};
