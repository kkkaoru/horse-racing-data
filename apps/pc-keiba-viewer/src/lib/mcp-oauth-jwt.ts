// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { toBase64Url } from "./mcp-oauth-pkce";

export interface McpAccessTokenClaims {
  aud: string;
  client_id: string;
  exp: number;
  iat: number;
  iss: string;
  scope: string;
  sub: string;
}

const JWT_HEADER_JSON: string = JSON.stringify({ alg: "HS256", typ: "JWT" });

const base64UrlToBytes = (value: string): Uint8Array | null => {
  const padded = value.replaceAll("-", "+").replaceAll("_", "/");
  const padLength = (4 - (padded.length % 4)) % 4;
  try {
    const binary = atob(`${padded}${"=".repeat(padLength)}`);
    return Uint8Array.from(binary, (char) => char.charCodeAt(0));
  } catch {
    return null;
  }
};

const importHmacKey = (secret: string): Promise<CryptoKey> =>
  crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { hash: "SHA-256", name: "HMAC" },
    false,
    ["sign", "verify"],
  );

export const signHs256Jwt = async (
  claims: McpAccessTokenClaims,
  secret: string,
): Promise<string> => {
  const header = toBase64Url(new TextEncoder().encode(JWT_HEADER_JSON));
  const payload = toBase64Url(new TextEncoder().encode(JSON.stringify(claims)));
  const data = `${header}.${payload}`;
  const key = await importHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  return `${data}.${toBase64Url(new Uint8Array(signature))}`;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const readStringClaim = (value: unknown): string | null =>
  typeof value === "string" && value.length > 0 ? value : null;

const readNumberClaim = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

export const verifyHs256Jwt = async (
  token: string,
  secret: string,
  expectedIss: string,
  expectedAud: string,
  nowSeconds: number,
): Promise<McpAccessTokenClaims | null> => {
  const parts = token.split(".");
  if (parts.length !== 3) {
    return null;
  }
  const headerPart = parts[0];
  const payloadPart = parts[1];
  const signaturePart = parts[2];
  if (headerPart === undefined || payloadPart === undefined || signaturePart === undefined) {
    return null;
  }
  const signed = `${headerPart}.${payloadPart}`;
  const signatureBytes = base64UrlToBytes(signaturePart);
  if (signatureBytes === null) {
    return null;
  }
  const key = await importHmacKey(secret);
  const signatureCopy = new Uint8Array(signatureBytes.byteLength);
  signatureCopy.set(signatureBytes);
  const valid = await crypto.subtle.verify(
    "HMAC",
    key,
    signatureCopy,
    new TextEncoder().encode(signed),
  );
  if (!valid) {
    return null;
  }
  const payloadBytes = base64UrlToBytes(payloadPart);
  if (payloadBytes === null) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(new TextDecoder().decode(payloadBytes));
    if (!isRecord(parsed)) {
      return null;
    }
    const iss = readStringClaim(parsed.iss);
    const aud = readStringClaim(parsed.aud);
    const sub = readStringClaim(parsed.sub);
    const clientId = readStringClaim(parsed.client_id);
    const scope = readStringClaim(parsed.scope);
    const exp = readNumberClaim(parsed.exp);
    const iat = readNumberClaim(parsed.iat);
    if (
      iss === null ||
      aud === null ||
      sub === null ||
      clientId === null ||
      scope === null ||
      exp === null ||
      iat === null
    ) {
      return null;
    }
    if (iss !== expectedIss || aud !== expectedAud || exp <= nowSeconds) {
      return null;
    }
    return { aud, client_id: clientId, exp, iat, iss, scope, sub };
  } catch {
    return null;
  }
};
