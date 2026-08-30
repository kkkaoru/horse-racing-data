// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

const AUTHORIZATION_HEADER_NAME: string = "Authorization";
const BEARER_PREFIX: string = "Bearer ";
const UNAUTHORIZED_BODY: string = JSON.stringify({ error: { message: "Unauthorized" } });
const UNAUTHORIZED_STATUS: number = 401;
const WWW_AUTHENTICATE_HEADER_NAME: string = "WWW-Authenticate";
const WWW_AUTHENTICATE_REALM: string = "pc-keiba-viewer-mcp";

export const timingSafeEqualBytes = (left: Uint8Array, right: Uint8Array): boolean => {
  if (left.length !== right.length) {
    return false;
  }
  const mismatch = left.reduce(
    (accumulator, byte, index) => accumulator | (byte ^ (right[index] ?? 0)),
    0,
  );
  return mismatch === 0;
};

export const secretsEqual = (left: string, right: string): boolean => {
  const encoder = new TextEncoder();
  return timingSafeEqualBytes(encoder.encode(left), encoder.encode(right));
};

export const parseBearerToken = (header: string | null): string | null => {
  if (header === null) {
    return null;
  }
  if (header.length < BEARER_PREFIX.length) {
    return null;
  }
  const prefix = header.slice(0, BEARER_PREFIX.length);
  if (prefix.toLowerCase() !== BEARER_PREFIX.toLowerCase()) {
    return null;
  }
  const token = header.slice(BEARER_PREFIX.length).trim();
  if (token.length === 0) {
    return null;
  }
  return token;
};

export const isMcpAuthorized = (request: Request, expectedToken: string): boolean => {
  if (expectedToken.trim().length === 0) {
    return false;
  }
  const provided = parseBearerToken(request.headers.get(AUTHORIZATION_HEADER_NAME));
  if (provided === null) {
    return false;
  }
  return secretsEqual(provided, expectedToken);
};

export const buildMcpWwwAuthenticate = (origin: string): string =>
  `Bearer realm="${WWW_AUTHENTICATE_REALM}", resource_metadata="${origin}/.well-known/oauth-protected-resource", scope="mcp"`;

export const mcpUnauthorizedResponse = (origin: string): Response =>
  new Response(UNAUTHORIZED_BODY, {
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Expose-Headers": WWW_AUTHENTICATE_HEADER_NAME,
      "Content-Type": "application/json; charset=utf-8",
      [WWW_AUTHENTICATE_HEADER_NAME]: buildMcpWwwAuthenticate(origin),
    },
    status: UNAUTHORIZED_STATUS,
  });
