// Run with bun. Authentication helpers for the Worker and browser bootstrap UI.

const AUTHORIZATION_HEADER: string = "authorization";
const BASIC_PREFIX: string = "Basic ";
const BEARER_PREFIX: string = "Bearer ";
const BASIC_USERNAME: string = "jvlink";
const DIGEST_ALGORITHM: string = "SHA-256";

const encode = (value: string): Uint8Array => new TextEncoder().encode(value);

const digest = async (value: string): Promise<ArrayBuffer> =>
  crypto.subtle.digest(DIGEST_ALGORITHM, encode(value));

const equalBytes = (left: ArrayBuffer, right: ArrayBuffer): boolean => {
  const leftBytes = new Uint8Array(left);
  const rightBytes = new Uint8Array(right);
  return (
    leftBytes.reduce((difference, value, index) => difference | (value ^ rightBytes[index]!), 0) ===
    0
  );
};

const decodeBasicPassword = (authorization: string): string | undefined => {
  if (!authorization.startsWith(BASIC_PREFIX)) return undefined;
  try {
    const decoded = atob(authorization.slice(BASIC_PREFIX.length));
    const separator = decoded.indexOf(":");
    if (separator < 0 || decoded.slice(0, separator) !== BASIC_USERNAME) return undefined;
    return decoded.slice(separator + 1);
  } catch (error) {
    if (error instanceof DOMException) return undefined;
    throw error;
  }
};

export const authorizationCandidate = (request: Request): string | undefined => {
  const authorization = request.headers.get(AUTHORIZATION_HEADER);
  if (authorization === null) return undefined;
  if (authorization.startsWith(BEARER_PREFIX)) return authorization.slice(BEARER_PREFIX.length);
  return decodeBasicPassword(authorization);
};

export const isAuthorized = async (request: Request, secret: string): Promise<boolean> => {
  const candidate = authorizationCandidate(request);
  if (candidate === undefined || secret.length === 0) return false;
  const [candidateDigest, secretDigest] = await Promise.all([digest(candidate), digest(secret)]);
  return equalBytes(candidateDigest, secretDigest);
};

export const unauthorizedResponse = (): Response =>
  Response.json(
    { error: "Unauthorized" },
    {
      status: 401,
      headers: { "WWW-Authenticate": 'Basic realm="JV-Link demo", charset="UTF-8"' },
    },
  );
