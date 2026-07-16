// Run with bun.
export interface BasicAuthCredentials {
  username: string;
  password: string;
}

export interface AuthEnv {
  MLFLOW_UI_USERNAME: string;
  MLFLOW_UI_PASSWORD: string;
}

interface HmacAlgorithmParams {
  name: string;
  hash: string;
}

const BASIC_AUTH_PREFIX = "Basic ";
const BASIC_AUTH_SEPARATOR = ":";
const HMAC_ALGORITHM: HmacAlgorithmParams = { name: "HMAC", hash: "SHA-256" };
const HMAC_KEY_LENGTH_BYTES = 32;
const UNAUTHORIZED_STATUS = 401;
const UNAUTHORIZED_BODY = "Unauthorized";
const WWW_AUTHENTICATE_HEADER_NAME = "WWW-Authenticate";
const WWW_AUTHENTICATE_HEADER_VALUE = 'Basic realm="MLflow UI", charset="UTF-8"';
const AUTHORIZATION_HEADER_NAME = "Authorization";
const NOT_FOUND_INDEX = -1;

// atob throws on malformed base64 input; isolate that failure so a garbled
// Authorization header degrades to "unauthenticated" instead of crashing.
const decodeBase64 = (value: string): string | null => {
  try {
    return atob(value);
  } catch {
    return null;
  }
};

export const parseBasicAuth = (header: string | null): BasicAuthCredentials | null => {
  if (header === null || !header.startsWith(BASIC_AUTH_PREFIX)) {
    return null;
  }
  const decoded = decodeBase64(header.slice(BASIC_AUTH_PREFIX.length));
  if (decoded === null) {
    return null;
  }
  const separatorIndex = decoded.indexOf(BASIC_AUTH_SEPARATOR);
  if (separatorIndex === NOT_FOUND_INDEX) {
    return null;
  }
  return {
    username: decoded.slice(0, separatorIndex),
    password: decoded.slice(separatorIndex + 1),
  };
};

export const hmacDigest = async (key: CryptoKey, data: string): Promise<Uint8Array> => {
  const encoded = new TextEncoder().encode(data);
  const signature = await crypto.subtle.sign(HMAC_ALGORITHM, key, encoded);
  return new Uint8Array(signature);
};

export const timingSafeEqualBytes = (a: Uint8Array, b: Uint8Array): boolean => {
  if (a.length !== b.length) {
    return false;
  }
  const mismatch = a.reduce(
    (accumulator, byte, index) => accumulator | (byte ^ (b[index] ?? 0)),
    0,
  );
  return mismatch === 0;
};

const generateEphemeralHmacKey = (): Promise<CryptoKey> => {
  const rawKey = crypto.getRandomValues(new Uint8Array(HMAC_KEY_LENGTH_BYTES));
  return crypto.subtle.importKey("raw", rawKey, HMAC_ALGORITHM, false, ["sign"]);
};

const digestsMatch = async (
  key: CryptoKey,
  provided: string,
  expected: string,
): Promise<boolean> => {
  const [providedDigest, expectedDigest] = await Promise.all([
    hmacDigest(key, provided),
    hmacDigest(key, expected),
  ]);
  return timingSafeEqualBytes(providedDigest, expectedDigest);
};

export const isAuthorized = async (request: Request, env: AuthEnv): Promise<boolean> => {
  const provided = parseBasicAuth(request.headers.get(AUTHORIZATION_HEADER_NAME));
  if (provided === null) {
    return false;
  }
  const key = await generateEphemeralHmacKey();
  const [usernameMatches, passwordMatches] = await Promise.all([
    digestsMatch(key, provided.username, env.MLFLOW_UI_USERNAME),
    digestsMatch(key, provided.password, env.MLFLOW_UI_PASSWORD),
  ]);
  return usernameMatches && passwordMatches;
};

export const unauthorizedResponse = (): Response =>
  new Response(UNAUTHORIZED_BODY, {
    status: UNAUTHORIZED_STATUS,
    headers: { [WWW_AUTHENTICATE_HEADER_NAME]: WWW_AUTHENTICATE_HEADER_VALUE },
  });
