// Run with bun. Bearer authentication for the public Worker API.

const PREFIX: string = "Bearer ";
const encode = (value: string): Uint8Array<ArrayBuffer> => new TextEncoder().encode(value);
const digest = (value: string): Promise<ArrayBuffer> =>
  crypto.subtle.digest("SHA-256", encode(value));

export const isAuthorized = async (request: Request, secret: string): Promise<boolean> => {
  const authorization: string | null = request.headers.get("Authorization");
  if (authorization === null || !authorization.startsWith(PREFIX) || secret.length === 0)
    return false;
  const candidate: ArrayBuffer = await digest(authorization.slice(PREFIX.length));
  const expected: ArrayBuffer = await digest(secret);
  const left: Uint8Array = new Uint8Array(candidate);
  const right: Uint8Array = new Uint8Array(expected);
  return (
    left.reduce(
      (difference: number, value: number, index: number): number =>
        difference | (value ^ (right[index] ?? 0)),
      0,
    ) === 0
  );
};

export const unauthorizedResponse = (): Response =>
  Response.json(
    { error: "Unauthorized" },
    { headers: { "WWW-Authenticate": "Bearer" }, status: 401 },
  );
