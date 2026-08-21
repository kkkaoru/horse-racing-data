// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { signHs256Jwt, verifyHs256Jwt } from "./mcp-oauth-jwt";
import { toBase64Url } from "./mcp-oauth-pkce";

const SECRET: string = "test-signing-secret";
const ISS: string = "https://viewer.example.test";
const AUD: string = "https://viewer.example.test/mcp";

const signRaw = async (payloadJson: string): Promise<string> => {
  const header = toBase64Url(
    new TextEncoder().encode(JSON.stringify({ alg: "HS256", typ: "JWT" })),
  );
  const payload = toBase64Url(new TextEncoder().encode(payloadJson));
  const data = `${header}.${payload}`;
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(SECRET),
    { hash: "SHA-256", name: "HMAC" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  return `${data}.${toBase64Url(new Uint8Array(signature))}`;
};

const signWithPayloadPart = async (payloadPart: string): Promise<string> => {
  const header = toBase64Url(
    new TextEncoder().encode(JSON.stringify({ alg: "HS256", typ: "JWT" })),
  );
  const data = `${header}.${payloadPart}`;
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(SECRET),
    { hash: "SHA-256", name: "HMAC" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  return `${data}.${toBase64Url(new Uint8Array(signature))}`;
};

it("round-trips a valid access token and rejects expired or wrong audience tokens", async () => {
  const token = await signHs256Jwt(
    {
      aud: AUD,
      client_id: "client-1",
      exp: 2_000,
      iat: 1_000,
      iss: ISS,
      scope: "mcp",
      sub: "user@example.test",
    },
    SECRET,
  );
  const valid = await verifyHs256Jwt(token, SECRET, ISS, AUD, 1_500);
  expect(valid).toStrictEqual({
    aud: "https://viewer.example.test/mcp",
    client_id: "client-1",
    exp: 2_000,
    iat: 1_000,
    iss: "https://viewer.example.test",
    scope: "mcp",
    sub: "user@example.test",
  });
  expect(await verifyHs256Jwt(token, SECRET, ISS, AUD, 2_000)).toBe(null);
  expect(await verifyHs256Jwt(token, SECRET, ISS, AUD, 2_001)).toBe(null);
  expect(await verifyHs256Jwt(token, SECRET, ISS, "https://other.example.test/mcp", 1_500)).toBe(
    null,
  );
  expect(await verifyHs256Jwt("not-a-jwt", SECRET, ISS, AUD, 1_500)).toBe(null);
  expect(await verifyHs256Jwt("a.b", SECRET, ISS, AUD, 1_500)).toBe(null);
  expect(await verifyHs256Jwt(token, "wrong-secret", ISS, AUD, 1_500)).toBe(null);
});

it("rejects a token whose signature is not base64url", async () => {
  expect(await verifyHs256Jwt("aaa.bbb.!!!", SECRET, ISS, AUD, 1_500)).toBe(null);
});

it("rejects a signed payload that is not JSON", async () => {
  const token = await signRaw("not-json");
  expect(await verifyHs256Jwt(token, SECRET, ISS, AUD, 1_500)).toBe(null);
});

it("rejects a signed payload that is not an object", async () => {
  const token = await signRaw("[]");
  expect(await verifyHs256Jwt(token, SECRET, ISS, AUD, 1_500)).toBe(null);
});

it("rejects signed payloads with missing or empty claims", async () => {
  expect(await verifyHs256Jwt(await signRaw("{}"), SECRET, ISS, AUD, 1_500)).toBe(null);
  expect(
    await verifyHs256Jwt(
      await signRaw(
        JSON.stringify({
          aud: AUD,
          client_id: "client-1",
          exp: 2_000,
          iat: 1_000,
          iss: "",
          scope: "mcp",
          sub: "user@example.test",
        }),
      ),
      SECRET,
      ISS,
      AUD,
      1_500,
    ),
  ).toBe(null);
  expect(
    await verifyHs256Jwt(
      await signRaw(
        JSON.stringify({
          aud: AUD,
          client_id: "client-1",
          exp: Number.NaN,
          iat: 1_000,
          iss: ISS,
          scope: "mcp",
          sub: "user@example.test",
        }),
      ),
      SECRET,
      ISS,
      AUD,
      1_500,
    ),
  ).toBe(null);
  expect(
    await verifyHs256Jwt(
      await signRaw(
        JSON.stringify({
          aud: AUD,
          client_id: "client-1",
          exp: 2_000,
          iat: "1000",
          iss: ISS,
          scope: "mcp",
          sub: "user@example.test",
        }),
      ),
      SECRET,
      ISS,
      AUD,
      1_500,
    ),
  ).toBe(null);
});

it("rejects a token with the wrong issuer", async () => {
  const token = await signHs256Jwt(
    {
      aud: AUD,
      client_id: "client-1",
      exp: 2_000,
      iat: 1_000,
      iss: "https://other.example.test",
      scope: "mcp",
      sub: "user@example.test",
    },
    SECRET,
  );
  expect(await verifyHs256Jwt(token, SECRET, ISS, AUD, 1_500)).toBe(null);
});

it("rejects a signed payload that is not valid base64url", async () => {
  const token = await signWithPayloadPart("!!!");
  expect(await verifyHs256Jwt(token, SECRET, ISS, AUD, 1_500)).toBe(null);
});
