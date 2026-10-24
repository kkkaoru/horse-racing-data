import { expect, it, vi } from "vitest";

import { readAccessCookie, verifyAccessJwt } from "./access-jwt";

const AUD = "app-aud";
const NOW_MS = Date.parse("2026-10-25T12:00:00Z");
const EXP = NOW_MS / 1000 + 3600;

const encode = (value: unknown): string =>
  btoa(typeof value === "string" ? value : JSON.stringify(value))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");

const encodeBytes = (bytes: ArrayBuffer): string =>
  btoa(String.fromCharCode(...new Uint8Array(bytes)))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");

const keyPair = async (): Promise<CryptoKeyPair> =>
  crypto.subtle.generateKey(
    {
      hash: "SHA-256",
      modulusLength: 2048,
      name: "RSASSA-PKCS1-v1_5",
      publicExponent: new Uint8Array([1, 0, 1]),
    },
    true,
    ["sign", "verify"],
  );

const sign = async (
  pair: CryptoKeyPair,
  header: Record<string, unknown>,
  claims: Record<string, unknown>,
): Promise<string> => {
  const body = `${encode(header)}.${encode(claims)}`;
  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    pair.privateKey,
    new TextEncoder().encode(body),
  );
  return `${body}.${encodeBytes(signature)}`;
};

const certsFetch = async (pair: CryptoKeyPair) => {
  const jwk = await crypto.subtle.exportKey("jwk", pair.publicKey);
  return vi
    .fn<typeof fetch>()
    .mockResolvedValue(Response.json({ keys: [{ ...jwk, kid: "k1" }, { kid: 1 }] }));
};

it("reads the CF_Authorization cookie", () => {
  expect(readAccessCookie("a=1; CF_Authorization=tok.en.sig; b=2")).toBe("tok.en.sig");
  expect(readAccessCookie("a=1")).toBeNull();
  expect(readAccessCookie(null)).toBeNull();
});

it("accepts a token signed by Access for the app audience", async () => {
  const pair = await keyPair();
  const fetchImpl = await certsFetch(pair);
  const token = await sign(
    pair,
    { alg: "RS256", kid: "k1" },
    { aud: [AUD], exp: EXP, iss: "https://team-a.cloudflareaccess.com" },
  );
  const config = { aud: AUD, fetchImpl, nowMs: NOW_MS, teamDomain: "team-a.cloudflareaccess.com" };
  expect(await verifyAccessJwt(token, config)).toBe(true);
  expect(await verifyAccessJwt(token, config)).toBe(true);
  expect(fetchImpl).toHaveBeenCalledTimes(1);
  expect(fetchImpl).toHaveBeenCalledWith(
    "https://team-a.cloudflareaccess.com/cdn-cgi/access/certs",
  );
});

it("accepts a single string audience", async () => {
  const pair = await keyPair();
  const token = await sign(
    pair,
    { alg: "RS256", kid: "k1" },
    { aud: AUD, exp: EXP, iss: "https://team-b.cloudflareaccess.com" },
  );
  expect(
    await verifyAccessJwt(token, {
      aud: AUD,
      fetchImpl: await certsFetch(pair),
      nowMs: NOW_MS,
      teamDomain: "team-b.cloudflareaccess.com",
    }),
  ).toBe(true);
});

it("rejects a token signed by another key", async () => {
  const pair = await keyPair();
  const other = await keyPair();
  const token = await sign(
    other,
    { alg: "RS256", kid: "k1" },
    { aud: [AUD], exp: EXP, iss: "https://team-c.cloudflareaccess.com" },
  );
  expect(
    await verifyAccessJwt(token, {
      aud: AUD,
      fetchImpl: await certsFetch(pair),
      nowMs: NOW_MS,
      teamDomain: "team-c.cloudflareaccess.com",
    }),
  ).toBe(false);
});

it("rejects expired, foreign-audience, foreign-issuer and non-RS256 tokens", async () => {
  const pair = await keyPair();
  const config = {
    aud: AUD,
    fetchImpl: await certsFetch(pair),
    nowMs: NOW_MS,
    teamDomain: "team-d.cloudflareaccess.com",
  };
  const iss = "https://team-d.cloudflareaccess.com";
  const expired = await sign(pair, { alg: "RS256", kid: "k1" }, { aud: [AUD], exp: 1, iss });
  const otherAud = await sign(pair, { alg: "RS256", kid: "k1" }, { aud: ["x"], exp: EXP, iss });
  const otherIss = await sign(
    pair,
    { alg: "RS256", kid: "k1" },
    { aud: [AUD], exp: EXP, iss: "https://evil.example" },
  );
  const noneAlg = await sign(pair, { alg: "none", kid: "k1" }, { aud: [AUD], exp: EXP, iss });
  const noExp = await sign(pair, { alg: "RS256", kid: "k1" }, { aud: [AUD], iss });
  const unknownKid = await sign(pair, { alg: "RS256", kid: "k2" }, { aud: [AUD], exp: EXP, iss });
  expect(await verifyAccessJwt(expired, config)).toBe(false);
  expect(await verifyAccessJwt(otherAud, config)).toBe(false);
  expect(await verifyAccessJwt(otherIss, config)).toBe(false);
  expect(await verifyAccessJwt(noneAlg, config)).toBe(false);
  expect(await verifyAccessJwt(noExp, config)).toBe(false);
  expect(await verifyAccessJwt(unknownKid, config)).toBe(false);
});

it("rejects malformed tokens", async () => {
  const config = {
    aud: AUD,
    fetchImpl: vi.fn<typeof fetch>(),
    nowMs: NOW_MS,
    teamDomain: "team-e.cloudflareaccess.com",
  };
  expect(await verifyAccessJwt("only-one-part", config)).toBe(false);
  expect(await verifyAccessJwt("a.b", config)).toBe(false);
  expect(await verifyAccessJwt("!!.!!.!!", config)).toBe(false);
  expect(await verifyAccessJwt(`${encode("[]")}.${encode({})}.x`, config)).toBe(false);
  expect(config.fetchImpl).not.toHaveBeenCalled();
});

it("does not cache a failed certificate fetch", async () => {
  const pair = await keyPair();
  const jwk = await crypto.subtle.exportKey("jwk", pair.publicKey);
  const fetchImpl = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(new Response("down", { status: 503 }))
    .mockResolvedValueOnce(Response.json({ keys: "none" }))
    .mockResolvedValue(Response.json({ keys: [{ ...jwk, kid: "k1" }] }));
  const token = await sign(
    pair,
    { alg: "RS256", kid: "k1" },
    { aud: [AUD], exp: EXP, iss: "https://team-f.cloudflareaccess.com" },
  );
  const config = { aud: AUD, fetchImpl, nowMs: NOW_MS, teamDomain: "team-f.cloudflareaccess.com" };
  expect(await verifyAccessJwt(token, config)).toBe(false);
  expect(await verifyAccessJwt(token, config)).toBe(false);
  expect(await verifyAccessJwt(token, config)).toBe(true);
});
