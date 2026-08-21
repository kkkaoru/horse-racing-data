// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { isS256Method, sha256Base64Url, toBase64Url, verifyPkceS256 } from "./mcp-oauth-pkce";

it("accepts only S256 as the PKCE method", () => {
  expect(isS256Method("S256")).toBe(true);
  expect(isS256Method("plain")).toBe(false);
  expect(isS256Method(null)).toBe(false);
});

it("verifies an S256 code verifier against its challenge", async () => {
  const verifier = "a".repeat(43);
  const challenge = await sha256Base64Url(verifier);
  expect(await verifyPkceS256(verifier, challenge)).toBe(true);
  expect(await verifyPkceS256(verifier, "other")).toBe(false);
  expect(await verifyPkceS256("short", challenge)).toBe(false);
  expect(await verifyPkceS256(verifier, "")).toBe(false);
  expect(await verifyPkceS256("a".repeat(42), challenge)).toBe(false);
  expect(await verifyPkceS256("a".repeat(129), challenge)).toBe(false);
  const longVerifier = "b".repeat(128);
  const longChallenge = await sha256Base64Url(longVerifier);
  expect(await verifyPkceS256(longVerifier, longChallenge)).toBe(true);
});

it("encodes bytes as unpadded base64url", () => {
  expect(toBase64Url(new Uint8Array([251, 255, 191]))).toBe("-_-_");
  expect(toBase64Url(new Uint8Array([0]))).toBe("AA");
  expect(toBase64Url(new Uint8Array([255]))).toBe("_w");
});
