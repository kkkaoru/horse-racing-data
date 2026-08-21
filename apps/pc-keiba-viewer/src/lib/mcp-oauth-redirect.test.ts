// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { isAllowedOAuthRedirectUri, redirectUrisInclude } from "./mcp-oauth-redirect";

it("allows https and loopback http redirect URIs", () => {
  expect(isAllowedOAuthRedirectUri("https://agents.example.test/callback")).toBe(true);
  expect(isAllowedOAuthRedirectUri("http://127.0.0.1:8787/callback")).toBe(true);
  expect(isAllowedOAuthRedirectUri("http://localhost:8787/callback")).toBe(true);
});

it("rejects fragments, non-loopback http, and invalid URLs", () => {
  expect(isAllowedOAuthRedirectUri("https://agents.example.test/callback#x")).toBe(false);
  expect(isAllowedOAuthRedirectUri("http://evil.example.test/callback")).toBe(false);
  expect(isAllowedOAuthRedirectUri("not-a-url")).toBe(false);
  expect(isAllowedOAuthRedirectUri("ftp://127.0.0.1/callback")).toBe(false);
  expect(isAllowedOAuthRedirectUri("http://[::1]/callback")).toBe(false);
  expect(isAllowedOAuthRedirectUri("https://")).toBe(false);
});

it("allows https URIs that include a query string", () => {
  expect(isAllowedOAuthRedirectUri("https://agents.example.test/callback?x=1")).toBe(true);
});

it("matches redirect URIs exactly", () => {
  expect(redirectUrisInclude(["http://127.0.0.1/cb"], "http://127.0.0.1/cb")).toBe(true);
  expect(redirectUrisInclude(["http://127.0.0.1/cb"], "http://127.0.0.1/other")).toBe(false);
});
