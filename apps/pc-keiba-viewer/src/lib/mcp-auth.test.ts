// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  isMcpAuthorized,
  parseBearerToken,
  secretsEqual,
  mcpUnauthorizedResponse,
  timingSafeEqualBytes,
} from "./mcp-auth";

it("parseBearerToken rejects missing, short, basic, and empty tokens", () => {
  expect(parseBearerToken(null)).toBe(null);
  expect(parseBearerToken("Bear")).toBe(null);
  expect(parseBearerToken("Basic abc")).toBe(null);
  expect(parseBearerToken("Bearer    ")).toBe(null);
});

it("parseBearerToken accepts a case-insensitive Bearer prefix", () => {
  expect(parseBearerToken("bearer secret-token")).toBe("secret-token");
});

it("secretsEqual distinguishes equal and unequal strings", () => {
  expect(secretsEqual("abc", "abc")).toBe(true);
  expect(secretsEqual("abc", "abd")).toBe(false);
  expect(secretsEqual("ab", "abc")).toBe(false);
});

it("timingSafeEqualBytes rejects mismatched lengths and bytes", () => {
  expect(timingSafeEqualBytes(new Uint8Array([1, 2]), new Uint8Array([1, 2]))).toBe(true);
  expect(timingSafeEqualBytes(new Uint8Array([1, 2]), new Uint8Array([1, 3]))).toBe(false);
  expect(timingSafeEqualBytes(new Uint8Array([1]), new Uint8Array([1, 2]))).toBe(false);
});

it("isMcpAuthorized rejects an empty configured token", () => {
  const request = new Request("https://viewer.example.test/mcp", {
    headers: { Authorization: "Bearer secret-token" },
  });
  expect(isMcpAuthorized(request, "")).toBe(false);
});

it("isMcpAuthorized rejects a request without a bearer token", () => {
  expect(isMcpAuthorized(new Request("https://viewer.example.test/mcp"), "secret-token")).toBe(
    false,
  );
});

it("isMcpAuthorized accepts a matching bearer token", () => {
  const request = new Request("https://viewer.example.test/mcp", {
    headers: { Authorization: "Bearer secret-token" },
  });
  expect(isMcpAuthorized(request, "secret-token")).toBe(true);
});

it("mcpUnauthorizedResponse challenges with Bearer and resource metadata", async () => {
  const response = mcpUnauthorizedResponse("https://viewer.example.test");
  expect(response.status).toBe(401);
  expect(response.headers.get("WWW-Authenticate")).toBe(
    'Bearer realm="pc-keiba-viewer-mcp", resource_metadata="https://viewer.example.test/.well-known/oauth-protected-resource", scope="mcp"',
  );
  expect(response.headers.get("Access-Control-Allow-Origin")).toBe("*");
  expect(response.headers.get("Access-Control-Expose-Headers")).toBe("WWW-Authenticate");
  await expect(response.text()).resolves.toBe("Unauthorized");
});
