// Run with bun.
import { afterEach, expect, it, vi } from "vitest";
import { authorizationCandidate, isAuthorized, unauthorizedResponse } from "./auth";

const ORIGINAL_ATOB: typeof atob = globalThis.atob;

afterEach(() => {
  vi.restoreAllMocks();
  globalThis.atob = ORIGINAL_ATOB;
});

it("accepts a Bearer token candidate", () => {
  const request = new Request("https://example.test/v1/records", {
    headers: { Authorization: "Bearer secret-value" },
  });

  expect(authorizationCandidate(request)).toBe("secret-value");
});

it("accepts a browser Basic password for the fixed username", () => {
  const request = new Request("https://example.test/bootstrap/vnc.html", {
    headers: { Authorization: `Basic ${btoa("jvlink:secret-value")}` },
  });

  expect(authorizationCandidate(request)).toBe("secret-value");
});

it("rejects a missing authorization header", () => {
  expect(authorizationCandidate(new Request("https://example.test/"))).toBeUndefined();
});

it("rejects an unsupported authorization scheme", () => {
  const request = new Request("https://example.test/", {
    headers: { Authorization: "Digest value" },
  });

  expect(authorizationCandidate(request)).toBeUndefined();
});

it("rejects Basic credentials without a separator", () => {
  const request = new Request("https://example.test/", {
    headers: { Authorization: `Basic ${btoa("jvlink")}` },
  });

  expect(authorizationCandidate(request)).toBeUndefined();
});

it("rejects Basic credentials for another username", () => {
  const request = new Request("https://example.test/", {
    headers: { Authorization: `Basic ${btoa("other:secret-value")}` },
  });

  expect(authorizationCandidate(request)).toBeUndefined();
});

it("rejects malformed Basic base64", () => {
  globalThis.atob = vi.fn(() => {
    throw new DOMException("Invalid character");
  });
  const request = new Request("https://example.test/", {
    headers: { Authorization: "Basic malformed" },
  });

  expect(authorizationCandidate(request)).toBeUndefined();
});

it("does not hide unexpected decoder failures", () => {
  globalThis.atob = vi.fn(() => {
    throw new Error("decoder unavailable");
  });
  const request = new Request("https://example.test/", {
    headers: { Authorization: "Basic malformed" },
  });

  expect(() => authorizationCandidate(request)).toThrow("decoder unavailable");
});

it("authorizes equal non-empty secrets", async () => {
  const request = new Request("https://example.test/", {
    headers: { Authorization: "Bearer secret-value" },
  });

  await expect(isAuthorized(request, "secret-value")).resolves.toBe(true);
});

it("rejects unequal secrets of equal length", async () => {
  const request = new Request("https://example.test/", {
    headers: { Authorization: "Bearer secret-value" },
  });

  await expect(isAuthorized(request, "secret-other")).resolves.toBe(false);
});

it("rejects unequal secrets of different lengths", async () => {
  const request = new Request("https://example.test/", {
    headers: { Authorization: "Bearer short" },
  });

  await expect(isAuthorized(request, "secret-value")).resolves.toBe(false);
});

it("rejects an empty configured secret", async () => {
  const request = new Request("https://example.test/", {
    headers: { Authorization: "Bearer secret-value" },
  });

  await expect(isAuthorized(request, "")).resolves.toBe(false);
});

it("returns a browser-compatible authentication challenge", async () => {
  const response = unauthorizedResponse();

  expect(response.status).toBe(401);
  expect(response.headers.get("WWW-Authenticate")).toBe(
    'Basic realm="JV-Link demo", charset="UTF-8"',
  );
  await expect(response.json()).resolves.toStrictEqual({ error: "Unauthorized" });
});
