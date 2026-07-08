// Run with: bun run --filter mlflow-ui-proxy test
import { expect, it } from "vitest";

import { isAuthorized, parseBasicAuth, timingSafeEqualBytes, unauthorizedResponse } from "./auth";
import type { Env } from "./types";

const TEST_ENV: Env = {
  MLFLOW_ORIGIN: "https://mlflow-origin.test",
  MLFLOW_UI_USERNAME: "operator",
  MLFLOW_UI_PASSWORD: "s3cret",
};

const buildBasicAuthHeaderValue = (username: string, password: string): string =>
  `Basic ${btoa(`${username}:${password}`)}`;

const buildRequestWithAuthHeader = (headerValue: string | null): Request => {
  const headers = new Headers();
  if (headerValue !== null) {
    headers.set("Authorization", headerValue);
  }
  return new Request("https://worker.test/", { headers });
};

it("parseBasicAuth parses a valid header into username and password", () => {
  const parsed = parseBasicAuth(buildBasicAuthHeaderValue("operator", "s3cret"));
  expect(parsed).toStrictEqual({ username: "operator", password: "s3cret" });
});

it("parseBasicAuth returns null when the header is missing", () => {
  expect(parseBasicAuth(null)).toBe(null);
});

it("parseBasicAuth returns null when the scheme is not Basic", () => {
  expect(parseBasicAuth("Bearer abc123")).toBe(null);
});

it("parseBasicAuth returns null when the base64 payload is malformed", () => {
  expect(parseBasicAuth("Basic ###not-base64###")).toBe(null);
});

it("parseBasicAuth returns null when the decoded payload has no colon separator", () => {
  expect(parseBasicAuth(`Basic ${btoa("no-colon-here")}`)).toBe(null);
});

it("timingSafeEqualBytes returns true for equal byte arrays", () => {
  const a = new Uint8Array([1, 2, 3, 4]);
  const b = new Uint8Array([1, 2, 3, 4]);
  expect(timingSafeEqualBytes(a, b)).toBe(true);
});

it("timingSafeEqualBytes returns false for byte arrays that differ in content", () => {
  const a = new Uint8Array([1, 2, 3, 4]);
  const b = new Uint8Array([1, 2, 3, 5]);
  expect(timingSafeEqualBytes(a, b)).toBe(false);
});

it("timingSafeEqualBytes returns false for byte arrays that differ in length", () => {
  const a = new Uint8Array([1, 2, 3]);
  const b = new Uint8Array([1, 2, 3, 4]);
  expect(timingSafeEqualBytes(a, b)).toBe(false);
});

it("isAuthorized returns true for the correct username and password", async () => {
  const request = buildRequestWithAuthHeader(buildBasicAuthHeaderValue("operator", "s3cret"));
  await expect(isAuthorized(request, TEST_ENV)).resolves.toBe(true);
});

it("isAuthorized returns false for a wrong username", async () => {
  const request = buildRequestWithAuthHeader(buildBasicAuthHeaderValue("intruder", "s3cret"));
  await expect(isAuthorized(request, TEST_ENV)).resolves.toBe(false);
});

it("isAuthorized returns false for a wrong password", async () => {
  const request = buildRequestWithAuthHeader(buildBasicAuthHeaderValue("operator", "wrong"));
  await expect(isAuthorized(request, TEST_ENV)).resolves.toBe(false);
});

it("isAuthorized returns false when the Authorization header is missing", async () => {
  const request = buildRequestWithAuthHeader(null);
  await expect(isAuthorized(request, TEST_ENV)).resolves.toBe(false);
});

it("unauthorizedResponse returns a 401 with the expected WWW-Authenticate header", () => {
  const response = unauthorizedResponse();
  expect(response.status).toBe(401);
  expect(response.headers.get("WWW-Authenticate")).toBe('Basic realm="MLflow UI", charset="UTF-8"');
});
