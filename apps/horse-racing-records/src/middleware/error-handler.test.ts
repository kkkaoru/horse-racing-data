// Run with: bun
// Tests for error-handler.ts middleware

import { describe, it, expect } from "vitest";
import { Hono } from "hono";
import type { AppEnv } from "../types.ts";
import { errorHandler } from "./error-handler.ts";

const createMockEnv = (): AppEnv["Bindings"] => ({
  R2_BUCKET: {} as R2Bucket,
  CLOUDFLARE_API_TOKEN: "test-token",
  CLOUDFLARE_ACCOUNT_ID: "test-account",
  R2_BUCKET_NAME: "test-bucket",
  ICEBERG_NAMESPACE: "test",
  CATALOG_URI: "https://catalog.example.com",
  R2_SQL_ENDPOINT: "https://sql.example.com",
  R2_ACCESS_KEY_ID: "test-key",
  R2_SECRET_ACCESS_KEY: "test-secret",
  SKIP_MTLS: "1",
});

describe("errorHandler", () => {
  it("should return 500 JSON error for unhandled exceptions", async () => {
    const app = new Hono<AppEnv>();
    app.onError(errorHandler);
    app.get("/error", () => {
      throw new Error("Test error");
    });

    const request = new Request("http://localhost/error");
    const response = await app.request(request, undefined, createMockEnv());

    expect(response.status).toStrictEqual(500);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Internal server error",
      status: 500,
    });
  });

  it("should handle error without stack trace", async () => {
    const app = new Hono<AppEnv>();
    app.onError(errorHandler);
    app.get("/no-stack", () => {
      const err = new Error("No stack");
      err.stack = undefined;
      throw err;
    });

    const request = new Request("http://localhost/no-stack");
    const response = await app.request(request, undefined, createMockEnv());

    expect(response.status).toStrictEqual(500);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Internal server error",
      status: 500,
    });
  });

  it("should handle TypeError", async () => {
    const app = new Hono<AppEnv>();
    app.onError(errorHandler);
    app.get("/type-error", () => {
      throw new TypeError("Type mismatch");
    });

    const request = new Request("http://localhost/type-error");
    const response = await app.request(request, undefined, createMockEnv());

    expect(response.status).toStrictEqual(500);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Internal server error",
      status: 500,
    });
  });
});
