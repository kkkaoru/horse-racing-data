// Run with: bun
// Tests for mtls.ts middleware

import { describe, it, expect } from "vitest";
import { Hono } from "hono";
import type { AppEnv } from "../types.ts";
import { mtlsMiddleware, isMtlsSkipped, isCertValid } from "./mtls.ts";
import type { TlsClientAuth } from "./mtls.ts";

const createTestApp = (): Hono<AppEnv> => {
  const app = new Hono<AppEnv>();
  app.use("*", mtlsMiddleware);
  app.get("/test", (c) => c.json({ status: "ok" }));
  return app;
};

const createMockEnv = (skipMtls?: string): AppEnv["Bindings"] => ({
  R2_BUCKET: {} as R2Bucket,
  CLOUDFLARE_API_TOKEN: "test-token",
  CLOUDFLARE_ACCOUNT_ID: "test-account",
  R2_BUCKET_NAME: "test-bucket",
  ICEBERG_NAMESPACE: "test",
  CATALOG_URI: "https://catalog.example.com",
  R2_SQL_ENDPOINT: "https://sql.example.com",
  R2_ACCESS_KEY_ID: "test-key",
  R2_SECRET_ACCESS_KEY: "test-secret",
  SKIP_MTLS: skipMtls,
});

describe("isMtlsSkipped", () => {
  it("should return true when SKIP_MTLS is 1", () => {
    const env = createMockEnv("1");
    expect(isMtlsSkipped(env)).toStrictEqual(true);
  });

  it("should return false when SKIP_MTLS is undefined", () => {
    const env = createMockEnv(undefined);
    expect(isMtlsSkipped(env)).toStrictEqual(false);
  });

  it("should return false when SKIP_MTLS is 0", () => {
    const env = createMockEnv("0");
    expect(isMtlsSkipped(env)).toStrictEqual(false);
  });
});

describe("isCertValid", () => {
  it("should return true when cert is presented and verified", () => {
    const auth: TlsClientAuth = { certPresented: "1", certVerified: "SUCCESS" };
    expect(isCertValid(auth)).toStrictEqual(true);
  });

  it("should return false when cert is not presented", () => {
    const auth: TlsClientAuth = { certPresented: "0", certVerified: "SUCCESS" };
    expect(isCertValid(auth)).toStrictEqual(false);
  });

  it("should return false when cert is not verified", () => {
    const auth: TlsClientAuth = { certPresented: "1", certVerified: "FAILED" };
    expect(isCertValid(auth)).toStrictEqual(false);
  });

  it("should return false when cert has empty values", () => {
    const auth: TlsClientAuth = { certPresented: "", certVerified: "" };
    expect(isCertValid(auth)).toStrictEqual(false);
  });
});

describe("mtlsMiddleware", () => {
  it("should skip mTLS when SKIP_MTLS is 1", async () => {
    const app = createTestApp();
    const request = new Request("http://localhost/test");
    const env = createMockEnv("1");

    const response = await app.request(request, undefined, env);
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ status: "ok" });
  });

  it("should return 403 when no TLS client auth", async () => {
    const app = createTestApp();
    const request = new Request("http://localhost/test");
    const env = createMockEnv(undefined);

    const response = await app.request(request, undefined, env);
    expect(response.status).toStrictEqual(403);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Forbidden: valid mTLS client certificate required",
      status: 403,
    });
  });
});
