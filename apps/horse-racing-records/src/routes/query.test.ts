// Run with: bun
// Tests for routes/query.ts

import { describe, it, expect, vi, beforeEach } from "vitest";
import { Hono } from "hono";
import type { AppEnv } from "../types.ts";
import { queryRoute } from "./query.ts";

const createMockEnv = (): AppEnv["Bindings"] => ({
  R2_BUCKET: {} as R2Bucket,
  CLOUDFLARE_API_TOKEN: "test-token",
  CLOUDFLARE_ACCOUNT_ID: "test-account",
  R2_BUCKET_NAME: "test-bucket",
  ICEBERG_NAMESPACE: "horse_racing",
  CATALOG_URI: "https://catalog.example.com",
  R2_SQL_ENDPOINT: "https://api.sql.cloudflarestorage.com/api/v1/accounts",
  R2_ACCESS_KEY_ID: "test-key",
  R2_SECRET_ACCESS_KEY: "test-secret",
  SKIP_MTLS: "1",
});

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("POST /tables/:table/query", () => {
  it("should return 400 for invalid table name", async () => {
    const app = new Hono<AppEnv>();
    app.route("/", queryRoute);

    const request = new Request("http://localhost/tables/invalid_table/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [] }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(400);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Invalid table name: invalid_table",
      status: 400,
    });
  });

  it("should return query results on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: { rows: [{ id: "1", horse_name: "Deep Impact" }] },
            errors: [],
          }),
      }),
    );

    const app = new Hono<AppEnv>();
    app.route("/", queryRoute);

    const request = new Request("http://localhost/tables/horse_info/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [], limit: 10 }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({
      data: [{ id: "1", horse_name: "Deep Impact" }],
      count: 1,
    });
  });

  it("should return 400 for invalid column in filters", async () => {
    const app = new Hono<AppEnv>();
    app.route("/", queryRoute);

    const request = new Request("http://localhost/tables/horse_info/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filters: [{ column: "nonexistent", op: "eq", value: "test" }],
      }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(400);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Invalid column name: nonexistent",
      status: 400,
    });
  });

  it("should return 500 when R2 SQL query fails", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        text: () => Promise.resolve("Server Error"),
      }),
    );

    const app = new Hono<AppEnv>();
    app.route("/", queryRoute);

    const request = new Request("http://localhost/tables/horse_info/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [] }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(500);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "R2 SQL request failed: 500 Server Error",
      status: 500,
    });
  });

  it("should handle query with specific columns", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: { rows: [{ id: "1" }] },
            errors: [],
          }),
      }),
    );

    const app = new Hono<AppEnv>();
    app.route("/", queryRoute);

    const request = new Request("http://localhost/tables/horse_info/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [], columns: ["id"] }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ data: [{ id: "1" }], count: 1 });
  });

  it("should handle race_info table", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: { rows: [] },
            errors: [],
          }),
      }),
    );

    const app = new Hono<AppEnv>();
    app.route("/", queryRoute);

    const request = new Request("http://localhost/tables/race_info/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [] }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ data: [], count: 0 });
  });
});
