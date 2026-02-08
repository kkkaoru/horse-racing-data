// Run with: bun
// Tests for routes/delete.ts

import { describe, it, expect, vi, beforeEach } from "vitest";
import { Hono } from "hono";
import type { AppEnv } from "../types.ts";
import { deleteRoute } from "./delete.ts";

vi.mock("../iceberg/equality-delete.ts", () => ({
  executeEqualityDelete: vi.fn(),
}));

import { executeEqualityDelete } from "../iceberg/equality-delete.ts";

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

describe("POST /tables/:table/delete", () => {
  it("should return 400 for invalid table name", async () => {
    const app = new Hono<AppEnv>();
    app.route("/", deleteRoute);

    const request = new Request("http://localhost/tables/invalid/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [{ column: "id", op: "eq", value: "1" }], confirm: true }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(400);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Invalid table name: invalid",
      status: 400,
    });
  });

  it("should return 400 when confirm is false", async () => {
    const app = new Hono<AppEnv>();
    app.route("/", deleteRoute);

    const request = new Request("http://localhost/tables/horse_info/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [{ column: "id", op: "eq", value: "1" }], confirm: false }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(400);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Delete requires confirm: true",
      status: 400,
    });
  });

  it("should return 400 when both filters and ids are empty", async () => {
    const app = new Hono<AppEnv>();
    app.route("/", deleteRoute);

    const request = new Request("http://localhost/tables/horse_info/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [], ids: [], confirm: true }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(400);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Delete requires ids or at least one filter",
      status: 400,
    });
  });

  it("should return 400 when neither filters nor ids are provided", async () => {
    const app = new Hono<AppEnv>();
    app.route("/", deleteRoute);

    const request = new Request("http://localhost/tables/horse_info/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirm: true }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(400);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Delete requires ids or at least one filter",
      status: 400,
    });
  });

  it("should return success when delete with filters completes", async () => {
    vi.mocked(executeEqualityDelete).mockResolvedValue({
      success: true,
      deletedCount: 5,
    });

    const app = new Hono<AppEnv>();
    app.route("/", deleteRoute);

    const request = new Request("http://localhost/tables/horse_info/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filters: [{ column: "id", op: "eq", value: "hi-001" }],
        confirm: true,
      }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ success: true, deletedCount: 5 });
  });

  it("should return success when delete with ids completes", async () => {
    vi.mocked(executeEqualityDelete).mockResolvedValue({
      success: true,
      deletedCount: 3,
    });

    const app = new Hono<AppEnv>();
    app.route("/", deleteRoute);

    const request = new Request("http://localhost/tables/horse_info/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ids: ["hi-001", "hi-002", "hi-003"],
        confirm: true,
      }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ success: true, deletedCount: 3 });

    expect(vi.mocked(executeEqualityDelete)).toHaveBeenCalledWith({
      env: createMockEnv(),
      table: "horse_info",
      filters: undefined,
      ids: ["hi-001", "hi-002", "hi-003"],
    });
  });

  it("should return 500 when delete fails", async () => {
    vi.mocked(executeEqualityDelete).mockResolvedValue({
      success: false,
      deletedCount: 0,
      error: "Commit failed",
    });

    const app = new Hono<AppEnv>();
    app.route("/", deleteRoute);

    const request = new Request("http://localhost/tables/horse_info/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filters: [{ column: "id", op: "eq", value: "hi-001" }],
        confirm: true,
      }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(500);

    const body = await response.json();
    expect(body).toStrictEqual({
      error: "Commit failed",
      status: 500,
    });
  });

  it("should return success with 0 count when no rows match", async () => {
    vi.mocked(executeEqualityDelete).mockResolvedValue({
      success: true,
      deletedCount: 0,
    });

    const app = new Hono<AppEnv>();
    app.route("/", deleteRoute);

    const request = new Request("http://localhost/tables/horse_info/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filters: [{ column: "id", op: "eq", value: "nonexistent" }],
        confirm: true,
      }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ success: true, deletedCount: 0 });
  });
});
