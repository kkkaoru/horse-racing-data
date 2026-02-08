// Run with: bun
// Tests for app.ts entry point

import { describe, it, expect, vi, beforeEach } from "vitest";
import app from "./app.ts";
import type { AppEnv } from "./types.ts";

vi.mock("./iceberg/equality-delete.ts", () => ({
  executeEqualityDelete: vi.fn(),
}));

import { executeEqualityDelete } from "./iceberg/equality-delete.ts";

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

describe("app routes", () => {
  it("should respond to GET /health", async () => {
    const request = new Request("http://localhost/health");
    const response = await app.request(request, undefined, createMockEnv());

    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ status: "ok" });
  });

  it("should respond to POST /tables/horse_info/query with mocked fetch", async () => {
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

    const request = new Request("http://localhost/tables/horse_info/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filters: [] }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ data: [{ id: "1" }], count: 1 });
  });

  it("should return 404 for unknown routes", async () => {
    const request = new Request("http://localhost/unknown");
    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(404);
  });

  it("should handle POST to delete endpoint", async () => {
    vi.mocked(executeEqualityDelete).mockResolvedValue({ success: true, deletedCount: 0 });

    const request = new Request("http://localhost/tables/horse_info/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filters: [{ column: "id", op: "eq", value: "test-1" }],
        confirm: true,
      }),
    });

    const response = await app.request(request, undefined, createMockEnv());
    expect(response.status).toStrictEqual(200);

    const body = await response.json();
    expect(body).toStrictEqual({ success: true, deletedCount: 0 });
  });
});
