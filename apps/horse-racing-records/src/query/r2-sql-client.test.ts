// Run with: bun
// Tests for r2-sql-client.ts

import { describe, it, expect, vi, beforeEach } from "vitest";
import { executeR2SqlQuery, buildR2SqlUrl } from "./r2-sql-client.ts";
import type { R2SqlConfig } from "./r2-sql-client.ts";

const TEST_CONFIG: R2SqlConfig = {
  endpoint: "https://api.sql.cloudflarestorage.com/api/v1/accounts",
  accountId: "test-account-id",
  bucketName: "test-bucket",
  apiToken: "test-api-token",
};

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("buildR2SqlUrl", () => {
  it("should build correct URL from config", () => {
    const url = buildR2SqlUrl(TEST_CONFIG);
    expect(url).toStrictEqual(
      "https://api.sql.cloudflarestorage.com/api/v1/accounts/test-account-id/r2-sql/query/test-bucket",
    );
  });
});

describe("executeR2SqlQuery", () => {
  it("should return data on successful query", async () => {
    const mockResponse = {
      success: true,
      result: { rows: [{ id: "1", horse_name: "Deep Impact" }] },
      errors: [],
    };

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(mockResponse),
      }),
    );

    const result = await executeR2SqlQuery(TEST_CONFIG, "SELECT * FROM test.horse_info LIMIT 1");

    expect(result.success).toStrictEqual(true);
    expect(result.data).toStrictEqual([{ id: "1", horse_name: "Deep Impact" }]);
  });

  it("should return error on HTTP failure", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        text: () => Promise.resolve("Internal Server Error"),
      }),
    );

    const result = await executeR2SqlQuery(TEST_CONFIG, "SELECT * FROM test.horse_info");

    expect(result.success).toStrictEqual(false);
    expect(result.data).toStrictEqual([]);
    expect(result.error).toStrictEqual("R2 SQL request failed: 500 Internal Server Error");
  });

  it("should return error on query failure response", async () => {
    const mockResponse = {
      success: false,
      result: [],
      errors: [{ code: 1000, message: "Syntax error in SQL" }],
    };

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(mockResponse),
      }),
    );

    const result = await executeR2SqlQuery(TEST_CONFIG, "INVALID SQL");

    expect(result.success).toStrictEqual(false);
    expect(result.data).toStrictEqual([]);
    expect(result.error).toStrictEqual("R2 SQL query error: Syntax error in SQL");
  });

  it("should send correct headers and body", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ success: true, result: { rows: [] }, errors: [] }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await executeR2SqlQuery(TEST_CONFIG, "SELECT 1");

    expect(fetchMock).toHaveBeenCalledWith(
      "https://api.sql.cloudflarestorage.com/api/v1/accounts/test-account-id/r2-sql/query/test-bucket",
      {
        method: "POST",
        headers: {
          Authorization: "Bearer test-api-token",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ query: "SELECT 1" }),
      },
    );
  });

  it("should handle multiple error messages", async () => {
    const mockResponse = {
      success: false,
      result: [],
      errors: [
        { code: 1000, message: "Error one" },
        { code: 1001, message: "Error two" },
      ],
    };

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(mockResponse),
      }),
    );

    const result = await executeR2SqlQuery(TEST_CONFIG, "BAD SQL");

    expect(result.success).toStrictEqual(false);
    expect(result.error).toStrictEqual("R2 SQL query error: Error one, Error two");
  });
});
