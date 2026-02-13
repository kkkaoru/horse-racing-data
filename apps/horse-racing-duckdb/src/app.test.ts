// Run with: bun
// Tests for app.ts entry point

import { it, expect, vi, beforeEach } from "vitest";
import app from "./app.ts";

vi.mock("./duckdb/connection.ts", () => ({
  createDuckDBInstance: vi.fn(),
}));

vi.mock("./duckdb/parquet-writer.ts", () => ({
  writeDeleteParquet: vi.fn(),
}));

import { createDuckDBInstance } from "./duckdb/connection.ts";
import { writeDeleteParquet } from "./duckdb/parquet-writer.ts";

beforeEach(() => {
  vi.restoreAllMocks();
});

it("should respond to GET /health", async () => {
  const request = new Request("http://localhost/health");
  const response = await app.request(request);

  expect(response.status).toStrictEqual(200);

  const body = await response.json();
  expect(body).toStrictEqual({ status: "ok" });
});

it("should respond to POST /parquet/delete", async () => {
  vi.mocked(createDuckDBInstance).mockResolvedValue({
    connect: () =>
      ({
        query: vi.fn().mockResolvedValue([]),
        execute: vi.fn().mockResolvedValue(0),
        close: vi.fn(),
      }) as never,
    close: vi.fn(),
  });
  vi.mocked(writeDeleteParquet).mockResolvedValue({
    bytes: new Uint8Array([1, 2, 3]),
    recordCount: 2,
  });

  const request = new Request("http://localhost/parquet/delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ deleteIds: ["id-001", "id-002"], columnName: "id" }),
  });

  const response = await app.request(request);
  expect(response.status).toStrictEqual(200);
  expect(response.headers.get("X-Record-Count")).toStrictEqual("2");
});

it("should return 404 for unknown routes", async () => {
  const request = new Request("http://localhost/unknown");
  const response = await app.request(request);
  expect(response.status).toStrictEqual(404);
});
