// Run with: bun
// Tests for routes/parquet-delete.ts

import { it, expect, vi, beforeEach } from "vitest";
import { Hono } from "hono";
import type { DuckDBAppEnv } from "../types.ts";

vi.mock("../duckdb/connection.ts", () => ({
  createDuckDBInstance: vi.fn(),
}));

vi.mock("../duckdb/parquet-writer.ts", () => ({
  writeDeleteParquet: vi.fn(),
}));

import { parquetDeleteRoute, RECORD_COUNT_HEADER } from "./parquet-delete.ts";
import { createDuckDBInstance } from "../duckdb/connection.ts";
import { writeDeleteParquet } from "../duckdb/parquet-writer.ts";

const setupDuckDBMock = (): void => {
  vi.mocked(createDuckDBInstance).mockResolvedValue({
    connect: () =>
      ({
        query: vi.fn().mockResolvedValue([]),
        execute: vi.fn().mockResolvedValue(0),
        close: vi.fn(),
      }) as never,
    close: vi.fn(),
  });
};

beforeEach(() => {
  vi.restoreAllMocks();
});

it("should return parquet bytes with record count header", async () => {
  setupDuckDBMock();
  vi.mocked(writeDeleteParquet).mockResolvedValue({
    bytes: new Uint8Array([1, 2, 3]),
    recordCount: 2,
  });

  const app = new Hono<DuckDBAppEnv>();
  app.route("/", parquetDeleteRoute);

  const request = new Request("http://localhost/parquet/delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ deleteIds: ["id-001", "id-002"], columnName: "id" }),
  });

  const response = await app.request(request);
  expect(response.status).toStrictEqual(200);
  expect(response.headers.get("Content-Type")).toStrictEqual("application/octet-stream");
  expect(response.headers.get(RECORD_COUNT_HEADER)).toStrictEqual("2");

  const buffer = await response.arrayBuffer();
  expect(new Uint8Array(buffer)).toStrictEqual(new Uint8Array([1, 2, 3]));
});

it("should return 400 when deleteIds is empty", async () => {
  const app = new Hono<DuckDBAppEnv>();
  app.route("/", parquetDeleteRoute);

  const request = new Request("http://localhost/parquet/delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ deleteIds: [], columnName: "id" }),
  });

  const response = await app.request(request);
  expect(response.status).toStrictEqual(400);

  const body = await response.json();
  expect(body).toStrictEqual({
    error: "deleteIds is required and must not be empty",
    status: 400,
  });
});

it("should return 400 when columnName is missing", async () => {
  const app = new Hono<DuckDBAppEnv>();
  app.route("/", parquetDeleteRoute);

  const request = new Request("http://localhost/parquet/delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ deleteIds: ["id-001"], columnName: "" }),
  });

  const response = await app.request(request);
  expect(response.status).toStrictEqual(400);

  const body = await response.json();
  expect(body).toStrictEqual({
    error: "columnName is required",
    status: 400,
  });
});

it("should return 500 when writeDeleteParquet fails", async () => {
  setupDuckDBMock();
  vi.mocked(writeDeleteParquet).mockRejectedValue(
    new Error("Failed to read Parquet file bytes from DuckDB"),
  );

  const app = new Hono<DuckDBAppEnv>();
  app.route("/", parquetDeleteRoute);

  const request = new Request("http://localhost/parquet/delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ deleteIds: ["id-001"], columnName: "id" }),
  });

  const response = await app.request(request);
  expect(response.status).toStrictEqual(500);

  const body = await response.json();
  expect(body).toStrictEqual({
    error: "Failed to read Parquet file bytes from DuckDB",
    status: 500,
  });
});

it("should pass correct args to writeDeleteParquet", async () => {
  setupDuckDBMock();
  vi.mocked(writeDeleteParquet).mockResolvedValue({
    bytes: new Uint8Array([5, 6]),
    recordCount: 3,
  });

  const app = new Hono<DuckDBAppEnv>();
  app.route("/", parquetDeleteRoute);

  const request = new Request("http://localhost/parquet/delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ deleteIds: ["ri-001", "ri-002", "ri-003"], columnName: "id" }),
  });

  const response = await app.request(request);
  expect(response.status).toStrictEqual(200);
  expect(response.headers.get(RECORD_COUNT_HEADER)).toStrictEqual("3");

  expect(vi.mocked(writeDeleteParquet)).toHaveBeenCalledWith({
    conn: expect.anything(),
    deleteIds: ["ri-001", "ri-002", "ri-003"],
    columnName: "id",
  });
});

it("should handle non-Error exceptions", async () => {
  setupDuckDBMock();
  vi.mocked(writeDeleteParquet).mockRejectedValue("string-error");

  const app = new Hono<DuckDBAppEnv>();
  app.route("/", parquetDeleteRoute);

  const request = new Request("http://localhost/parquet/delete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ deleteIds: ["id-001"], columnName: "id" }),
  });

  const response = await app.request(request);
  expect(response.status).toStrictEqual(500);

  const body = await response.json();
  expect(body).toStrictEqual({
    error: "Unknown error creating delete parquet",
    status: 500,
  });
});

it("should have RECORD_COUNT_HEADER as X-Record-Count", () => {
  expect(RECORD_COUNT_HEADER).toStrictEqual("X-Record-Count");
});
