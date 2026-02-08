// Run with: bun
// Tests for duckdb/parquet-writer.ts

import { describe, it, expect, vi } from "vitest";
import { writeDeleteParquet, DELETE_PARQUET_PATH, escapeId } from "./parquet-writer.ts";
import type { Connection } from "@ducklings/workers";

describe("escapeId", () => {
  it("should escape single quotes", () => {
    expect(escapeId("O'Brien")).toStrictEqual("O''Brien");
  });

  it("should return string without quotes unchanged", () => {
    expect(escapeId("simple-id")).toStrictEqual("simple-id");
  });

  it("should escape multiple single quotes", () => {
    expect(escapeId("it's a test's id")).toStrictEqual("it''s a test''s id");
  });
});

describe("DELETE_PARQUET_PATH", () => {
  it("should be /tmp/delete.parquet", () => {
    expect(DELETE_PARQUET_PATH).toStrictEqual("/tmp/delete.parquet");
  });
});

describe("writeDeleteParquet", () => {
  it("should create table, insert values, and return bytes", async () => {
    const mockBytes = new Uint8Array([1, 2, 3, 4]);
    const executeMock = vi.fn().mockResolvedValue(0);
    const queryMock = vi.fn().mockResolvedValue([{ content: mockBytes }]);
    const closeMock = vi.fn();

    const mockConn = {
      execute: executeMock,
      query: queryMock,
      close: closeMock,
    } as unknown as Connection;

    const result = await writeDeleteParquet({
      conn: mockConn,
      deleteIds: ["id-001", "id-002"],
      columnName: "id",
    });

    expect(executeMock).toHaveBeenCalledWith("CREATE TABLE delete_ids (id VARCHAR)");
    expect(executeMock).toHaveBeenCalledWith(
      "INSERT INTO delete_ids VALUES ('id-001'), ('id-002')",
    );
    expect(executeMock).toHaveBeenCalledWith(
      "COPY delete_ids TO '/tmp/delete.parquet' (FORMAT PARQUET)",
    );
    expect(queryMock).toHaveBeenCalledWith("SELECT content FROM read_blob('/tmp/delete.parquet')");
    expect(result.bytes).toStrictEqual(new Uint8Array([1, 2, 3, 4]));
    expect(result.recordCount).toStrictEqual(2);
  });

  it("should throw when no result returned", async () => {
    const mockConn = {
      execute: vi.fn().mockResolvedValue(0),
      query: vi.fn().mockResolvedValue([]),
      close: vi.fn(),
    } as unknown as Connection;

    await expect(
      writeDeleteParquet({
        conn: mockConn,
        deleteIds: ["id-001"],
        columnName: "id",
      }),
    ).rejects.toThrow("Failed to read Parquet file bytes from DuckDB");
  });

  it("should escape single quotes in IDs", async () => {
    const mockBytes = new Uint8Array([5, 6]);
    const executeMock = vi.fn().mockResolvedValue(0);
    const mockConn = {
      execute: executeMock,
      query: vi.fn().mockResolvedValue([{ content: mockBytes }]),
      close: vi.fn(),
    } as unknown as Connection;

    await writeDeleteParquet({
      conn: mockConn,
      deleteIds: ["O'Brien"],
      columnName: "id",
    });

    expect(executeMock).toHaveBeenCalledWith("INSERT INTO delete_ids VALUES ('O''Brien')");
  });

  it("should handle single ID", async () => {
    const mockBytes = new Uint8Array([7]);
    const executeMock = vi.fn().mockResolvedValue(0);
    const mockConn = {
      execute: executeMock,
      query: vi.fn().mockResolvedValue([{ content: mockBytes }]),
      close: vi.fn(),
    } as unknown as Connection;

    const result = await writeDeleteParquet({
      conn: mockConn,
      deleteIds: ["single-id"],
      columnName: "id",
    });

    expect(result.recordCount).toStrictEqual(1);
    expect(executeMock).toHaveBeenCalledWith("INSERT INTO delete_ids VALUES ('single-id')");
  });
});
