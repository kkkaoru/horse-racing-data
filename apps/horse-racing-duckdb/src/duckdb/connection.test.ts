// Run with: bun
// Tests for duckdb/connection.ts

import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("@ducklings/workers", () => {
  const mockClose = vi.fn();
  const mockConnect = vi.fn(() => ({
    query: vi.fn().mockResolvedValue([]),
    execute: vi.fn().mockResolvedValue(0),
    close: vi.fn(),
  }));

  return {
    init: vi.fn().mockResolvedValue(undefined),
    DuckDB: vi.fn(() => ({
      connect: mockConnect,
      close: mockClose,
    })),
  };
});

vi.mock("./duckdb-workers.wasm", () => ({
  default: {} as WebAssembly.Module,
}));

import { createDuckDBInstance } from "./connection.ts";

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("createDuckDBInstance", () => {
  it("should create a DuckDB instance", async () => {
    const instance = await createDuckDBInstance();
    expect(instance).toBeDefined();
    expect(typeof instance.connect).toStrictEqual("function");
    expect(typeof instance.close).toStrictEqual("function");
  });

  it("should return a connection with query and execute methods", async () => {
    const instance = await createDuckDBInstance();
    const conn = instance.connect();
    expect(typeof conn.query).toStrictEqual("function");
    expect(typeof conn.execute).toStrictEqual("function");
    expect(typeof conn.close).toStrictEqual("function");
  });

  it("should close the database", async () => {
    const instance = await createDuckDBInstance();
    instance.close();
    // No error thrown = success
    expect(true).toStrictEqual(true);
  });
});
