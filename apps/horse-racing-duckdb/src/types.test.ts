// Run with: bun
// Tests for types.ts

import { it, expect } from "vitest";
import type { DuckDBBindings, DuckDBAppEnv, CreateDeleteParquetRequest } from "./types.ts";

it("should define DuckDBBindings structure", () => {
  const bindings: DuckDBBindings = {};
  expect(bindings).toStrictEqual({});
});

it("should define DuckDBAppEnv structure", () => {
  const env: DuckDBAppEnv = { Bindings: {} };
  expect(env.Bindings).toStrictEqual({});
});

it("should define CreateDeleteParquetRequest structure", () => {
  const request: CreateDeleteParquetRequest = {
    deleteIds: ["id-001", "id-002"],
    columnName: "id",
  };
  expect(request.deleteIds).toStrictEqual(["id-001", "id-002"]);
  expect(request.columnName).toStrictEqual("id");
});
