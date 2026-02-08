// Run with: bun
// Tests for types.ts - verifying type exports work correctly

import { describe, it, expect } from "vitest";
import type {
  TableName,
  FilterOperator,
  QueryFilter,
  QueryRequest,
  DeleteRequest,
  R2SqlResponse,
  R2SqlError,
  ApiErrorResponse,
  SchemaField,
} from "./types.ts";

describe("types", () => {
  it("should allow valid TableName values", () => {
    const name1: TableName = "horse_racing_records";
    const name2: TableName = "horse_info";
    const name3: TableName = "race_info";
    expect(name1).toStrictEqual("horse_racing_records");
    expect(name2).toStrictEqual("horse_info");
    expect(name3).toStrictEqual("race_info");
  });

  it("should allow valid FilterOperator values", () => {
    const op1: FilterOperator = "eq";
    const op2: FilterOperator = "neq";
    const op3: FilterOperator = "gt";
    const op4: FilterOperator = "gte";
    const op5: FilterOperator = "lt";
    const op6: FilterOperator = "lte";
    const op7: FilterOperator = "in";
    const op8: FilterOperator = "like";
    expect(op1).toStrictEqual("eq");
    expect(op2).toStrictEqual("neq");
    expect(op3).toStrictEqual("gt");
    expect(op4).toStrictEqual("gte");
    expect(op5).toStrictEqual("lt");
    expect(op6).toStrictEqual("lte");
    expect(op7).toStrictEqual("in");
    expect(op8).toStrictEqual("like");
  });

  it("should create a valid QueryFilter with string value", () => {
    const filter: QueryFilter = {
      column: "horse_name",
      op: "eq",
      value: "Deep Impact",
    };
    expect(filter.column).toStrictEqual("horse_name");
    expect(filter.op).toStrictEqual("eq");
    expect(filter.value).toStrictEqual("Deep Impact");
  });

  it("should create a valid QueryFilter with number value", () => {
    const filter: QueryFilter = {
      column: "race_distance",
      op: "gte",
      value: 2000,
    };
    expect(filter.column).toStrictEqual("race_distance");
    expect(filter.op).toStrictEqual("gte");
    expect(filter.value).toStrictEqual(2000);
  });

  it("should create a valid QueryFilter with array value for IN operator", () => {
    const filter: QueryFilter = {
      column: "id",
      op: "in",
      value: ["id-001", "id-002", "id-003"],
    };
    expect(filter.column).toStrictEqual("id");
    expect(filter.op).toStrictEqual("in");
    expect(filter.value).toStrictEqual(["id-001", "id-002", "id-003"]);
  });

  it("should create a valid QueryRequest", () => {
    const request: QueryRequest = {
      filters: [{ column: "id", op: "eq", value: "test-1" }],
      columns: ["id", "horse_name"],
      limit: 50,
    };
    expect(request.limit).toStrictEqual(50);
    expect(request.columns).toStrictEqual(["id", "horse_name"]);
  });

  it("should create a valid DeleteRequest with filters", () => {
    const request: DeleteRequest = {
      filters: [{ column: "id", op: "eq", value: "test-1" }],
      confirm: true,
    };
    expect(request.confirm).toStrictEqual(true);
    expect(request.filters).toStrictEqual([{ column: "id", op: "eq", value: "test-1" }]);
  });

  it("should create a valid DeleteRequest with ids", () => {
    const request: DeleteRequest = {
      ids: ["id-001", "id-002", "id-003"],
      confirm: true,
    };
    expect(request.confirm).toStrictEqual(true);
    expect(request.ids).toStrictEqual(["id-001", "id-002", "id-003"]);
  });

  it("should define R2SqlResponse structure", () => {
    const response: R2SqlResponse = {
      success: true,
      result: { rows: [{ id: "1", horse_name: "Test Horse" }] },
      errors: [],
    };
    expect(response.success).toStrictEqual(true);
    expect(response.result.rows).toStrictEqual([{ id: "1", horse_name: "Test Horse" }]);
    expect(response.errors).toStrictEqual([]);
  });

  it("should define R2SqlError structure", () => {
    const error: R2SqlError = {
      code: 500,
      message: "Internal error",
    };
    expect(error.code).toStrictEqual(500);
    expect(error.message).toStrictEqual("Internal error");
  });

  it("should define ApiErrorResponse structure", () => {
    const errorResponse: ApiErrorResponse = {
      error: "Not found",
      status: 404,
    };
    expect(errorResponse.error).toStrictEqual("Not found");
    expect(errorResponse.status).toStrictEqual(404);
  });

  it("should define SchemaField structure", () => {
    const field: SchemaField = {
      name: "horse_name",
      type: "string",
      required: true,
    };
    expect(field.name).toStrictEqual("horse_name");
    expect(field.type).toStrictEqual("string");
    expect(field.required).toStrictEqual(true);
  });
});
