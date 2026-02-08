// Run with: bun
// Tests for sql-builder.ts

import { describe, it, expect } from "vitest";
import {
  buildSelectQuery,
  buildWhereClause,
  escapeStringValue,
  formatValue,
  formatInValues,
  DEFAULT_QUERY_LIMIT,
  MAX_QUERY_LIMIT,
} from "./sql-builder.ts";

describe("escapeStringValue", () => {
  it("should escape single quotes", () => {
    expect(escapeStringValue("O'Brien")).toStrictEqual("O''Brien");
  });

  it("should return string without quotes unchanged", () => {
    expect(escapeStringValue("Deep Impact")).toStrictEqual("Deep Impact");
  });

  it("should escape multiple single quotes", () => {
    expect(escapeStringValue("it's a horse's name")).toStrictEqual("it''s a horse''s name");
  });
});

describe("formatValue", () => {
  it("should format number as string", () => {
    expect(formatValue(2000)).toStrictEqual("2000");
  });

  it("should format string with single quotes", () => {
    expect(formatValue("Deep Impact")).toStrictEqual("'Deep Impact'");
  });

  it("should escape single quotes in string values", () => {
    expect(formatValue("O'Brien")).toStrictEqual("'O''Brien'");
  });
});

describe("formatInValues", () => {
  it("should format string array", () => {
    expect(formatInValues(["a", "b", "c"])).toStrictEqual("('a', 'b', 'c')");
  });

  it("should format number array", () => {
    expect(formatInValues([1, 2, 3])).toStrictEqual("(1, 2, 3)");
  });

  it("should format mixed array", () => {
    expect(formatInValues(["a", 1])).toStrictEqual("('a', 1)");
  });
});

describe("buildWhereClause", () => {
  it("should return empty string for empty filters", () => {
    expect(buildWhereClause("horse_info", [])).toStrictEqual("");
  });

  it("should build WHERE clause for eq filter", () => {
    const result = buildWhereClause("horse_info", [{ column: "id", op: "eq", value: "test-1" }]);
    expect(result).toStrictEqual(" WHERE id = 'test-1'");
  });

  it("should build WHERE clause for numeric eq filter", () => {
    const result = buildWhereClause("horse_info", [
      { column: "race_distance", op: "eq", value: 2000 },
    ]);
    expect(result).toStrictEqual(" WHERE race_distance = 2000");
  });

  it("should build WHERE clause for gte filter", () => {
    const result = buildWhereClause("horse_info", [
      { column: "race_distance", op: "gte", value: 1600 },
    ]);
    expect(result).toStrictEqual(" WHERE race_distance >= 1600");
  });

  it("should build WHERE clause for IN filter", () => {
    const result = buildWhereClause("horse_info", [
      { column: "id", op: "in", value: ["id-1", "id-2"] },
    ]);
    expect(result).toStrictEqual(" WHERE id IN ('id-1', 'id-2')");
  });

  it("should build WHERE clause for LIKE filter", () => {
    const result = buildWhereClause("horse_info", [
      { column: "horse_name", op: "like", value: "%Impact%" },
    ]);
    expect(result).toStrictEqual(" WHERE horse_name LIKE '%Impact%'");
  });

  it("should combine multiple filters with AND", () => {
    const result = buildWhereClause("horse_info", [
      { column: "race_course", op: "eq", value: "Tokyo" },
      { column: "race_distance", op: "gte", value: 2000 },
    ]);
    expect(result).toStrictEqual(" WHERE race_course = 'Tokyo' AND race_distance >= 2000");
  });

  it("should throw for invalid column", () => {
    expect(() =>
      buildWhereClause("horse_info", [{ column: "invalid_column", op: "eq", value: "test" }]),
    ).toThrow("Invalid column name: invalid_column");
  });

  it("should throw for invalid operator", () => {
    expect(() =>
      buildWhereClause("horse_info", [{ column: "id", op: "invalid" as "eq", value: "test" }]),
    ).toThrow("Invalid operator: invalid");
  });

  it("should throw when IN operator gets non-array value", () => {
    expect(() =>
      buildWhereClause("horse_info", [{ column: "id", op: "in", value: "single-value" }]),
    ).toThrow("IN operator requires an array value for column: id");
  });

  it("should throw when non-IN operator gets array value", () => {
    expect(() =>
      buildWhereClause("horse_info", [{ column: "id", op: "eq", value: ["a", "b"] }]),
    ).toThrow("Non-IN operator does not accept array value for column: id");
  });
});

describe("buildSelectQuery", () => {
  it("should build SELECT * query with no filters", () => {
    const result = buildSelectQuery({
      table: "horse_info",
      namespace: "horse_racing",
      filters: [],
    });
    expect(result).toStrictEqual("SELECT * FROM horse_racing.horse_info LIMIT 100");
  });

  it("should use default limit", () => {
    const result = buildSelectQuery({
      table: "horse_info",
      namespace: "horse_racing",
      filters: [],
    });
    expect(result).toStrictEqual(
      `SELECT * FROM horse_racing.horse_info LIMIT ${String(DEFAULT_QUERY_LIMIT)}`,
    );
  });

  it("should use custom limit", () => {
    const result = buildSelectQuery({
      table: "horse_info",
      namespace: "horse_racing",
      filters: [],
      limit: 50,
    });
    expect(result).toStrictEqual("SELECT * FROM horse_racing.horse_info LIMIT 50");
  });

  it("should cap limit at MAX_QUERY_LIMIT", () => {
    const result = buildSelectQuery({
      table: "horse_info",
      namespace: "horse_racing",
      filters: [],
      limit: 999999,
    });
    expect(result).toStrictEqual(
      `SELECT * FROM horse_racing.horse_info LIMIT ${String(MAX_QUERY_LIMIT)}`,
    );
  });

  it("should select specific columns", () => {
    const result = buildSelectQuery({
      table: "horse_info",
      namespace: "horse_racing",
      filters: [],
      columns: ["id", "horse_name"],
    });
    expect(result).toStrictEqual("SELECT id, horse_name FROM horse_racing.horse_info LIMIT 100");
  });

  it("should include WHERE clause from filters", () => {
    const result = buildSelectQuery({
      table: "horse_info",
      namespace: "horse_racing",
      filters: [{ column: "id", op: "eq", value: "test-1" }],
    });
    expect(result).toStrictEqual(
      "SELECT * FROM horse_racing.horse_info WHERE id = 'test-1' LIMIT 100",
    );
  });

  it("should throw for invalid column in select list", () => {
    expect(() =>
      buildSelectQuery({
        table: "horse_info",
        namespace: "horse_racing",
        filters: [],
        columns: ["invalid_column"],
      }),
    ).toThrow("Invalid column name: invalid_column");
  });

  it("should build query for race_info table", () => {
    const result = buildSelectQuery({
      table: "race_info",
      namespace: "horse_racing",
      filters: [{ column: "race_course", op: "eq", value: "Nakayama" }],
      columns: ["id", "race_name"],
      limit: 10,
    });
    expect(result).toStrictEqual(
      "SELECT id, race_name FROM horse_racing.race_info WHERE race_course = 'Nakayama' LIMIT 10",
    );
  });
});
