// Run with: bun
// Tests for table-schemas.ts

import { describe, it, expect } from "vitest";
import {
  HORSE_RACING_RECORDS_COLUMNS,
  HORSE_INFO_COLUMNS,
  RACE_INFO_COLUMNS,
  isValidTableName,
  getTableColumns,
  isValidColumn,
} from "./table-schemas.ts";

describe("isValidTableName", () => {
  it("should return true for horse_racing_records", () => {
    expect(isValidTableName("horse_racing_records")).toStrictEqual(true);
  });

  it("should return true for horse_info", () => {
    expect(isValidTableName("horse_info")).toStrictEqual(true);
  });

  it("should return true for race_info", () => {
    expect(isValidTableName("race_info")).toStrictEqual(true);
  });

  it("should return false for invalid table name", () => {
    expect(isValidTableName("invalid_table")).toStrictEqual(false);
  });

  it("should return false for empty string", () => {
    expect(isValidTableName("")).toStrictEqual(false);
  });

  it("should return false for SQL injection attempt", () => {
    expect(isValidTableName("horse_info; DROP TABLE")).toStrictEqual(false);
  });
});

describe("getTableColumns", () => {
  it("should return columns for horse_racing_records", () => {
    const columns = getTableColumns("horse_racing_records");
    expect(columns).toStrictEqual(HORSE_RACING_RECORDS_COLUMNS);
  });

  it("should return columns for horse_info", () => {
    const columns = getTableColumns("horse_info");
    expect(columns).toStrictEqual(HORSE_INFO_COLUMNS);
  });

  it("should return columns for race_info", () => {
    const columns = getTableColumns("race_info");
    expect(columns).toStrictEqual(RACE_INFO_COLUMNS);
  });

  it("should include id column in horse_racing_records", () => {
    const columns = getTableColumns("horse_racing_records");
    expect(columns[0]).toStrictEqual("id");
  });

  it("should include horse_name in horse_info", () => {
    const columns = getTableColumns("horse_info");
    expect(columns[2]).toStrictEqual("horse_name");
  });
});

describe("isValidColumn", () => {
  it("should return true for valid column in horse_racing_records", () => {
    expect(isValidColumn("horse_racing_records", "id")).toStrictEqual(true);
  });

  it("should return true for horse_name in horse_info", () => {
    expect(isValidColumn("horse_info", "horse_name")).toStrictEqual(true);
  });

  it("should return true for race_hash in race_info", () => {
    expect(isValidColumn("race_info", "race_hash")).toStrictEqual(true);
  });

  it("should return false for invalid column", () => {
    expect(isValidColumn("horse_info", "nonexistent_column")).toStrictEqual(false);
  });

  it("should return false for column from different table", () => {
    expect(isValidColumn("horse_info", "finishing_position")).toStrictEqual(false);
  });

  it("should return true for finishing_position in horse_racing_records", () => {
    expect(isValidColumn("horse_racing_records", "finishing_position")).toStrictEqual(true);
  });

  it("should return true for ticket_tansho in race_info", () => {
    expect(isValidColumn("race_info", "ticket_tansho")).toStrictEqual(true);
  });
});

describe("column constants", () => {
  it("should have correct count for horse_racing_records", () => {
    expect(HORSE_RACING_RECORDS_COLUMNS.length).toStrictEqual(53);
  });

  it("should have correct count for horse_info", () => {
    expect(HORSE_INFO_COLUMNS.length).toStrictEqual(28);
  });

  it("should have correct count for race_info", () => {
    expect(RACE_INFO_COLUMNS.length).toStrictEqual(25);
  });
});
