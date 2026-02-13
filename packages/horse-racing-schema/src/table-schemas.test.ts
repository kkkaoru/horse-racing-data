// Run with: bun
// Tests for table-schemas.ts

import { describe, it, expect } from "vitest";
import {
  RACE_RECORDS_COLUMNS,
  HORSE_INFO_COLUMNS,
  RACE_INFO_COLUMNS,
  TRAINER_INFO_COLUMNS,
  JOCKEY_INFO_COLUMNS,
  OWNER_INFO_COLUMNS,
  BREEDER_INFO_COLUMNS,
  isValidTableName,
  getTableColumns,
  isValidColumn,
} from "./table-schemas.ts";

describe("isValidTableName", () => {
  it("should return true for race_records", () => {
    expect(isValidTableName("race_records")).toStrictEqual(true);
  });

  it("should return true for horse_info", () => {
    expect(isValidTableName("horse_info")).toStrictEqual(true);
  });

  it("should return true for race_info", () => {
    expect(isValidTableName("race_info")).toStrictEqual(true);
  });

  it("should return true for trainer_info", () => {
    expect(isValidTableName("trainer_info")).toStrictEqual(true);
  });

  it("should return true for jockey_info", () => {
    expect(isValidTableName("jockey_info")).toStrictEqual(true);
  });

  it("should return true for owner_info", () => {
    expect(isValidTableName("owner_info")).toStrictEqual(true);
  });

  it("should return true for breeder_info", () => {
    expect(isValidTableName("breeder_info")).toStrictEqual(true);
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
  it("should return columns for race_records", () => {
    const columns = getTableColumns("race_records");
    expect(columns).toStrictEqual(RACE_RECORDS_COLUMNS);
  });

  it("should return columns for horse_info", () => {
    const columns = getTableColumns("horse_info");
    expect(columns).toStrictEqual(HORSE_INFO_COLUMNS);
  });

  it("should return columns for race_info", () => {
    const columns = getTableColumns("race_info");
    expect(columns).toStrictEqual(RACE_INFO_COLUMNS);
  });

  it("should return columns for trainer_info", () => {
    const columns = getTableColumns("trainer_info");
    expect(columns).toStrictEqual(TRAINER_INFO_COLUMNS);
  });

  it("should return columns for jockey_info", () => {
    const columns = getTableColumns("jockey_info");
    expect(columns).toStrictEqual(JOCKEY_INFO_COLUMNS);
  });

  it("should return columns for owner_info", () => {
    const columns = getTableColumns("owner_info");
    expect(columns).toStrictEqual(OWNER_INFO_COLUMNS);
  });

  it("should return columns for breeder_info", () => {
    const columns = getTableColumns("breeder_info");
    expect(columns).toStrictEqual(BREEDER_INFO_COLUMNS);
  });

  it("should include id column in race_records", () => {
    const columns = getTableColumns("race_records");
    expect(columns[0]).toStrictEqual("id");
  });

  it("should include horse_name in horse_info", () => {
    const columns = getTableColumns("horse_info");
    expect(columns[3]).toStrictEqual("horse_name");
  });
});

describe("isValidColumn", () => {
  it("should return true for valid column in race_records", () => {
    expect(isValidColumn("race_records", "id")).toStrictEqual(true);
  });

  it("should return true for horse_name in horse_info", () => {
    expect(isValidColumn("horse_info", "horse_name")).toStrictEqual(true);
  });

  it("should return true for race_hash in race_info", () => {
    expect(isValidColumn("race_info", "race_hash")).toStrictEqual(true);
  });

  it("should return true for trainer_slug in trainer_info", () => {
    expect(isValidColumn("trainer_info", "trainer_slug")).toStrictEqual(true);
  });

  it("should return true for jockey_slug in jockey_info", () => {
    expect(isValidColumn("jockey_info", "jockey_slug")).toStrictEqual(true);
  });

  it("should return true for owner_slug in owner_info", () => {
    expect(isValidColumn("owner_info", "owner_slug")).toStrictEqual(true);
  });

  it("should return true for breeder_slug in breeder_info", () => {
    expect(isValidColumn("breeder_info", "breeder_slug")).toStrictEqual(true);
  });

  it("should return false for invalid column", () => {
    expect(isValidColumn("horse_info", "nonexistent_column")).toStrictEqual(false);
  });

  it("should return false for column from different table", () => {
    expect(isValidColumn("horse_info", "finishing_position")).toStrictEqual(false);
  });

  it("should return true for finishing_position in race_records", () => {
    expect(isValidColumn("race_records", "finishing_position")).toStrictEqual(true);
  });

  it("should return true for ticket_win in race_info", () => {
    expect(isValidColumn("race_info", "ticket_win")).toStrictEqual(true);
  });
});

describe("getTableColumns fallback", () => {
  it("should return empty array for unknown table", () => {
    const columns = getTableColumns("nonexistent" as never);
    expect(columns).toStrictEqual([]);
  });
});

describe("column constants", () => {
  it("should have correct count for race_records", () => {
    expect(RACE_RECORDS_COLUMNS.length).toStrictEqual(59);
  });

  it("should have correct count for horse_info", () => {
    expect(HORSE_INFO_COLUMNS.length).toStrictEqual(19);
  });

  it("should have correct count for race_info", () => {
    expect(RACE_INFO_COLUMNS.length).toStrictEqual(26);
  });

  it("should have correct count for trainer_info", () => {
    expect(TRAINER_INFO_COLUMNS.length).toStrictEqual(3);
  });

  it("should have correct count for jockey_info", () => {
    expect(JOCKEY_INFO_COLUMNS.length).toStrictEqual(3);
  });

  it("should have correct count for owner_info", () => {
    expect(OWNER_INFO_COLUMNS.length).toStrictEqual(3);
  });

  it("should have correct count for breeder_info", () => {
    expect(BREEDER_INFO_COLUMNS.length).toStrictEqual(3);
  });
});
