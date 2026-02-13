// Run with: bun
// Tests for types.ts

import { it, expect } from "vitest";
import type { TableName, SchemaField } from "./types.ts";

it("should allow valid TableName values", () => {
  const name1: TableName = "race_records";
  const name2: TableName = "horse_info";
  const name3: TableName = "race_info";
  const name4: TableName = "trainer_info";
  const name5: TableName = "jockey_info";
  const name6: TableName = "owner_info";
  const name7: TableName = "breeder_info";
  expect(name1).toStrictEqual("race_records");
  expect(name2).toStrictEqual("horse_info");
  expect(name3).toStrictEqual("race_info");
  expect(name4).toStrictEqual("trainer_info");
  expect(name5).toStrictEqual("jockey_info");
  expect(name6).toStrictEqual("owner_info");
  expect(name7).toStrictEqual("breeder_info");
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
