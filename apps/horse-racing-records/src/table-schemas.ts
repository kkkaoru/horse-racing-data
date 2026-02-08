// Run with: bun
// Valid column names per table, derived from schema JSON files (single source of truth)

import horseRacingRecordsSchema from "../schemas/horse_racing_records.json";
import horseInfoSchema from "../schemas/horse_info.json";
import raceInfoSchema from "../schemas/race_info.json";
import type { TableName } from "./types.ts";

interface SchemaJson {
  readonly fields: ReadonlyArray<{ readonly name: string }>;
}

const extractColumnNames = (schema: SchemaJson): ReadonlyArray<string> =>
  schema.fields.map((field) => field.name);

const HORSE_RACING_RECORDS_COLUMNS = extractColumnNames(horseRacingRecordsSchema);
const HORSE_INFO_COLUMNS = extractColumnNames(horseInfoSchema);
const RACE_INFO_COLUMNS = extractColumnNames(raceInfoSchema);

const TABLE_COLUMNS: ReadonlyMap<TableName, ReadonlyArray<string>> = new Map([
  ["horse_racing_records", HORSE_RACING_RECORDS_COLUMNS],
  ["horse_info", HORSE_INFO_COLUMNS],
  ["race_info", RACE_INFO_COLUMNS],
]);

const VALID_TABLE_NAMES: ReadonlySet<string> = new Set<string>([
  "horse_racing_records",
  "horse_info",
  "race_info",
]);

const isValidTableName = (name: string): name is TableName => VALID_TABLE_NAMES.has(name);

const getTableColumns = (table: TableName): ReadonlyArray<string> => TABLE_COLUMNS.get(table) ?? [];

const isValidColumn = (table: TableName, column: string): boolean =>
  getTableColumns(table).includes(column);

export {
  HORSE_RACING_RECORDS_COLUMNS,
  HORSE_INFO_COLUMNS,
  RACE_INFO_COLUMNS,
  TABLE_COLUMNS,
  VALID_TABLE_NAMES,
  isValidTableName,
  getTableColumns,
  isValidColumn,
};
