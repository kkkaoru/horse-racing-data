// Run with: bun
// Valid column names per table, derived from schema JSON files (single source of truth)

import raceRecordsSchema from "./schemas/race_records.json";
import horseInfoSchema from "./schemas/horse_info.json";
import raceInfoSchema from "./schemas/race_info.json";
import trainerInfoSchema from "./schemas/trainer_info.json";
import jockeyInfoSchema from "./schemas/jockey_info.json";
import ownerInfoSchema from "./schemas/owner_info.json";
import breederInfoSchema from "./schemas/breeder_info.json";
import type { TableName } from "./types.ts";

interface SchemaJson {
  readonly fields: ReadonlyArray<{ readonly name: string }>;
}

const extractColumnNames = (schema: SchemaJson): ReadonlyArray<string> =>
  schema.fields.map((field) => field.name);

const RACE_RECORDS_COLUMNS = extractColumnNames(raceRecordsSchema);
const HORSE_INFO_COLUMNS = extractColumnNames(horseInfoSchema);
const RACE_INFO_COLUMNS = extractColumnNames(raceInfoSchema);
const TRAINER_INFO_COLUMNS = extractColumnNames(trainerInfoSchema);
const JOCKEY_INFO_COLUMNS = extractColumnNames(jockeyInfoSchema);
const OWNER_INFO_COLUMNS = extractColumnNames(ownerInfoSchema);
const BREEDER_INFO_COLUMNS = extractColumnNames(breederInfoSchema);

const TABLE_COLUMNS: ReadonlyMap<TableName, ReadonlyArray<string>> = new Map([
  ["race_records", RACE_RECORDS_COLUMNS],
  ["horse_info", HORSE_INFO_COLUMNS],
  ["race_info", RACE_INFO_COLUMNS],
  ["trainer_info", TRAINER_INFO_COLUMNS],
  ["jockey_info", JOCKEY_INFO_COLUMNS],
  ["owner_info", OWNER_INFO_COLUMNS],
  ["breeder_info", BREEDER_INFO_COLUMNS],
]);

const VALID_TABLE_NAMES: ReadonlySet<string> = new Set<string>([
  "race_records",
  "horse_info",
  "race_info",
  "trainer_info",
  "jockey_info",
  "owner_info",
  "breeder_info",
]);

const isValidTableName = (name: string): name is TableName => VALID_TABLE_NAMES.has(name);

const getTableColumns = (table: TableName): ReadonlyArray<string> => TABLE_COLUMNS.get(table) ?? [];

const isValidColumn = (table: TableName, column: string): boolean =>
  getTableColumns(table).includes(column);

export {
  RACE_RECORDS_COLUMNS,
  HORSE_INFO_COLUMNS,
  RACE_INFO_COLUMNS,
  TRAINER_INFO_COLUMNS,
  JOCKEY_INFO_COLUMNS,
  OWNER_INFO_COLUMNS,
  BREEDER_INFO_COLUMNS,
  TABLE_COLUMNS,
  VALID_TABLE_NAMES,
  isValidTableName,
  getTableColumns,
  isValidColumn,
};
