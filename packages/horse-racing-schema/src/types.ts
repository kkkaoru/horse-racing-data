// Run with: bun
// Shared domain types for horse racing data

type TableName =
  | "race_records"
  | "horse_info"
  | "race_info"
  | "trainer_info"
  | "jockey_info"
  | "owner_info"
  | "breeder_info";

interface SchemaField {
  readonly name: string;
  readonly type: string;
  readonly required: boolean;
}

export type { TableName, SchemaField };
