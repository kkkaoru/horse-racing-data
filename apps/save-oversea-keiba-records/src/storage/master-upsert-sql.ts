// This file runs with Bun.
//
// INSERT-only SQL for numeric-only master backfill.
// Always uses ON CONFLICT DO NOTHING — never UPDATE or DELETE master rows.

import type { BuiltMasterRow } from "../domain/master-row-builder";
import type { SqlStatement } from "../types";

const readColumnValue = (row: Readonly<Record<string, string>>, column: string): string => {
  const value: string | undefined = row[column];
  if (value === undefined) {
    throw new Error(`Missing required master INSERT column: ${column}`);
  }
  return value;
};

/**
 * Build a parameterized INSERT ... ON CONFLICT (pk) DO NOTHING for one master row.
 */
export const buildMasterInsertDoNothing = (built: BuiltMasterRow): SqlStatement => {
  const columns: readonly string[] = Object.keys(built.row);
  if (columns.length === 0) {
    throw new Error(`Master row for ${built.table} has no columns`);
  }
  const placeholders: string[] = columns.map(
    (_column: string, index: number): string => `$${index + 1}`,
  );
  return {
    text: `INSERT INTO ${built.table} (${columns.join(", ")}) VALUES (${placeholders.join(", ")}) ON CONFLICT (${built.primaryKeyColumn}) DO NOTHING`,
    values: columns.map((column: string): string => readColumnValue(built.row, column)),
  };
};

export const buildMasterInsertStatements = (
  builtRows: readonly BuiltMasterRow[],
): readonly SqlStatement[] => builtRows.map(buildMasterInsertDoNothing);
