// Run with: bun
// Safe SQL query builder from structured QueryFilter objects

import type { QueryFilter, TableName } from "../types.ts";
import { isValidColumn } from "../table-schemas.ts";

const OPERATOR_MAP: ReadonlyMap<string, string> = new Map([
  ["eq", "="],
  ["neq", "!="],
  ["gt", ">"],
  ["gte", ">="],
  ["lt", "<"],
  ["lte", "<="],
  ["in", "IN"],
  ["like", "LIKE"],
]);

const MAX_QUERY_LIMIT = 10000;
const DEFAULT_QUERY_LIMIT = 100;

const escapeStringValue = (value: string): string => value.replace(/'/g, "''");

const formatValue = (value: string | number): string =>
  typeof value === "number" ? String(value) : `'${escapeStringValue(String(value))}'`;

const isArrayValue = (
  value: string | number | ReadonlyArray<string | number>,
): value is ReadonlyArray<string | number> => Array.isArray(value);

const formatInValues = (values: ReadonlyArray<string | number>): string =>
  `(${values.map(formatValue).join(", ")})`;

const buildWhereClause = (table: TableName, filters: ReadonlyArray<QueryFilter>): string => {
  if (filters.length === 0) return "";

  const conditions = filters.map((filter) => {
    if (!isValidColumn(table, filter.column)) {
      throw new Error(`Invalid column name: ${filter.column}`);
    }

    const sqlOp = OPERATOR_MAP.get(filter.op);
    if (!sqlOp) {
      throw new Error(`Invalid operator: ${filter.op}`);
    }

    if (filter.op === "in") {
      if (!isArrayValue(filter.value)) {
        throw new Error(`IN operator requires an array value for column: ${filter.column}`);
      }
      return `${filter.column} ${sqlOp} ${formatInValues(filter.value)}`;
    }

    if (isArrayValue(filter.value)) {
      throw new Error(`Non-IN operator does not accept array value for column: ${filter.column}`);
    }

    return `${filter.column} ${sqlOp} ${formatValue(filter.value)}`;
  });

  return ` WHERE ${conditions.join(" AND ")}`;
};

interface BuildSelectQueryArgs {
  readonly table: TableName;
  readonly namespace: string;
  readonly filters: ReadonlyArray<QueryFilter>;
  readonly columns?: ReadonlyArray<string>;
  readonly limit?: number;
}

const buildSelectQuery = (args: BuildSelectQueryArgs): string => {
  const columnList = args.columns?.length
    ? args.columns
        .map((col) => {
          if (!isValidColumn(args.table, col)) {
            throw new Error(`Invalid column name: ${col}`);
          }
          return col;
        })
        .join(", ")
    : "*";

  const where = buildWhereClause(args.table, args.filters ?? []);
  const limitValue = Math.min(args.limit ?? DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT);

  return `SELECT ${columnList} FROM ${args.namespace}.${args.table}${where} LIMIT ${String(limitValue)}`;
};

export {
  buildSelectQuery,
  buildWhereClause,
  escapeStringValue,
  formatValue,
  formatInValues,
  OPERATOR_MAP,
  MAX_QUERY_LIMIT,
  DEFAULT_QUERY_LIMIT,
};
export type { BuildSelectQueryArgs };
