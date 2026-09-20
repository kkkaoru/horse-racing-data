// Runs with bun; read-only capture preflight. Never omit an unsupported unique index.
import { createHash } from "node:crypto";
import {
  buildD1CapturePlan,
  type D1CaptureInput,
  type D1CaptureUniqueKey,
} from "./d1-change-capture";
import { buildD1SnapshotRowEncoding, type D1SnapshotQuery } from "./d1-snapshot";

export interface D1CaptureSchema extends Pick<D1CaptureInput, "table" | "columns" | "uniqueKeys"> {
  sourceSql: string;
  definitionHash: string;
}
export type D1CaptureMetadataQuery = (
  query: D1SnapshotQuery,
) => Promise<readonly Record<string, unknown>[]>;
const literal = (value: string): string => `'${value.replaceAll("'", "''")}'`;
const quote = (value: string): string => `"${value.replaceAll('"', '""')}"`;
const integer = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const redundantNullPredicate = (sql: string, columns: readonly { name: string }[]): boolean => {
  if (!/^CREATE\s+UNIQUE\s+INDEX\b/iu.test(sql) || /--|\/\*/u.test(sql)) return false;
  const match =
    /\bWHERE\s+(?:"([a-z_][a-z0-9_]*)"|([a-z_][a-z0-9_]*))\s+IS\s+NOT\s+NULL\s*;?\s*$/iu.exec(sql);
  const column: string | undefined = match?.[1] ?? match?.[2];
  return (
    column !== undefined && columns.some(({ name }) => name.toLowerCase() === column.toLowerCase())
  );
};

export const discoverD1CaptureSchema = async (
  table: string,
  query: D1CaptureMetadataQuery,
): Promise<D1CaptureSchema> => {
  if (table.length === 0 || /^(?:_cf_|sqlite_|__pc_keiba_catalog_cdc)/iu.test(table))
    throw new Error("Invalid capture source table");
  const definitions = await query({
    sql: "SELECT type, sql FROM sqlite_schema WHERE name = ?",
    params: [table],
  });
  const definition = definitions[0];
  if (
    definitions.length !== 1 ||
    definition === undefined ||
    definition.type !== "table" ||
    typeof definition.sql !== "string" ||
    !/^CREATE\s+TABLE\b/iu.test(definition.sql)
  )
    throw new Error("Capture requires an ordinary source table");
  const columnRows = await query({ sql: `PRAGMA table_xinfo(${literal(table)})`, params: [] });
  const columns = columnRows.map((row) => {
    if (typeof row.name !== "string" || row.hidden !== 0 || !integer(row.cid))
      throw new Error("Unsupported hidden or generated source column");
    return { name: row.name };
  });
  const encoding = buildD1SnapshotRowEncoding({ table, columns });
  // A WITHOUT ROWID table must fail here rather than silently produce an invalid trigger.
  await query({ sql: `SELECT ${encoding.rowId} FROM ${quote(table)} LIMIT 0`, params: [] });
  const indexes = await query({ sql: `PRAGMA index_list(${literal(table)})`, params: [] });
  const uniqueKeys: D1CaptureUniqueKey[] = [];
  const details: {
    index: Record<string, unknown>;
    columns: readonly Record<string, unknown>[];
    predicateSql?: string;
  }[] = [];
  for (const index of indexes) {
    if (index.unique === 0) continue;
    if (
      index.unique !== 1 ||
      typeof index.name !== "string" ||
      (index.partial !== 0 && index.partial !== 1)
    )
      throw new Error("Unsupported unique index definition");
    const rows = await query({ sql: `PRAGMA index_xinfo(${literal(index.name)})`, params: [] });
    const keyColumns = rows
      .filter((row) => {
        if (row.key !== 0 && row.key !== 1) throw new Error("Invalid unique index key flag");
        return row.key === 1;
      })
      .map((row) => {
        if (
          !integer(row.cid) ||
          !integer(row.seqno) ||
          typeof row.name !== "string" ||
          typeof row.coll !== "string"
        )
          throw new Error("Expression or malformed unique index column");
        return { position: row.seqno, name: row.name, collation: row.coll };
      })
      .sort((left, right) => left.position - right.position);
    if (keyColumns.some((column, position) => column.position !== position))
      throw new Error("Non-contiguous unique index metadata");
    if (index.partial === 1) {
      const predicates = await query({
        sql: "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
        params: [index.name],
      });
      const predicate = predicates[0];
      if (
        predicates.length !== 1 ||
        predicate === undefined ||
        typeof predicate.sql !== "string" ||
        !redundantNullPredicate(predicate.sql, keyColumns)
      )
        throw new Error("Unsupported unique index predicate");
      // SQL '=' never matches NULL keys: an indexed-key IS NOT NULL predicate cannot
      // remove any conflicting pair. Normalize only this proven-equivalent conflict rule.
      details.push({ index, columns: rows, predicateSql: predicate.sql });
    } else {
      details.push({ index, columns: rows });
    }
    uniqueKeys.push({
      partial: false,
      columns: keyColumns.map(({ name, collation }) => ({ name, collation })),
    });
  }
  const captured = { table, columns, uniqueKeys };
  buildD1CapturePlan({ ...captured, captureId: "preflight" });
  return {
    ...captured,
    sourceSql: definition.sql,
    definitionHash: createHash("sha256")
      .update(JSON.stringify({ definition, columnRows, indexes, details }))
      .digest("hex"),
  };
};
