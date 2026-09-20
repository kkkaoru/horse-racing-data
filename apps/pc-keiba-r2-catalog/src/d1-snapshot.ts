// Runs with bun; lossless, read-only D1 extraction for Iceberg snapshot migration.
export interface D1SnapshotColumn {
  name: string;
}

export interface D1SnapshotPageRequest {
  afterRowId: string | null;
  columns: readonly D1SnapshotColumn[];
  limit: number;
  table: string;
}

export interface D1SnapshotPageRow {
  payload: string;
  row_key: string;
}

export interface D1SnapshotClient {
  accountId: string;
  databaseId: string;
  fetchImpl(input: string, init: RequestInit): Promise<Response>;
  token: string;
}

export interface D1SnapshotQuery {
  sql: string;
  params: readonly (number | string)[];
}

const MAX_PAGE_ROWS: number = 1000;
const MAX_COLUMNS: number = 1000;
// D1 documents a 32-argument function limit: each JSON property uses two arguments.
const COLUMNS_PER_JSON_OBJECT: number = 16;
const ROWID_ALIASES: readonly string[] = ["rowid", "_rowid_", "oid"];
const INTEGER_PATTERN: RegExp = /^-?\d+$/u;
const MAX_ROWID: bigint = 9223372036854775807n;
const MIN_ROWID: bigint = -9223372036854775808n;
const D1_QUERY_TIMEOUT_MS: number = 60000;

const quoteIdentifier = (value: string): string => `"${value.replaceAll('"', '""')}"`;
const literal = (value: string): string => `'${value.replaceAll("'", "''")}'`;
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const columnPayload = (column: D1SnapshotColumn, qualifier?: "OLD" | "NEW"): string => {
  const name: string = `${qualifier === undefined ? "" : `${qualifier}.`}${quoteIdentifier(column.name)}`;
  // Text preserves SQLite int64 exactly. Round-trip precision preserves REAL values.
  return `${literal(column.name)}, json_object('type', typeof(${name}), 'value', CASE typeof(${name}) WHEN 'null' THEN NULL WHEN 'blob' THEN hex(${name}) WHEN 'real' THEN printf('%!.17g', ${name}) ELSE CAST(${name} AS TEXT) END)`;
};

export const buildD1SnapshotRowEncoding = (
  input: Pick<D1SnapshotPageRequest, "table" | "columns">,
  qualifier?: "OLD" | "NEW",
): { rowId: string; payloadSql: string } => {
  if (
    input.table.length === 0 ||
    input.table.startsWith("_cf_") ||
    input.table.startsWith("sqlite_")
  )
    throw new Error("Platform-owned tables cannot be migrated as application data");
  if (
    input.columns.length === 0 ||
    input.columns.length > MAX_COLUMNS ||
    new Set(input.columns.map((column) => column.name)).size !== input.columns.length
  )
    throw new Error("Invalid D1 snapshot column list");
  const rowId: string | undefined = ROWID_ALIASES.find(
    (alias) => !input.columns.some((column) => column.name.toLowerCase() === alias),
  );
  if (rowId === undefined) throw new Error("D1 table shadows every rowid alias");
  // Bound each JSON function's argument count, including wide application tables.
  // Every top-level value is a non-null typed object, so json_patch cannot delete a column.
  const payloadSql: string = Array.from(
    { length: Math.ceil(input.columns.length / COLUMNS_PER_JSON_OBJECT) },
    (_, index) =>
      `json_object(${input.columns
        .slice(index * COLUMNS_PER_JSON_OBJECT, (index + 1) * COLUMNS_PER_JSON_OBJECT)
        .map((column) => columnPayload(column, qualifier))
        .join(", ")})`,
  ).reduce((previous, current) => `json_patch(${previous}, ${current})`);
  return { rowId: `${qualifier === undefined ? "" : `${qualifier}.`}${rowId}`, payloadSql };
};

export const buildD1SnapshotPageQuery = (input: D1SnapshotPageRequest): D1SnapshotQuery => {
  if (
    input.afterRowId !== null &&
    (!INTEGER_PATTERN.test(input.afterRowId) ||
      BigInt(input.afterRowId) < MIN_ROWID ||
      BigInt(input.afterRowId) > MAX_ROWID)
  )
    throw new Error("Invalid D1 snapshot row cursor");
  if (!Number.isInteger(input.limit) || input.limit < 1 || input.limit > MAX_PAGE_ROWS)
    throw new Error("Invalid D1 snapshot page limit");
  const { rowId, payloadSql } = buildD1SnapshotRowEncoding(input);
  return {
    sql: `SELECT CAST(${rowId} AS TEXT) AS row_key, ${payloadSql} AS payload FROM ${quoteIdentifier(input.table)} WHERE ${rowId} ${input.afterRowId === null ? ">=" : ">"} CAST(? AS INTEGER) ORDER BY ${rowId} LIMIT ?`,
    params: [input.afterRowId ?? String(MIN_ROWID), input.limit],
  };
};

export const queryD1Snapshot = async (
  client: D1SnapshotClient,
  query: D1SnapshotQuery,
): Promise<Record<string, unknown>[]> => {
  const response: Response = await client.fetchImpl(
    `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(client.accountId)}/d1/database/${encodeURIComponent(client.databaseId)}/query`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${client.token}`, "Content-Type": "application/json" },
      body: JSON.stringify(query),
      signal: AbortSignal.timeout(D1_QUERY_TIMEOUT_MS),
    },
  );
  if (!response.ok) throw new Error(`D1 snapshot query returned HTTP ${response.status}`);
  const body: unknown = await response.json();
  if (
    !isRecord(body) ||
    body.success !== true ||
    !Array.isArray(body.result) ||
    body.result.length !== 1
  )
    throw new Error("Invalid D1 snapshot response");
  const result: unknown = body.result[0];
  if (
    !isRecord(result) ||
    result.success !== true ||
    !Array.isArray(result.results) ||
    !result.results.every(isRecord)
  )
    throw new Error("D1 snapshot statement did not succeed");
  return result.results;
};

export const parseD1SnapshotPage = (
  rows: Record<string, unknown>[],
  request: D1SnapshotPageRequest,
): D1SnapshotPageRow[] => {
  if (rows.length > request.limit) throw new Error("D1 snapshot page exceeded requested limit");
  const previous: { rowId: bigint } = {
    rowId: request.afterRowId === null ? MIN_ROWID - 1n : BigInt(request.afterRowId),
  };
  return rows.map((row) => {
    if (
      typeof row.row_key !== "string" ||
      !INTEGER_PATTERN.test(row.row_key) ||
      typeof row.payload !== "string"
    )
      throw new Error("Invalid D1 snapshot row");
    const rowId: bigint = BigInt(row.row_key);
    if (rowId <= previous.rowId || rowId > MAX_ROWID)
      throw new Error("D1 snapshot row cursor did not advance");
    const payload: unknown = JSON.parse(row.payload);
    if (
      !isRecord(payload) ||
      Object.keys(payload).length !== request.columns.length ||
      !request.columns.every((column) => Object.hasOwn(payload, column.name))
    )
      throw new Error("D1 snapshot payload does not match source schema");
    previous.rowId = rowId;
    return { row_key: row.row_key, payload: row.payload };
  });
};
