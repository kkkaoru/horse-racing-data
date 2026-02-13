// Run with: bun
// Create equality delete Parquet files via @ducklings/workers (DuckDB WASM)

import type { Connection } from "@ducklings/workers";

const DELETE_PARQUET_PATH = "/tmp/delete.parquet";

interface WriteDeleteParquetArgs {
  readonly conn: Connection;
  readonly deleteIds: ReadonlyArray<string>;
  readonly columnName: string;
}

interface WriteDeleteParquetResult {
  readonly bytes: Uint8Array;
  readonly recordCount: number;
}

const escapeId = (id: string): string => id.replace(/'/g, "''");

const writeDeleteParquet = async (
  args: WriteDeleteParquetArgs,
): Promise<WriteDeleteParquetResult> => {
  const { conn, deleteIds, columnName } = args;

  await conn.execute(`CREATE TABLE delete_ids (${columnName} VARCHAR)`);

  const valuesList = deleteIds.map((id) => `('${escapeId(id)}')`).join(", ");

  await conn.execute(`INSERT INTO delete_ids VALUES ${valuesList}`);
  await conn.execute(`COPY delete_ids TO '${DELETE_PARQUET_PATH}' (FORMAT PARQUET)`);

  const result = await conn.query<{ content: Uint8Array }>(
    `SELECT content FROM read_blob('${DELETE_PARQUET_PATH}')`,
  );
  const firstRow = result[0];

  if (!firstRow) {
    throw new Error("Failed to read Parquet file bytes from DuckDB");
  }

  const bytes =
    firstRow.content instanceof Uint8Array
      ? firstRow.content
      : new Uint8Array(firstRow.content as ArrayBufferLike);

  return {
    bytes,
    recordCount: deleteIds.length,
  };
};

export { writeDeleteParquet, DELETE_PARQUET_PATH, escapeId };
export type { WriteDeleteParquetArgs, WriteDeleteParquetResult };
