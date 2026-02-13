// Run with: bun
// POST /parquet/delete - Create equality delete Parquet via DuckDB

import { Hono } from "hono";
import type { DuckDBAppEnv, CreateDeleteParquetRequest } from "../types.ts";
import { createDuckDBInstance } from "../duckdb/connection.ts";
import { writeDeleteParquet } from "../duckdb/parquet-writer.ts";

const BAD_REQUEST_STATUS = 400;
const INTERNAL_ERROR_STATUS = 500;
const RECORD_COUNT_HEADER = "X-Record-Count";

const parquetDeleteRoute = new Hono<DuckDBAppEnv>();

parquetDeleteRoute.post("/parquet/delete", async (c) => {
  const body = await c.req.json<CreateDeleteParquetRequest>();

  if (!body.deleteIds || body.deleteIds.length === 0) {
    return c.json(
      { error: "deleteIds is required and must not be empty", status: BAD_REQUEST_STATUS },
      BAD_REQUEST_STATUS,
    );
  }

  if (!body.columnName) {
    return c.json(
      { error: "columnName is required", status: BAD_REQUEST_STATUS },
      BAD_REQUEST_STATUS,
    );
  }

  const db = await createDuckDBInstance();
  const conn = db.connect();

  try {
    const result = await writeDeleteParquet({
      conn,
      deleteIds: body.deleteIds,
      columnName: body.columnName,
    });

    return new Response(result.bytes, {
      status: 200,
      headers: {
        "Content-Type": "application/octet-stream",
        [RECORD_COUNT_HEADER]: String(result.recordCount),
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : "Unknown error creating delete parquet";
    return c.json({ error: message, status: INTERNAL_ERROR_STATUS }, INTERNAL_ERROR_STATUS);
  } finally {
    conn.close();
    db.close();
  }
});

export { parquetDeleteRoute, RECORD_COUNT_HEADER };
