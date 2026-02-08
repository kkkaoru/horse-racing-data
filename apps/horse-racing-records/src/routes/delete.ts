// Run with: bun
// POST /tables/:table/delete - Delete records via Iceberg equality deletes

import { Hono } from "hono";
import type { AppEnv, DeleteRequest } from "../types.ts";
import { isValidTableName } from "../table-schemas.ts";
import { executeEqualityDelete } from "../iceberg/equality-delete.ts";

const BAD_REQUEST_STATUS = 400;
const INTERNAL_ERROR_STATUS = 500;
const MIN_FILTER_COUNT = 1;
const MIN_IDS_COUNT = 1;

const deleteRoute = new Hono<AppEnv>();

deleteRoute.post("/tables/:table/delete", async (c) => {
  const tableName = c.req.param("table");

  if (!isValidTableName(tableName)) {
    return c.json(
      { error: `Invalid table name: ${tableName}`, status: BAD_REQUEST_STATUS },
      BAD_REQUEST_STATUS,
    );
  }

  const body = await c.req.json<DeleteRequest>();

  if (!body.confirm) {
    return c.json(
      { error: "Delete requires confirm: true", status: BAD_REQUEST_STATUS },
      BAD_REQUEST_STATUS,
    );
  }

  const hasIds = body.ids && body.ids.length >= MIN_IDS_COUNT;
  const hasFilters = body.filters && body.filters.length >= MIN_FILTER_COUNT;

  if (!hasIds && !hasFilters) {
    return c.json(
      { error: "Delete requires ids or at least one filter", status: BAD_REQUEST_STATUS },
      BAD_REQUEST_STATUS,
    );
  }

  const result = await executeEqualityDelete({
    env: c.env,
    table: tableName,
    filters: body.filters,
    ids: body.ids,
  });

  if (!result.success) {
    return c.json(
      { error: result.error ?? "Delete operation failed", status: INTERNAL_ERROR_STATUS },
      INTERNAL_ERROR_STATUS,
    );
  }

  return c.json({ success: true, deletedCount: result.deletedCount });
});

export { deleteRoute };
