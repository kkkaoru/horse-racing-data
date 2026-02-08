// Run with: bun
// POST /tables/:table/query - Query Iceberg tables via R2 SQL

import { Hono } from "hono";
import type { AppEnv, QueryRequest } from "../types.ts";
import { isValidTableName } from "../table-schemas.ts";
import { buildSelectQuery } from "../query/sql-builder.ts";
import { executeR2SqlQuery } from "../query/r2-sql-client.ts";
import type { R2SqlConfig } from "../query/r2-sql-client.ts";

const BAD_REQUEST_STATUS = 400;
const INTERNAL_ERROR_STATUS = 500;

const queryRoute = new Hono<AppEnv>();

const buildR2SqlConfig = (env: AppEnv["Bindings"]): R2SqlConfig => ({
  endpoint: env.R2_SQL_ENDPOINT,
  accountId: env.CLOUDFLARE_ACCOUNT_ID,
  bucketName: env.R2_BUCKET_NAME,
  apiToken: env.CLOUDFLARE_API_TOKEN,
});

queryRoute.post("/tables/:table/query", async (c) => {
  const tableName = c.req.param("table");

  if (!isValidTableName(tableName)) {
    return c.json(
      { error: `Invalid table name: ${tableName}`, status: BAD_REQUEST_STATUS },
      BAD_REQUEST_STATUS,
    );
  }

  const body = await c.req.json<QueryRequest>();

  const query = (() => {
    try {
      return buildSelectQuery({
        table: tableName,
        namespace: c.env.ICEBERG_NAMESPACE,
        filters: body.filters ?? [],
        columns: body.columns,
        limit: body.limit,
      });
    } catch (e: unknown) {
      return e instanceof Error ? e : new Error("Unknown query build error");
    }
  })();

  if (query instanceof Error) {
    return c.json({ error: query.message, status: BAD_REQUEST_STATUS }, BAD_REQUEST_STATUS);
  }

  const config = buildR2SqlConfig(c.env);
  const result = await executeR2SqlQuery(config, query);

  if (!result.success) {
    return c.json(
      { error: result.error ?? "Query execution failed", status: INTERNAL_ERROR_STATUS },
      INTERNAL_ERROR_STATUS,
    );
  }

  return c.json({ data: result.data, count: result.data.length });
});

export { queryRoute, buildR2SqlConfig };
