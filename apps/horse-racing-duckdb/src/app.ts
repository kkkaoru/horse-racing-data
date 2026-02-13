// Run with: bun
// Hono Worker entry point for horse-racing-duckdb API

import { Hono } from "hono";
import type { DuckDBAppEnv } from "./types.ts";
import { healthRoute } from "./routes/health.ts";
import { parquetDeleteRoute } from "./routes/parquet-delete.ts";

const app = new Hono<DuckDBAppEnv>();

app.route("/", healthRoute);
app.route("/", parquetDeleteRoute);

export default app;
