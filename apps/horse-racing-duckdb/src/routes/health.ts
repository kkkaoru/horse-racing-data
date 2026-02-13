// Run with: bun
// Health check endpoint

import { Hono } from "hono";
import type { DuckDBAppEnv } from "../types.ts";

const healthRoute = new Hono<DuckDBAppEnv>();

healthRoute.get("/health", (c) => c.json({ status: "ok" }));

export { healthRoute };
