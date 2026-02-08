// Run with: bun
// Health check endpoint

import { Hono } from "hono";
import type { AppEnv } from "../types.ts";

const healthRoute = new Hono<AppEnv>();

healthRoute.get("/health", (c) => c.json({ status: "ok" }));

export { healthRoute };
