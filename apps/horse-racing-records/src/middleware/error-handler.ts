// Run with: bun
// Global error handler middleware for consistent JSON error responses

import type { Context } from "hono";
import type { AppEnv } from "../types.ts";

const INTERNAL_SERVER_ERROR_STATUS = 500;

const errorHandler = (err: Error, c: Context<AppEnv>): Response => {
  console.error(`Unhandled error: ${err.message}`, err.stack);
  return c.json(
    {
      error: "Internal server error",
      status: INTERNAL_SERVER_ERROR_STATUS,
    },
    INTERNAL_SERVER_ERROR_STATUS,
  );
};

export { errorHandler };
