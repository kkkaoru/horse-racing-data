// Run with: bun
// Hono Worker entry point for horse-racing-records API

import "./avsc-patch.ts";
import { Hono } from "hono";
import type { AppEnv } from "./types.ts";
import { mtlsMiddleware } from "./middleware/mtls.ts";
import { errorHandler } from "./middleware/error-handler.ts";
import { healthRoute } from "./routes/health.ts";
import { queryRoute } from "./routes/query.ts";
import { deleteRoute } from "./routes/delete.ts";

const app = new Hono<AppEnv>();

app.onError(errorHandler);
app.use("*", mtlsMiddleware);

app.route("/", healthRoute);
app.route("/", queryRoute);
app.route("/", deleteRoute);

export default app;
