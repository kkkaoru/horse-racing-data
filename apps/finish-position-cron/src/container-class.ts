// Run with bun. Durable-Object-backed Container class for the predictor image.
// This is a thin binding wrapper around @cloudflare/containers — it only sets
// shared config and extends Container, which cannot be instantiated inside the
// vitest pool, so it is excluded from the coverage gate (the start() options it
// receives are built + tested in dispatch.ts).

import { Container } from "@cloudflare/containers";
import type { Env } from "./types";

// The container is a batch job: it exposes no HTTP port and is driven by
// start({ entrypoint, envVars }) from the Worker's scheduled() handler. We give
// it generous sleep + no required ports so it runs to completion and is then
// reaped. Sized to standard-4 in wrangler.jsonc (4 vCPU / 12 GiB / 20 GB) for
// the DuckDB + CatBoost feature build.
export class FinishPositionPredictContainer extends Container<Env> {
  override sleepAfter = "45m";
  override enableInternet = true;
}
