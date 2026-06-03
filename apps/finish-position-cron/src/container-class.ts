// Run with bun. Durable-Object-backed Container class for the predictor image.
// This is a thin binding wrapper around @cloudflare/containers — it only sets
// shared config and extends Container, which cannot be instantiated inside the
// vitest pool, so it is excluded from the coverage gate (the start() options it
// receives are built + tested in dispatch.ts).

import { Container } from "@cloudflare/containers";
import type { Env } from "./types";

// The container is a batch job: it exposes a no-op TCP listener on
// LIVENESS_PORT in predict_upcoming.py purely so Cloudflare Containers'
// internal liveness probe sees a listening socket and does not SIGTERM the
// process during the multi-minute DuckDB + CatBoost feature build. Without a
// defaultPort, the runtime reaps the container ~90s after start. sleepAfter is
// generous so the alarm loop never expires activity mid-run. Sized to standard-4
// in wrangler.jsonc (4 vCPU / 12 GiB / 20 GB) for the DuckDB aggregation.
const LIVENESS_PORT = 8080;

export class FinishPositionPredictContainer extends Container<Env> {
  override defaultPort = LIVENESS_PORT;
  override sleepAfter = "45m";
  override enableInternet = true;
}
