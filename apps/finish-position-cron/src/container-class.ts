// Run with bun. Durable-Object-backed Container class for the predictor image.
// This is a thin binding wrapper around @cloudflare/containers — it only sets
// shared config and extends Container, which cannot be instantiated inside the
// vitest pool, so it is excluded from the coverage gate (the start() options it
// receives are built + tested in dispatch.ts).
//
// IMPORTANT: Cloudflare Containers reaps batch instances that receive no
// inbound HTTP traffic, independent of sleepAfter. To survive the multi-minute
// DuckDB + CatBoost feature build we run a DO-side keepalive loop:
// onStart() schedules a recurring containerFetch every KEEPALIVE_INTERVAL_SECS
// against the LIVENESS_PORT HTTP server in predict_upcoming.py. The loop stops
// on its own when the container exits (containerFetch raises after the
// process is gone). Without this loop the container is SIGTERM'd ~90s after
// the Worker request returns and only a partial traceback survives.

import { Container } from "@cloudflare/containers";
import type { Env } from "./types";

const LIVENESS_PORT = 8080;
const KEEPALIVE_INTERVAL_SECS = 30;
const KEEPALIVE_CALLBACK = "keepalivePing";
const KEEPALIVE_PATH = "/keepalive";

export class FinishPositionPredictContainer extends Container<Env> {
  override defaultPort = LIVENESS_PORT;
  override sleepAfter = "45m";
  override enableInternet = true;

  override async onStart(): Promise<void> {
    await this.schedule(KEEPALIVE_INTERVAL_SECS, KEEPALIVE_CALLBACK, {});
  }

  async keepalivePing(): Promise<void> {
    try {
      const response = await this.containerFetch(new Request(`http://container${KEEPALIVE_PATH}`));
      if (!response.ok) {
        return;
      }
    } catch {
      return;
    }
    await this.schedule(KEEPALIVE_INTERVAL_SECS, KEEPALIVE_CALLBACK, {});
  }
}
