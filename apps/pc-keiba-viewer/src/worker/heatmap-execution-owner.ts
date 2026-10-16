// Run with bun (vitest) / Cloudflare Workers runtime.
import { DurableObject } from "cloudflare:workers";

import {
  HeatmapExecutionCoordinator,
  type HeatmapExecutionRequest,
} from "../lib/heatmap-execution";

export class HeatmapExecutionOwner extends DurableObject<CloudflareEnv> {
  private readonly coordinator: HeatmapExecutionCoordinator;

  constructor(ctx: PcKeibaDurableObjectState, env: CloudflareEnv) {
    super(ctx, env);
    const sql = ctx.storage.sql;
    const workflow = env.HEATMAP_WARM_WORKFLOW;
    if (sql === undefined || workflow === undefined)
      throw new Error("Heatmap execution bindings unavailable");
    sql.exec(
      "CREATE TABLE IF NOT EXISTS heatmap_owner (id INTEGER PRIMARY KEY CHECK (id = 1), state TEXT NOT NULL)",
    );
    this.coordinator = new HeatmapExecutionCoordinator(
      {
        read: () => {
          const row = sql.exec("SELECT state FROM heatmap_owner WHERE id = 1").toArray()[0];
          if (row === undefined) return undefined;
          try {
            return JSON.parse(String(row.state));
          } catch {
            return null;
          }
        },
        write: (state) => {
          sql.exec(
            "INSERT INTO heatmap_owner (id, state) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET state = excluded.state",
            JSON.stringify(state),
          );
        },
      },
      workflow,
    );
  }

  ensure(request: HeatmapExecutionRequest): Promise<string> {
    return this.coordinator.ensure(request);
  }
  async claim(token: string): Promise<boolean> {
    return this.coordinator.claim(token);
  }
  async settle(token: string, success: boolean): Promise<boolean> {
    return this.coordinator.settle(token, success);
  }
}
