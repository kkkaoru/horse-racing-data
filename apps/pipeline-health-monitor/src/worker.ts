// Run with bun.
import type {
  ExecutionContext,
  MessageBatch,
  ScheduledController,
} from "@cloudflare/workers-types";

import { runQueue } from "./queue-handler";
import { runScheduled } from "./scheduled-handler";
import type { AlertMessage, Env } from "./types";

const JSON_CONTENT_TYPE = "application/json";

const buildHealthResponse = (): Response =>
  new Response(JSON.stringify({ ok: true }), {
    headers: { "content-type": JSON_CONTENT_TYPE },
  });

export default {
  fetch: (): Response => buildHealthResponse(),
  scheduled: (controller: ScheduledController, env: Env, ctx: ExecutionContext): void => {
    ctx.waitUntil(runScheduled({ env, now: new Date(controller.scheduledTime) }));
  },
  queue: (batch: MessageBatch<AlertMessage>, env: Env): Promise<void> => runQueue({ batch, env }),
};
