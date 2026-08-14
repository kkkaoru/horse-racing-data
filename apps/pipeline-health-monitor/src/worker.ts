// Run with bun.
import type {
  ExecutionContext,
  MessageBatch,
  ScheduledController,
} from "@cloudflare/workers-types";

import { acknowledgeIncident } from "./incident-state";
import { runQueue } from "./queue-handler";
import { runScheduled } from "./scheduled-handler";
import type { AlertMessage, Env } from "./types";

const JSON_CONTENT_TYPE = "application/json";
const ACK_PATH_PREFIX = "/api/internal/incidents/";
const ACK_PATH_SUFFIX = "/ack";

const isAuthorized = (request: Request, env: Env): boolean =>
  request.headers.get("authorization") === `Bearer ${env.ALERT_ACK_TOKEN}`;

const handleFetch = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  if (
    request.method === "POST" &&
    url.pathname.startsWith(ACK_PATH_PREFIX) &&
    url.pathname.endsWith(ACK_PATH_SUFFIX)
  ) {
    if (!isAuthorized(request, env)) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }
    const incidentId = url.pathname.slice(ACK_PATH_PREFIX.length, -ACK_PATH_SUFFIX.length);
    const state = await acknowledgeIncident(env, incidentId, new Date());
    return state === null
      ? Response.json({ error: "not_found" }, { status: 404 })
      : Response.json({ acknowledgedAt: state.acknowledgedAt, incidentId });
  }
  return buildHealthResponse();
};

const buildHealthResponse = (): Response =>
  new Response(JSON.stringify({ ok: true }), {
    headers: { "content-type": JSON_CONTENT_TYPE },
  });

export default {
  fetch: (request: Request, env: Env): Promise<Response> => handleFetch(request, env),
  scheduled: (controller: ScheduledController, env: Env, ctx: ExecutionContext): void => {
    ctx.waitUntil(runScheduled({ env, now: new Date(controller.scheduledTime) }));
  },
  queue: (batch: MessageBatch<AlertMessage>, env: Env): Promise<void> => runQueue({ batch, env }),
};
