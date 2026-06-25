// Run with bun (vitest) / Cloudflare Workers runtime.
import { DurableObject } from "cloudflare:workers";

import {
  applyPaddockAction,
  createPaddockState,
  getPaddockKvKey,
  isPaddockAction,
  isPaddockState,
  type PaddockState,
} from "../lib/paddock";
import { closeSocket, trySend } from "./websocket-broadcast";

interface PaddockRoomEnv {
  PADDOCK_STATE_KV?: CloudflareEnv["PADDOCK_STATE_KV"];
}

const STORAGE_KEY = "state";
const PADDOCK_STATE_KV_TTL_SECONDS = 30 * 24 * 60 * 60;

const json = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: (() => {
      const headers = new Headers(init?.headers);
      headers.set("content-type", "application/json; charset=utf-8");
      return headers;
    })(),
    status: init?.status ?? 200,
  });

const getRaceKey = (request: Request): string | null => {
  const raceKey = new URL(request.url).searchParams.get("raceKey");
  return raceKey && /^[0-9]{8}:[0-9A-Z]{2}:[0-9]{2}$/u.test(raceKey) ? raceKey : null;
};

const getSyncedState = (
  stored: PaddockState | null,
  kvState: PaddockState | null,
): PaddockState | null => {
  if (!stored) {
    return kvState;
  }
  if (!kvState) {
    return stored;
  }
  if (kvState.history.length > stored.history.length) {
    return kvState;
  }
  return Date.parse(kvState.updatedAt) > Date.parse(stored.updatedAt) ? kvState : stored;
};

export class PaddockRoom extends DurableObject<PaddockRoomEnv> {
  private initialized: Promise<void> | null = null;
  private currentState: PaddockState | null = null;

  override async fetch(request: Request): Promise<Response> {
    const raceKey = getRaceKey(request);
    if (!raceKey) {
      return json({ error: "invalid_race_key" }, { status: 400 });
    }

    await this.ensureInitialized(raceKey);
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      return this.connect(request);
    }
    if (request.method === "GET") {
      return json(this.currentState);
    }
    if (request.method === "POST") {
      return this.update(request);
    }
    return json({ error: "method_not_allowed" }, { status: 405 });
  }

  private async ensureInitialized(raceKey: string): Promise<void> {
    this.initialized ??= this.ctx.blockConcurrencyWhile(async () => {
      const stored = await this.ctx.storage.get(STORAGE_KEY);
      const kvState = await this.env.PADDOCK_STATE_KV?.get(getPaddockKvKey(raceKey), {
        type: "json",
      });
      this.currentState =
        getSyncedState(
          isPaddockState(stored) ? stored : null,
          isPaddockState(kvState) ? kvState : null,
        ) ?? createPaddockState(raceKey);
      await this.ctx.storage.put(STORAGE_KEY, this.currentState);
    });
    await this.initialized;
  }

  private connect(request: Request): Response {
    if (request.headers.get("upgrade") !== "websocket") {
      return json({ error: "upgrade_required" }, { status: 426 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server);
    server.send(JSON.stringify({ state: this.currentState, type: "state" }));
    return new Response(null, { status: 101, webSocket: client });
  }

  private async update(request: Request): Promise<Response> {
    const body: unknown = await request.json().catch(() => null);
    if (!isPaddockAction(body) || !this.currentState) {
      return json({ error: "invalid_action" }, { status: 400 });
    }

    const nextState = applyPaddockAction(this.currentState, body);
    this.currentState = nextState;
    await this.ctx.storage.put(STORAGE_KEY, nextState);
    await this.env.PADDOCK_STATE_KV?.put(
      getPaddockKvKey(nextState.raceKey),
      JSON.stringify(nextState),
      { expirationTtl: PADDOCK_STATE_KV_TTL_SECONDS },
    );
    this.broadcast(nextState);
    return json(nextState);
  }

  private broadcast(state: PaddockState): void {
    const message = JSON.stringify({ state, type: "state" });
    for (const socket of this.ctx.getWebSockets()) {
      trySend(socket, message);
    }
  }

  // Hibernation runtime entrypoint. These rooms are broadcast-only, so inbound
  // client frames are ignored.
  async webSocketMessage(): Promise<void> {
    return Promise.resolve();
  }

  // Hibernation runtime entrypoint invoked when a peer closes; release the
  // server-side socket so it stops counting against billed duration.
  async webSocketClose(ws: WebSocket): Promise<void> {
    closeSocket(ws);
    return Promise.resolve();
  }

  // Hibernation runtime entrypoint invoked on socket error; release the socket.
  async webSocketError(ws: WebSocket): Promise<void> {
    closeSocket(ws);
    return Promise.resolve();
  }
}
