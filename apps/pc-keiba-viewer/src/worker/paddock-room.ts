import {
  applyPaddockAction,
  createPaddockState,
  getPaddockKvKey,
  isPaddockAction,
  isPaddockState,
  type PaddockState,
} from "../lib/paddock";

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

export class PaddockRoom {
  private readonly env: PaddockRoomEnv;
  private readonly sockets = new Set<WebSocket>();
  private readonly state: PcKeibaDurableObjectState;
  private initialized: Promise<void> | null = null;
  private currentState: PaddockState | null = null;

  constructor(state: PcKeibaDurableObjectState, env: PaddockRoomEnv) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
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
    this.initialized ??= this.state.blockConcurrencyWhile(async () => {
      const stored = await this.state.storage.get(STORAGE_KEY);
      const kvState = await this.env.PADDOCK_STATE_KV?.get(getPaddockKvKey(raceKey), {
        type: "json",
      });
      this.currentState =
        getSyncedState(
          isPaddockState(stored) ? stored : null,
          isPaddockState(kvState) ? kvState : null,
        ) ?? createPaddockState(raceKey);
      await this.state.storage.put(STORAGE_KEY, this.currentState);
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
    server.accept();
    this.sockets.add(server);
    server.send(JSON.stringify({ state: this.currentState, type: "state" }));
    server.addEventListener("close", () => this.sockets.delete(server));
    server.addEventListener("error", () => this.sockets.delete(server));
    return new Response(null, { status: 101, webSocket: client });
  }

  private async update(request: Request): Promise<Response> {
    const body: unknown = await request.json().catch(() => null);
    if (!isPaddockAction(body) || !this.currentState) {
      return json({ error: "invalid_action" }, { status: 400 });
    }

    const nextState = applyPaddockAction(this.currentState, body);
    this.currentState = nextState;
    await this.state.storage.put(STORAGE_KEY, nextState);
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
    for (const socket of this.sockets) {
      try {
        socket.send(message);
      } catch {
        this.sockets.delete(socket);
      }
    }
  }
}
