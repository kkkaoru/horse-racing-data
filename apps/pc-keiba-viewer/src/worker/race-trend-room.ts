// Run with bun (vitest) / Cloudflare Workers runtime.
import { DurableObject } from "cloudflare:workers";

interface RaceTrendRoomEvent {
  cacheKey: string | null;
  raceKey: string;
  type: "trend-updated";
  updatedAt: string;
}

const STORAGE_KEY = "latest-event";

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
  return raceKey && /^(jra|nar):[0-9]{8}:[0-9A-Z]{2}:[0-9]{2}$/u.test(raceKey) ? raceKey : null;
};

const isRaceTrendRoomEvent = (value: unknown): value is RaceTrendRoomEvent =>
  typeof value === "object" &&
  value !== null &&
  (value as { type?: unknown }).type === "trend-updated" &&
  typeof (value as { raceKey?: unknown }).raceKey === "string" &&
  typeof (value as { updatedAt?: unknown }).updatedAt === "string" &&
  ((value as { cacheKey?: unknown }).cacheKey === null ||
    typeof (value as { cacheKey?: unknown }).cacheKey === "string");

const getCacheKey = (value: unknown): string | null => {
  if (typeof value !== "object" || value === null || !("cacheKey" in value)) {
    return null;
  }
  return typeof value.cacheKey === "string" ? value.cacheKey : null;
};

export class RaceTrendRoom extends DurableObject<CloudflareEnv> {
  private currentEvent: RaceTrendRoomEvent | null = null;
  private initialized: Promise<void> | null = null;
  private readonly sockets = new Set<WebSocket>();

  override async fetch(request: Request): Promise<Response> {
    const raceKey = getRaceKey(request);
    if (!raceKey) {
      return json({ error: "invalid_race_key" }, { status: 400 });
    }

    await this.ensureInitialized();
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      return this.connect(request);
    }
    if (request.method === "GET") {
      return json(this.currentEvent);
    }
    if (request.method === "POST") {
      return this.update(request, raceKey);
    }
    return json({ error: "method_not_allowed" }, { status: 405 });
  }

  private async ensureInitialized(): Promise<void> {
    this.initialized ??= this.ctx.blockConcurrencyWhile(async () => {
      const stored = await this.ctx.storage.get(STORAGE_KEY);
      this.currentEvent = isRaceTrendRoomEvent(stored) ? stored : null;
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
    server.send(JSON.stringify({ type: "ready" }));
    server.addEventListener("close", () => this.sockets.delete(server));
    server.addEventListener("error", () => this.sockets.delete(server));
    return new Response(null, { status: 101, webSocket: client });
  }

  private async update(request: Request, raceKey: string): Promise<Response> {
    const body: unknown = await request.json().catch(() => null);
    const nextEvent: RaceTrendRoomEvent = {
      cacheKey: getCacheKey(body),
      raceKey,
      type: "trend-updated",
      updatedAt: new Date().toISOString(),
    };
    this.currentEvent = nextEvent;
    await this.ctx.storage.put(STORAGE_KEY, nextEvent);
    this.broadcast(nextEvent);
    return json(nextEvent);
  }

  private broadcast(event: RaceTrendRoomEvent): void {
    const message = JSON.stringify(event);
    for (const socket of this.sockets) {
      try {
        socket.send(message);
      } catch {
        this.sockets.delete(socket);
      }
    }
  }
}
