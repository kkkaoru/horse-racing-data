// run with: bun run test
// Durable Object that holds the latest horse-weight snapshot per race_key and
// broadcasts updates via Server-Sent Events. State is in-memory because D1
// already provides persistence for the underlying weight rows.
import type { DurableObjectState } from "@cloudflare/workers-types";

export interface HorseWeightEntry {
  horseNumber: string;
  horseName: string | null;
  weight: number | null;
  changeSign: string | null;
  changeAmount: number | null;
}

export interface HorseWeightSnapshot {
  fetchedAt: string;
  horses: HorseWeightEntry[];
}

interface Subscriber {
  controller: ReadableStreamDefaultController<Uint8Array>;
  encoder: TextEncoder;
}

interface HorseWeightDOStub {
  fetch: (input: string, init?: RequestInit) => Promise<Response>;
}

const SSE_RETRY_MS = 5000;
const SSE_HEADERS: Record<string, string> = {
  "Cache-Control": "no-cache, no-transform",
  Connection: "keep-alive",
  "Content-Type": "text/event-stream",
};
const STREAM_URL = "https://horse-weight-do/stream";
const WEIGHTS_URL = "https://horse-weight-do/weights";

const encodeEvent = (event: string, data: string): string => `event: ${event}\ndata: ${data}\n\n`;

const hasHorses = (record: Record<string, unknown>): boolean => Array.isArray(record.horses);

const isHorseWeightSnapshot = (value: unknown): value is HorseWeightSnapshot => {
  if (value === null || typeof value !== "object") return false;
  const record: Record<string, unknown> = { ...value };
  return typeof record.fetchedAt === "string" && hasHorses(record);
};

const notFoundResponse = (): Response => new Response("not found", { status: 404 });

const okJsonResponse = (): Response =>
  new Response(JSON.stringify({ ok: true }), {
    headers: { "Content-Type": "application/json" },
  });

const badRequestResponse = (): Response =>
  new Response(JSON.stringify({ error: "invalid body" }), {
    headers: { "Content-Type": "application/json" },
    status: 400,
  });

const snapshotJsonResponse = (snapshot: HorseWeightSnapshot): Response =>
  new Response(JSON.stringify(snapshot), {
    headers: { "Content-Type": "application/json" },
  });

export class HorseWeightDO {
  private snapshot: HorseWeightSnapshot | null = null;
  private readonly subscribers: Set<Subscriber> = new Set();
  // The DurableObjectState handle is accepted to satisfy the Cloudflare DO
  // contract; this implementation does not currently use storage because the
  // in-memory snapshot is small and D1 is the authoritative store.
  private readonly state: DurableObjectState;

  constructor(state: DurableObjectState, _env: unknown) {
    this.state = state;
  }

  // Test factory that bypasses the Cloudflare DO constructor signature so unit
  // tests do not need to forge a full DurableObjectState. Only call from tests.
  static createForTest(): HorseWeightDO {
    const instance: HorseWeightDO = Object.create(HorseWeightDO.prototype);
    Reflect.set(instance, "snapshot", null);
    Reflect.set(instance, "subscribers", new Set());
    Reflect.set(instance, "state", null);
    return instance;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "PUT" && url.pathname === "/weights") return this.handlePut(request);
    if (request.method === "GET" && url.pathname === "/weights") return this.handleGetLatest();
    if (request.method === "GET" && url.pathname === "/stream") return this.handleStream();
    return notFoundResponse();
  }

  private async handlePut(request: Request): Promise<Response> {
    const body: unknown = await request.json();
    if (!isHorseWeightSnapshot(body)) return badRequestResponse();
    this.snapshot = body;
    this.broadcast(body);
    return okJsonResponse();
  }

  private handleGetLatest(): Response {
    if (this.snapshot === null) return new Response(null, { status: 204 });
    return snapshotJsonResponse(this.snapshot);
  }

  private handleStream(): Response {
    const encoder = new TextEncoder();
    const stream = new ReadableStream<Uint8Array>({
      start: (controller) => this.onSubscribe({ controller, encoder }),
      cancel: () => this.onUnsubscribe(),
    });
    return new Response(stream, { headers: SSE_HEADERS });
  }

  private onSubscribe(subscriber: Subscriber): void {
    this.subscribers.add(subscriber);
    subscriber.controller.enqueue(subscriber.encoder.encode(`retry: ${SSE_RETRY_MS}\n\n`));
    if (this.snapshot !== null) {
      subscriber.controller.enqueue(
        subscriber.encoder.encode(encodeEvent("weights", JSON.stringify(this.snapshot))),
      );
    }
  }

  private onUnsubscribe(): void {
    // Best-effort cleanup; orphaned subscribers are also pruned on the next
    // broadcast when the controller throws on enqueue.
  }

  private broadcast(snapshot: HorseWeightSnapshot): void {
    const payload = encodeEvent("weights", JSON.stringify(snapshot));
    const dead: Subscriber[] = [];
    this.subscribers.forEach((sub) => this.deliverOrCollectDead(sub, payload, dead));
    dead.forEach((sub) => this.subscribers.delete(sub));
  }

  private deliverOrCollectDead(sub: Subscriber, payload: string, dead: Subscriber[]): void {
    try {
      sub.controller.enqueue(sub.encoder.encode(payload));
    } catch {
      dead.push(sub);
    }
  }
}

export const writeHorseWeightSnapshotToStub = async (args: {
  stub: HorseWeightDOStub;
  snapshot: HorseWeightSnapshot;
}): Promise<void> => {
  await args.stub.fetch(WEIGHTS_URL, {
    body: JSON.stringify(args.snapshot),
    method: "PUT",
  });
};

export const proxyHorseWeightStreamFromStub = async (stub: HorseWeightDOStub): Promise<Response> =>
  stub.fetch(STREAM_URL, { method: "GET" });

export const proxyHorseWeightLatestFromStub = async (stub: HorseWeightDOStub): Promise<Response> =>
  stub.fetch(WEIGHTS_URL, { method: "GET" });
