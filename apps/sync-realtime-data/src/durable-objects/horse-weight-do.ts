// run with: bun run test
// Durable Object that holds the latest horse-weight snapshot per race_key and
// broadcasts updates via Server-Sent Events. The snapshot is persisted via
// `state.storage` so it survives DO hibernation; D1 remains the authoritative
// store for the underlying weight rows.
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

interface HorseWeightStorageLike {
  get: (key: string) => Promise<HorseWeightSnapshot | undefined>;
  put: (key: string, value: HorseWeightSnapshot) => Promise<void>;
}

interface HorseWeightStateLike {
  storage: HorseWeightStorageLike;
  blockConcurrencyWhile: (callback: () => Promise<void>) => Promise<void>;
}

interface CreateForTestWithStorageParams {
  state: HorseWeightStateLike;
}

const SSE_RETRY_MS = 5000;
const SSE_HEADERS: Record<string, string> = {
  "Cache-Control": "no-cache, no-transform",
  Connection: "keep-alive",
  "Content-Type": "text/event-stream",
};
const STREAM_URL = "https://horse-weight-do/stream";
const WEIGHTS_URL = "https://horse-weight-do/weights";
const STORAGE_KEY = "snapshot";

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
  // The DurableObjectState handle is retained so we can persist the snapshot
  // through hibernation by mirroring it into state.storage on every PUT and
  // hydrating from storage on construction via blockConcurrencyWhile.
  private readonly state: HorseWeightStateLike;

  constructor(state: DurableObjectState, _env: unknown) {
    const stateLike: HorseWeightStateLike = {
      blockConcurrencyWhile: (callback) => state.blockConcurrencyWhile(callback),
      storage: {
        get: (key) => state.storage.get<HorseWeightSnapshot>(key),
        put: (key, value) => state.storage.put<HorseWeightSnapshot>(key, value),
      },
    };
    this.state = stateLike;
    // Block routing until the persisted snapshot (if any) is reloaded so that
    // SSR fetches arriving immediately after construction never race the
    // hydration and observe an empty cache. The promise is intentionally not
    // awaited here (constructors cannot be async); the Cloudflare DO runtime
    // gates incoming fetches until blockConcurrencyWhile resolves.
    void stateLike.blockConcurrencyWhile(async () => {
      const stored = await stateLike.storage.get(STORAGE_KEY);
      if (stored !== undefined) this.snapshot = stored;
    });
  }

  // Test factory that bypasses the Cloudflare DO constructor signature so unit
  // tests do not need to forge a full DurableObjectState. A no-op storage is
  // installed so handlePut can still call state.storage.put without hitting
  // the real Cloudflare runtime. Only call from tests.
  static createForTest(): HorseWeightDO {
    const instance: HorseWeightDO = Object.create(HorseWeightDO.prototype);
    const noopStorage: HorseWeightStorageLike = {
      get: async () => undefined,
      put: async () => undefined,
    };
    const noopState: HorseWeightStateLike = {
      blockConcurrencyWhile: (callback) => callback(),
      storage: noopStorage,
    };
    Reflect.set(instance, "snapshot", null);
    Reflect.set(instance, "subscribers", new Set());
    Reflect.set(instance, "state", noopState);
    return instance;
  }

  // Test factory that mirrors the real constructor's hydration path against a
  // typed fake state so hydration via blockConcurrencyWhile and storage.get
  // can be exercised without forging a full DurableObjectState. Returns the
  // hydration promise so tests can await full readiness before asserting.
  static async createForTestWithStorage(
    params: CreateForTestWithStorageParams,
  ): Promise<HorseWeightDO> {
    const instance: HorseWeightDO = Object.create(HorseWeightDO.prototype);
    Reflect.set(instance, "snapshot", null);
    Reflect.set(instance, "subscribers", new Set());
    Reflect.set(instance, "state", params.state);
    await params.state.blockConcurrencyWhile(async () => {
      const stored = await params.state.storage.get(STORAGE_KEY);
      if (stored !== undefined) Reflect.set(instance, "snapshot", stored);
    });
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
    await this.state.storage.put(STORAGE_KEY, body);
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

export const HORSE_WEIGHT_STORAGE_KEY = STORAGE_KEY;
