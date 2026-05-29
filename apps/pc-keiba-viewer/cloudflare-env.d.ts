export type PcKeibaHyperdriveBinding = {
  connectionString: string;
};

declare global {
  interface PcKeibaDurableObjectId {
    toString(): string;
  }

  interface PcKeibaDurableObjectStub {
    fetch(request: Request): Promise<Response>;
  }

  interface PcKeibaDurableObjectNamespace {
    get(id: PcKeibaDurableObjectId): PcKeibaDurableObjectStub;
    idFromName(name: string): PcKeibaDurableObjectId;
  }

  interface PcKeibaDurableObjectState {
    blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T>;
    storage: {
      get<T = unknown>(key: string): Promise<T | undefined>;
      put(key: string, value: unknown): Promise<void>;
    };
  }

  interface PcKeibaKvNamespace {
    delete(key: string): Promise<void>;
    get(key: string): Promise<string | null>;
    get<T = unknown>(key: string, options: { type: "json" }): Promise<T | null>;
    put(
      key: string,
      value: string,
      options?: { expirationTtl?: number; metadata?: Record<string, string> },
    ): Promise<void>;
  }

  interface PcKeibaQueue<Body = unknown> {
    send(body: Body, options?: { delaySeconds?: number }): Promise<void>;
    sendBatch(messages: Array<{ body: Body }>): Promise<void>;
  }

  interface PcKeibaMessage<Body = unknown> {
    ack(): void;
    body: Body;
    retry(): void;
  }

  interface PcKeibaMessageBatch<Body = unknown> {
    messages: PcKeibaMessage<Body>[];
    queue: string;
  }

  interface PcKeibaExecutionContext {
    waitUntil(promise: Promise<unknown>): void;
  }

  interface PcKeibaR2Object {
    body: ReadableStream;
    arrayBuffer(): Promise<ArrayBuffer>;
    text(): Promise<string>;
    json<T = unknown>(): Promise<T>;
    httpMetadata?: { contentType?: string };
    customMetadata?: Record<string, string>;
    size: number;
    uploaded: Date;
  }

  interface PcKeibaR2PutOptions {
    httpMetadata?: { contentType?: string };
    customMetadata?: Record<string, string>;
  }

  interface PcKeibaR2Bucket {
    get(key: string): Promise<PcKeibaR2Object | null>;
    put(
      key: string,
      value: ArrayBuffer | ArrayBufferView | ReadableStream | string,
      options?: PcKeibaR2PutOptions,
    ): Promise<PcKeibaR2Object>;
    delete(key: string): Promise<void>;
    head(key: string): Promise<PcKeibaR2Object | null>;
  }

  interface PcKeibaD1Result<T = unknown> {
    results: T[];
    success: boolean;
    meta?: Record<string, unknown>;
  }

  interface PcKeibaD1RunResult {
    success: boolean;
    meta?: Record<string, unknown>;
  }

  interface PcKeibaD1PreparedStatement {
    bind(...values: unknown[]): PcKeibaD1PreparedStatement;
    all<T = unknown>(): Promise<PcKeibaD1Result<T>>;
    first<T = unknown>(): Promise<T | null>;
    run(): Promise<PcKeibaD1RunResult>;
  }

  interface PcKeibaD1Database {
    prepare(query: string): PcKeibaD1PreparedStatement;
    batch<T = unknown>(statements: PcKeibaD1PreparedStatement[]): Promise<PcKeibaD1Result<T>[]>;
    exec(query: string): Promise<PcKeibaD1RunResult>;
  }

  const WebSocketPair: {
    new (): { 0: WebSocket; 1: WebSocket };
  };

  interface WebSocket {
    accept(): void;
  }

  interface ResponseInit {
    webSocket?: WebSocket;
  }

  interface CloudflareEnv {
    FINISH_POSITION_MODELS?: PcKeibaR2Bucket;
    DETAIL_SECTION_CACHE_KV?: PcKeibaKvNamespace;
    DETAIL_SECTION_CACHE_QUEUE?: PcKeibaQueue;
    HYPERDRIVE?: PcKeibaHyperdriveBinding;
    PADDOCK_ROOM?: PcKeibaDurableObjectNamespace;
    PADDOCK_STATE_KV?: PcKeibaKvNamespace;
    PC_KEIBA_DETAIL_SECTION_CACHE_AFTER_START_SECONDS?: string;
    PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_BOT_NAME?: string;
    PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_WEBHOOK_URL?: string;
    PC_KEIBA_PADDOCK_DISCORD_BOT_NAME?: string;
    PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL?: string;
    PC_KEIBA_RACE_AI_ACCENT_COLOR?: string;
    PC_KEIBA_RACE_AI_ICON_URL?: string;
    PC_KEIBA_RACE_AI_NAME?: string;
    PC_KEIBA_RACE_TREND_CACHE_AFTER_START_SECONDS?: string;
    PC_KEIBA_RUNNING_STYLE_CACHE_ORIGIN?: string;
    RACE_TREND_ROOM?: PcKeibaDurableObjectNamespace;
    REALTIME_DB?: PcKeibaD1Database;
    REALTIME_FEATURES?: { fetch: typeof fetch };
    REALTIME_FEATURES_DB?: PcKeibaD1Database;
    REALTIME_HOT?: { fetch: typeof fetch };
    REALTIME_HOT_DB?: PcKeibaD1Database;
  }
}
