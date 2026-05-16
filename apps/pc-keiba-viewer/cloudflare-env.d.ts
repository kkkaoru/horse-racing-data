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
    get<T = unknown>(key: string, options: { type: "json" }): Promise<T | null>;
    put(key: string, value: string): Promise<void>;
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
    HYPERDRIVE?: PcKeibaHyperdriveBinding;
    PADDOCK_ROOM?: PcKeibaDurableObjectNamespace;
    PADDOCK_STATE_KV?: PcKeibaKvNamespace;
    PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_BOT_NAME?: string;
    PC_KEIBA_EXTERNAL_PADDOCK_DISCORD_WEBHOOK_URL?: string;
    PC_KEIBA_PADDOCK_DISCORD_BOT_NAME?: string;
    PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL?: string;
  }
}
