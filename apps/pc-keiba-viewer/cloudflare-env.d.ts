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
    HYPERDRIVE?: PcKeibaHyperdriveBinding;
    PADDOCK_ROOM?: PcKeibaDurableObjectNamespace;
    PADDOCK_STATE_KV?: PcKeibaKvNamespace;
    PC_KEIBA_PADDOCK_DISCORD_WEBHOOK_URL?: string;
  }
}
