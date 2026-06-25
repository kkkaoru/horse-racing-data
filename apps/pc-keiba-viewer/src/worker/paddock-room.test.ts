// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import { DurableObject } from "../test-stubs/cloudflare-workers";
import { PaddockRoom } from "./paddock-room";

type StoragePutFn = (key: string, value: unknown) => Promise<void>;
type AcceptWebSocketFn = (ws: WebSocket) => void;

const FAKE_SOCKET_URL = "ws://localhost:9/x";

class FakeWebSocket extends WebSocket {
  public readonly sentMessages: string[] = [];
  public closeCount = 0;

  constructor() {
    super(FAKE_SOCKET_URL);
  }

  override send(message: string): void {
    this.sentMessages.push(message);
  }

  override close(): void {
    this.closeCount += 1;
  }
}

class ThrowingSendWebSocket extends WebSocket {
  public closeCount = 0;

  constructor() {
    super(FAKE_SOCKET_URL);
  }

  override send(): void {
    throw new Error("send failed");
  }

  override close(): void {
    this.closeCount += 1;
  }
}

class ThrowingCloseWebSocket extends WebSocket {
  public sendCount = 0;

  constructor() {
    super(FAKE_SOCKET_URL);
  }

  override send(): void {
    this.sendCount += 1;
  }

  override close(): void {
    throw new Error("close failed");
  }
}

const setupWebSocketGlobals = (): void => {
  Object.defineProperty(globalThis, "WebSocketPair", {
    configurable: true,
    value: class {
      0 = new FakeWebSocket();
      1 = new FakeWebSocket();
    },
    writable: true,
  });
};

// `Upgrade` is a forbidden request header, so the Request constructor strips it.
// Re-attach a Headers object that reports the websocket upgrade to exercise the
// upgrade-required branch.
const withUpgradeHeader = (request: Request): Request => {
  const headers = new Headers();
  headers.set("upgrade", "websocket");
  Object.defineProperty(request, "headers", { configurable: true, value: headers });
  return request;
};

setupWebSocketGlobals();

it("paddock-room-extends-durable-object-base-class", () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const env = {};
  const instance = new PaddockRoom(ctx, env);
  expect(instance).toBeInstanceOf(DurableObject);
});

it("paddock-room-prototype-chain-points-to-durable-object", () => {
  const proto = Object.getPrototypeOf(PaddockRoom.prototype);
  expect(proto.constructor.name).toBe("DurableObject");
});

it("paddock-room-fetch-returns-400-when-race-key-missing", async () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const response = await room.fetch(new Request("https://paddock-room/"));
  expect(response.status).toBe(400);
});

it("paddock-room-fetch-returns-405-for-delete-method", async () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const response = await room.fetch(
    new Request("https://paddock-room/?raceKey=20260601:05:11", { method: "DELETE" }),
  );
  expect(response.status).toBe(405);
});

it("paddock-room-fetch-get-returns-initial-state-from-ctx-storage", async () => {
  const putMock = vi.fn<StoragePutFn>().mockResolvedValue(undefined);
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: putMock,
    },
  };
  const room = new PaddockRoom(ctx, {});
  const response = await room.fetch(
    new Request("https://paddock-room/?raceKey=20260601:05:11", { method: "GET" }),
  );
  expect(response.status).toBe(200);
  expect(putMock).toHaveBeenCalledTimes(1);
});

it("paddock-room-fetch-ws-upgrade-returns-101-and-accepts-socket", async () => {
  const acceptMock = vi.fn<AcceptWebSocketFn>();
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: acceptMock,
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const response = await room.fetch(
    withUpgradeHeader(new Request("https://paddock-room/ws?raceKey=20260601:05:11")),
  );
  expect(response.status).toBe(101);
  expect(acceptMock).toHaveBeenCalledTimes(1);
});

it("paddock-room-fetch-ws-without-upgrade-header-returns-426", async () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const response = await room.fetch(new Request("https://paddock-room/ws?raceKey=20260601:05:11"));
  expect(response.status).toBe(426);
});

it("paddock-room-broadcast-sends-to-hibernation-sockets-on-post", async () => {
  const liveSocket = new FakeWebSocket();
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [liveSocket],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const response = await room.fetch(
    new Request("https://paddock-room/?raceKey=20260601:05:11", {
      body: JSON.stringify({
        category: "paddock",
        delta: 1,
        horseName: "Test",
        horseNumber: "01",
        type: "score",
      }),
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(liveSocket.sentMessages.length).toBe(1);
});

it("paddock-room-broadcast-closes-socket-when-send-throws-on-post", async () => {
  const failingSocket = new ThrowingSendWebSocket();
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [failingSocket],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  await room.fetch(
    new Request("https://paddock-room/?raceKey=20260601:05:11", {
      body: JSON.stringify({
        category: "paddock",
        delta: 1,
        horseName: "Test",
        horseNumber: "01",
        type: "score",
      }),
      method: "POST",
    }),
  );
  expect(failingSocket.closeCount).toBe(1);
});

it("paddock-room-web-socket-message-resolves-without-throwing", async () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const result = await room.webSocketMessage();
  expect(result).toBe(undefined);
});

it("paddock-room-web-socket-close-closes-socket", async () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const socket = new FakeWebSocket();
  await room.webSocketClose(socket);
  expect(socket.closeCount).toBe(1);
});

it("paddock-room-web-socket-error-closes-socket", async () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const socket = new FakeWebSocket();
  await room.webSocketError(socket);
  expect(socket.closeCount).toBe(1);
});

it("paddock-room-web-socket-close-swallows-error-when-close-throws", async () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<AcceptWebSocketFn>(),
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
    getWebSockets: (): WebSocket[] => [],
    storage: {
      get: <T>(): Promise<T | undefined> => Promise.resolve(undefined),
      put: vi.fn<StoragePutFn>().mockResolvedValue(undefined),
    },
  };
  const room = new PaddockRoom(ctx, {});
  const socket = new ThrowingCloseWebSocket();
  const result = await room.webSocketClose(socket);
  expect(result).toBe(undefined);
});
