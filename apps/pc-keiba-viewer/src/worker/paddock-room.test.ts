// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import { DurableObject } from "../test-stubs/cloudflare-workers";
import { PaddockRoom } from "./paddock-room";

type StoragePutFn = (key: string, value: unknown) => Promise<void>;

class FakeWebSocket {
  public readonly sentMessages: string[] = [];
  public readonly listeners = new Map<string, () => void>();

  accept(): void {}

  send(message: string): void {
    this.sentMessages.push(message);
  }

  addEventListener(event: string, handler: () => void): void {
    this.listeners.set(event, handler);
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

setupWebSocketGlobals();

it("paddock-room-extends-durable-object-base-class", () => {
  const ctx: PcKeibaDurableObjectState = {
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
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
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
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
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
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
    blockConcurrencyWhile: <T>(cb: () => Promise<T>): Promise<T> => cb(),
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
