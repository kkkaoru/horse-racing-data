// Run with bun (vitest).
import { expect, it } from "vitest";

import { closeSocket, trySend } from "./websocket-broadcast";

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

it("try-send-delivers-message-to-open-socket", () => {
  const socket = new FakeWebSocket();
  trySend(socket, "hello");
  expect(socket.sentMessages.length).toBe(1);
});

it("try-send-closes-socket-when-send-throws", () => {
  const socket = new ThrowingSendWebSocket();
  trySend(socket, "hello");
  expect(socket.closeCount).toBe(1);
});

it("close-socket-calls-close-on-open-socket", () => {
  const socket = new FakeWebSocket();
  closeSocket(socket);
  expect(socket.closeCount).toBe(1);
});

it("close-socket-swallows-error-when-close-throws", () => {
  const socket = new ThrowingCloseWebSocket();
  closeSocket(socket);
  expect(socket.sendCount).toBe(0);
});
