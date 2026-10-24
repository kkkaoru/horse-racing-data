// Run with bun.
import { describe, expect, it, vi } from "vitest";
import { parseSocketHttpResponse, socketFetch, type SocketConnector } from "./socket-fetch";
import { bytes } from "./test-fixtures";

const wire = (headers: string, body: string): Uint8Array<ArrayBuffer> =>
  bytes(`HTTP/1.1 200 OK\r\n${headers}\r\n\r\n${body}`);

describe("legacy origin socket HTTP", () => {
  it("parses bounded content-length and chunked responses", async () => {
    const fixed: Response = parseSocketHttpResponse(wire("Content-Length: 2", "OK"), 8);
    expect(fixed.status).toBe(200);
    await expect(fixed.text()).resolves.toBe("OK");

    const chunked: Response = parseSocketHttpResponse(
      wire("Transfer-Encoding: chunked", "2\r\nOK\r\n1;extension=x\r\n!\r\n0\r\n\r\n"),
      8,
    );
    await expect(chunked.text()).resolves.toBe("OK!");
  });

  it.each([
    bytes("bad"),
    bytes("HTTP/2 200 OK\r\n\r\n"),
    bytes("HTTP/1.1 200 OK\r\nbad\r\n\r\n"),
    wire("Content-Length: 3", "OK"),
    wire("Transfer-Encoding: chunked", "x\r\n"),
    wire("Transfer-Encoding: chunked", "2\r\nO"),
    wire("Transfer-Encoding: chunked", "1\r\nAxx0\r\n\r\n"),
  ])("rejects malformed HTTP wire data", (value: Uint8Array) => {
    expect(() => parseSocketHttpResponse(value, 8)).toThrow();
  });

  it("classifies parse failures without exposing the body", () => {
    expect(() => parseSocketHttpResponse(wire("Content-Length: 9", "123456789"), 8)).toThrow(
      expect.objectContaining({ reason: "limit", status: 200 }),
    );
    expect(() => parseSocketHttpResponse(wire("Content-Length: 3", "OK"), 8)).toThrow(
      expect.objectContaining({ reason: "content-length", status: 200 }),
    );
    expect(() => parseSocketHttpResponse(bytes("bad"), 8)).toThrow(
      expect.objectContaining({ reason: "framing" }),
    );
    expect(() => parseSocketHttpResponse(wire("Transfer-Encoding: chunked", "x\r\n"), 8)).toThrow(
      expect.objectContaining({ reason: "chunked" }),
    );
  });

  it("carries the parse failure class on a socket fallback error", async () => {
    const connector: SocketConnector = () => ({
      close: async (): Promise<void> => undefined,
      readable: new ReadableStream<Uint8Array>({
        start(controller): void {
          controller.enqueue(
            bytes("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 9\r\n\r\n123456789"),
          );
          controller.close();
        },
      }),
      writable: new WritableStream<Uint8Array>(),
    });
    await expect(
      socketFetch({ url: "https://example.test/x", userAgent: "test" }, 8, connector),
    ).rejects.toMatchObject({ stage: "parse", parse: { reason: "limit", status: 503 } });
  });

  it("sends a credential-contained target only through the socket", async () => {
    let requestBytes: Uint8Array = new Uint8Array();
    const close = vi.fn(async (): Promise<void> => undefined);
    const connector: SocketConnector = (address, options) => {
      expect(address).toEqual({ hostname: "example.test", port: 443 });
      expect(options.secureTransport).toBe("on");
      return {
        close,
        readable: new ReadableStream<Uint8Array>({
          start(controller): void {
            controller.enqueue(wire("Content-Length: 2", "OK"));
            controller.close();
          },
        }),
        writable: new WritableStream<Uint8Array>({
          write(chunk): void {
            requestBytes = chunk;
          },
        }),
      };
    };
    const response: Response = await socketFetch(
      { url: "https://example.test/private?opaque=value", userAgent: "UmaConn/test" },
      8,
      connector,
    );
    expect(new TextDecoder().decode(requestBytes)).toContain(
      "GET /private?opaque=value HTTP/1.1\r\nHost: example.test\r\nUser-Agent: UmaConn/test",
    );
    await expect(response.text()).resolves.toBe("OK");
    expect(close).toHaveBeenCalledOnce();
  });

  it("selects plaintext and explicit ports", async () => {
    const connector: SocketConnector = (address, options) => {
      expect(address).toEqual({ hostname: "example.test", port: 8080 });
      expect(options.secureTransport).toBe("off");
      return {
        close: async (): Promise<void> => undefined,
        readable: new ReadableStream<Uint8Array>({
          start(controller): void {
            controller.enqueue(wire("Content-Length: 0", ""));
            controller.close();
          },
        }),
        writable: new WritableStream<Uint8Array>(),
      };
    };
    await expect(
      socketFetch({ url: "http://example.test:8080/path", userAgent: "test" }, 8, connector),
    ).resolves.toBeInstanceOf(Response);
  });

  it.each(["ftp://example.test/x", "http://user@example.test/x"])(
    "rejects invalid socket destinations",
    async (url: string) => {
      await expect(socketFetch({ url, userAgent: "test" }, 8)).rejects.toThrow("Invalid socket");
    },
  );

  it("classifies connector, writer, reader, and parser failures", async () => {
    const request = { url: "http://example.test/path", userAgent: "test" };
    const connectFailure: SocketConnector = () => {
      throw new Error("connect");
    };
    await expect(socketFetch(request, 8, connectFailure)).rejects.toMatchObject({
      stage: "connect",
    });

    const socket = (
      readable: ReadableStream<Uint8Array>,
      writable: WritableStream<Uint8Array>,
    ) => ({ close: async (): Promise<void> => undefined, readable, writable });
    const writeFailure: SocketConnector = () =>
      socket(
        new ReadableStream<Uint8Array>(),
        new WritableStream<Uint8Array>({
          write(): never {
            throw new Error("write");
          },
        }),
      );
    await expect(socketFetch(request, 8, writeFailure)).rejects.toMatchObject({ stage: "write" });

    const readFailure: SocketConnector = () =>
      socket(
        new ReadableStream<Uint8Array>({
          start(controller): void {
            controller.error(new Error("read"));
          },
        }),
        new WritableStream<Uint8Array>(),
      );
    await expect(socketFetch(request, 8, readFailure)).rejects.toMatchObject({ stage: "read" });

    const parseFailure: SocketConnector = () =>
      socket(
        new ReadableStream<Uint8Array>({
          start(controller): void {
            controller.enqueue(bytes("bad"));
            controller.close();
          },
        }),
        new WritableStream<Uint8Array>(),
      );
    await expect(socketFetch(request, 8, parseFailure)).rejects.toMatchObject({ stage: "parse" });
  });
});
