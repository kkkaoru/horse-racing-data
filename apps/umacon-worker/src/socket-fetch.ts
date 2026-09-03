// Generic HTTP/1.1 socket fallback for legacy origins that Workers fetch cannot reach.

import { connect } from "cloudflare:sockets";
import type { CoreHttpRequest } from "./rust-core";

interface SocketLike {
  close(): Promise<void>;
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

export type SocketFailureStage = "connect" | "write" | "read" | "parse";

export class SocketFallbackError extends Error {
  readonly bytesRead?: number;
  readonly stage: SocketFailureStage;

  constructor(stage: SocketFailureStage, bytesRead?: number) {
    super("Socket HTTP fallback failed");
    this.name = "SocketFallbackError";
    this.bytesRead = bytesRead;
    this.stage = stage;
  }
}

export type SocketConnector = (
  address: { hostname: string; port: number },
  options: { allowHalfOpen: false; secureTransport: "off" | "on" },
) => SocketLike;

const CRLF: string = "\r\n";
const HEADER_END: Uint8Array = new TextEncoder().encode(`${CRLF}${CRLF}`);
const MAX_HEADER_BYTES: number = 32 * 1024;

const indexOf = (bytes: Uint8Array, needle: Uint8Array, start: number = 0): number => {
  for (let offset: number = start; offset + needle.length <= bytes.length; offset += 1) {
    if (needle.every((value: number, index: number): boolean => bytes[offset + index] === value))
      return offset;
  }
  return -1;
};

const concatenate = (chunks: Uint8Array[], size: number): Uint8Array<ArrayBuffer> => {
  const output: Uint8Array<ArrayBuffer> = new Uint8Array(size);
  chunks.reduce((offset: number, chunk: Uint8Array): number => {
    output.set(chunk, offset);
    return offset + chunk.length;
  }, 0);
  return output;
};

const decodeChunked = (bytes: Uint8Array, maxBytes: number): Uint8Array<ArrayBuffer> => {
  const chunks: Uint8Array[] = [];
  let outputSize: number = 0;
  let offset: number = 0;
  while (true) {
    const lineEnd: number = indexOf(bytes, new Uint8Array([13, 10]), offset);
    if (lineEnd < 0) throw new Error("Invalid chunked HTTP response");
    const sizeText: string =
      new TextDecoder().decode(bytes.slice(offset, lineEnd)).split(";", 1)[0] ?? "";
    if (!/^[0-9a-fA-F]+$/.test(sizeText)) throw new Error("Invalid chunked HTTP response");
    const size: number = Number.parseInt(sizeText, 16);
    offset = lineEnd + 2;
    if (size === 0) break;
    if (outputSize + size > maxBytes || offset + size + 2 > bytes.length)
      throw new Error("Socket HTTP response exceeded its limit");
    chunks.push(bytes.slice(offset, offset + size));
    outputSize += size;
    offset += size;
    if (bytes[offset] !== 13 || bytes[offset + 1] !== 10)
      throw new Error("Invalid chunked HTTP response");
    offset += 2;
  }
  return concatenate(chunks, outputSize);
};

export const parseSocketHttpResponse = (wire: Uint8Array, maxBytes: number): Response => {
  const headerEnd: number = indexOf(wire, HEADER_END);
  if (headerEnd < 0 || headerEnd > MAX_HEADER_BYTES)
    throw new Error("Invalid socket HTTP response");
  const headerText: string = new TextDecoder().decode(wire.slice(0, headerEnd));
  const lines: string[] = headerText.split(CRLF);
  const statusMatch: RegExpMatchArray | null =
    lines.shift()?.match(/^HTTP\/1\.[01] ([1-5][0-9]{2})(?: |$)/) ?? null;
  if (statusMatch === null) throw new Error("Invalid socket HTTP response");
  const headers: Headers = new Headers();
  for (const line of lines) {
    const separator: number = line.indexOf(":");
    if (separator < 1) throw new Error("Invalid socket HTTP response");
    headers.append(line.slice(0, separator).trim(), line.slice(separator + 1).trim());
  }
  const encodedBody: Uint8Array = wire.slice(headerEnd + HEADER_END.length);
  const body: Uint8Array<ArrayBuffer> =
    headers.get("Transfer-Encoding")?.toLowerCase() === "chunked"
      ? decodeChunked(encodedBody, maxBytes)
      : concatenate([encodedBody], encodedBody.length);
  if (body.length > maxBytes) throw new Error("Socket HTTP response exceeded its limit");
  const declared: string | null = headers.get("Content-Length");
  if (declared !== null && Number(declared) !== body.length)
    throw new Error("Invalid socket HTTP content length");
  return new Response(body.buffer, { headers, status: Number(statusMatch[1]) });
};

export const socketFetch = async (
  request: CoreHttpRequest,
  maxBytes: number,
  connector: SocketConnector = connect,
): Promise<Response> => {
  const url: URL = new URL(request.url);
  if ((url.protocol !== "http:" && url.protocol !== "https:") || url.username || url.password)
    throw new Error("Invalid socket HTTP request");
  const secure: boolean = url.protocol === "https:";
  const port: number = url.port ? Number(url.port) : secure ? 443 : 80;
  let socket: SocketLike;
  try {
    socket = connector(
      { hostname: url.hostname, port },
      { allowHalfOpen: false, secureTransport: secure ? "on" : "off" },
    );
  } catch {
    throw new SocketFallbackError("connect");
  }
  const writer: WritableStreamDefaultWriter<Uint8Array> = socket.writable.getWriter();
  const target: string = `${url.pathname}${url.search}`;
  try {
    await writer.write(
      new TextEncoder().encode(
        `GET ${target} HTTP/1.1${CRLF}Host: ${url.host}${CRLF}User-Agent: ${request.userAgent}${CRLF}Accept: */*${CRLF}Connection: close${CRLF}${CRLF}`,
      ),
    );
    writer.releaseLock();
  } catch {
    await socket.close();
    throw new SocketFallbackError("write");
  }

  const reader: ReadableStreamDefaultReader<Uint8Array> = socket.readable.getReader();
  const chunks: Uint8Array[] = [];
  let size: number = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.length;
      if (size > maxBytes + MAX_HEADER_BYTES) throw new Error("response too large");
      chunks.push(next.value);
    }
  } catch {
    await socket.close();
    throw new SocketFallbackError("read");
  }
  await socket.close();
  try {
    return parseSocketHttpResponse(concatenate(chunks, size), maxBytes);
  } catch {
    throw new SocketFallbackError("parse", size);
  }
};
