// Generic HTTP/1.1 socket fallback for legacy origins that Workers fetch cannot reach.

import { connect } from "cloudflare:sockets";
import type { CoreHttpRequest } from "./rust-core";

interface SocketLike {
  close(): Promise<void>;
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

export type SocketFailureStage = "connect" | "write" | "read" | "parse";

export type SocketParseReason = "framing" | "chunked" | "limit" | "content-length";

// Carries only the failure class and status code, never response bytes (README).
export class SocketParseError extends Error {
  readonly reason: SocketParseReason;
  readonly status?: number;

  constructor(reason: SocketParseReason, status?: number) {
    super("Socket HTTP response could not be parsed");
    this.name = "SocketParseError";
    this.reason = reason;
    this.status = status;
  }
}

export class SocketFallbackError extends Error {
  readonly bytesRead?: number;
  readonly parse?: SocketParseError;
  readonly stage: SocketFailureStage;

  constructor(stage: SocketFailureStage, bytesRead?: number, parse?: SocketParseError) {
    super("Socket HTTP fallback failed");
    this.name = "SocketFallbackError";
    this.bytesRead = bytesRead;
    this.parse = parse;
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
    if (lineEnd < 0) throw new SocketParseError("chunked");
    const sizeText: string =
      new TextDecoder().decode(bytes.slice(offset, lineEnd)).split(";", 1)[0] ?? "";
    if (!/^[0-9a-fA-F]+$/.test(sizeText)) throw new SocketParseError("chunked");
    const size: number = Number.parseInt(sizeText, 16);
    offset = lineEnd + 2;
    if (size === 0) break;
    if (outputSize + size > maxBytes || offset + size + 2 > bytes.length)
      throw new SocketParseError("limit");
    chunks.push(bytes.slice(offset, offset + size));
    outputSize += size;
    offset += size;
    if (bytes[offset] !== 13 || bytes[offset + 1] !== 10) throw new SocketParseError("chunked");
    offset += 2;
  }
  return concatenate(chunks, outputSize);
};

export const parseSocketHttpResponse = (wire: Uint8Array, maxBytes: number): Response => {
  const headerEnd: number = indexOf(wire, HEADER_END);
  if (headerEnd < 0 || headerEnd > MAX_HEADER_BYTES) throw new SocketParseError("framing");
  const headerText: string = new TextDecoder().decode(wire.slice(0, headerEnd));
  const lines: string[] = headerText.split(CRLF);
  const statusMatch: RegExpMatchArray | null =
    lines.shift()?.match(/^HTTP\/1\.[01] ([1-5][0-9]{2})(?: |$)/) ?? null;
  if (statusMatch === null) throw new SocketParseError("framing");
  const status: number = Number(statusMatch[1]);
  const headers: Headers = new Headers();
  for (const line of lines) {
    const separator: number = line.indexOf(":");
    if (separator < 1) throw new SocketParseError("framing", status);
    headers.append(line.slice(0, separator).trim(), line.slice(separator + 1).trim());
  }
  const encodedBody: Uint8Array = wire.slice(headerEnd + HEADER_END.length);
  const body: Uint8Array<ArrayBuffer> =
    headers.get("Transfer-Encoding")?.toLowerCase() === "chunked"
      ? decodeChunked(encodedBody, maxBytes)
      : concatenate([encodedBody], encodedBody.length);
  if (body.length > maxBytes) throw new SocketParseError("limit", status);
  const declared: string | null = headers.get("Content-Length");
  if (declared !== null && Number(declared) !== body.length)
    throw new SocketParseError("content-length", status);
  return new Response(body.buffer, { headers, status });
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
  } catch (error: unknown) {
    throw new SocketFallbackError(
      "parse",
      size,
      error instanceof SocketParseError ? error : undefined,
    );
  }
};
