// Run with bun.
import { describe, expect, it } from "vitest";
import type { JvFetch } from "./acquisition";
import {
  acquireRealtimeData,
  buildRealtimeAuthorizationBody,
  buildRealtimeGateBody,
  validateRealtimeQuery,
} from "./realtime";
import { TEST_CORE_CONFIG } from "./test-fixtures";

const encoder = new TextEncoder();
const SOURCES = TEST_CORE_CONFIG;
const QUERY = { dataSpec: "0B14", key: "20260830" };
const AUTH_BODY =
  "AN0001\r\nDB0002\r\nDL0003\r\nUB0004\r\nUL0005\r\nOB0006\r\nOL0007\r\nCB0008\r\n" +
  "IT              \r\n";

interface Call {
  init?: RequestInit;
  input: RequestInfo | URL;
}

const response = (body: Uint8Array, status: number = 200): Response =>
  new Response(body, { status });

const compress = async (input: string): Promise<Uint8Array> => {
  const source = new ReadableStream<Uint8Array>({
    start(controller): void {
      controller.enqueue(encoder.encode(input));
      controller.close();
    },
  });
  return new Uint8Array(
    await new Response(source.pipeThrough(new CompressionStream("deflate"))).arrayBuffer(),
  );
};

const envelope = async (header: string, body: string): Promise<Uint8Array> => {
  const prefix = encoder.encode(`${header}\r\n`);
  const compressed = await compress(body);
  const result = new Uint8Array(prefix.length + compressed.length);
  result.set(prefix);
  result.set(compressed, prefix.length);
  return result;
};

const authorization = (status: number = 0, body: string = AUTH_BODY): Promise<Uint8Array> =>
  envelope(`02001003${String(status).padStart(3, "0")}05001`, body);

const gate = (status: number = 0, body: string = "WE-one\r\nWE-two\r\n"): Promise<Uint8Array> =>
  envelope(`02001B14${String(status).padStart(3, "0")}00621`, body);

const sequenceFetcher = (responses: Response[]): { calls: Call[]; fetcher: JvFetch } => {
  const calls: Call[] = [];
  return {
    calls,
    fetcher: async (input, init) => {
      calls.push({ init, input });
      const next = responses.shift();
      if (next === undefined) throw new Error("Unexpected fetch");
      return next;
    },
  };
};

describe("JVRTOpen wire contract", () => {
  it("builds the exact authorization and raw service+terminal gate key fields", () => {
    const authorization = buildRealtimeAuthorizationBody(SOURCES, [0x12, 0x34]);
    const gate = buildRealtimeGateBody(SOURCES, QUERY);
    expect(authorization.length).toBeGreaterThan(0);
    expect(gate.length).toBeGreaterThan(0);
    expect(authorization).not.toContain(SOURCES);
    expect(gate).not.toContain(SOURCES);
  });

  it("validates one realtime data spec and official key shapes", () => {
    expect(validateRealtimeQuery(QUERY)).toBe(QUERY);
    expect(() => validateRealtimeQuery({ ...QUERY, dataSpec: "RACE0B14" })).toThrow("dataSpec");
    expect(() => validateRealtimeQuery({ ...QUERY, key: "bad-key" })).toThrow("key");
  });

  it("authorizes, opens GateServlet, and returns every record in order", async () => {
    const mock = sequenceFetcher([response(await authorization()), response(await gate())]);

    const result = await acquireRealtimeData(SOURCES, QUERY, mock.fetcher);

    expect(result.records.map((record) => new TextDecoder().decode(record))).toEqual([
      "WE-one\r\n",
      "WE-two\r\n",
    ]);
    expect(result.transitions).toEqual([
      "configured",
      "session-encoded",
      "authorized",
      "gate-opened",
      "payload-decoded",
    ]);
    expect(mock.calls).toHaveLength(2);
    const authorizationInput = mock.calls[0]!.input;
    const gateInput = mock.calls[1]!.input;
    expect(authorizationInput).toBeTypeOf("string");
    expect(gateInput).toBeTypeOf("string");
    if (typeof authorizationInput !== "string" || typeof gateInput !== "string")
      throw new Error("Expected string URLs");
    expect(authorizationInput).toContain("JVServlet");
    expect(gateInput).toContain("GateServlet");
    expect(mock.calls[0]!.init?.headers).toMatchObject({ "User-Agent": "UNKNOWN:" });
  });

  it("accepts the alternate fourteen-digit authorization timestamp", async () => {
    const dated = sequenceFetcher([
      response(await authorization(0, AUTH_BODY.replace("IT              ", "IT20260830000000"))),
      response(await gate()),
    ]);
    await expect(acquireRealtimeData(SOURCES, QUERY, dated.fetcher)).resolves.toMatchObject({
      status: 0,
    });
  });

  it("rejects HTTP, malformed authorization, and authorization status errors", async () => {
    const http = sequenceFetcher([response(new Uint8Array(), 500)]);
    await expect(acquireRealtimeData(SOURCES, QUERY, http.fetcher)).rejects.toThrow("HTTP error");

    for (const body of [
      encoder.encode("short"),
      encoder.encode("0200100300005001xx"),
      await authorization(0, AUTH_BODY.replace("AN0001", "ANbad!")),
      await authorization(0, AUTH_BODY.replace("AN0001\r\n", "")),
      await authorization(0, AUTH_BODY.replace("IT              ", "ITbad")),
      await authorization(0, AUTH_BODY.replace("IT              \r\n", "")),
    ]) {
      const malformed = sequenceFetcher([response(body)]);
      await expect(acquireRealtimeData(SOURCES, QUERY, malformed.fetcher)).rejects.toThrow(
        /authorization/,
      );
    }

    const rejected = sequenceFetcher([response(await authorization(1))]);
    await expect(acquireRealtimeData(SOURCES, QUERY, rejected.fetcher)).rejects.toThrow("rejected");
  });

  it("rejects malformed and nonzero GateServlet responses", async () => {
    for (const body of [
      encoder.encode("short"),
      encoder.encode("02001B1400000620xx"),
      await envelope("02001B1500000621", "WE-one\r\n"),
    ]) {
      const malformed = sequenceFetcher([response(await authorization()), response(body)]);
      await expect(acquireRealtimeData(SOURCES, QUERY, malformed.fetcher)).rejects.toThrow("gate");
    }

    const rejected = sequenceFetcher([response(await authorization()), response(await gate(1))]);
    await expect(acquireRealtimeData(SOURCES, QUERY, rejected.fetcher)).rejects.toThrow("status 1");
  });
});
