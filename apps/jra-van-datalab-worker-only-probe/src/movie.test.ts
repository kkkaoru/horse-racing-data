// Run with bun.
import { describe, expect, it } from "vitest";
import type { JvFetch } from "./acquisition";
import {
  acquireMovieList,
  buildMovieAuthorizationBody,
  normalizeMovieSearch,
  type MovieListQuery,
} from "./movie";
import { TEST_CORE_CONFIG } from "./test-fixtures";

const encoder = new TextEncoder();
const SOURCES = TEST_CORE_CONFIG;
const QUERY: MovieListQuery = { movieType: "11", searchKey: "20260830" };

const response = (body: Uint8Array, status: number = 200): Response =>
  new Response(body, { status });

const compress = async (body: Uint8Array): Promise<Uint8Array> => {
  const source = new ReadableStream<Uint8Array>({
    start(controller): void {
      controller.enqueue(body);
      controller.close();
    },
  });
  return new Uint8Array(
    await new Response(source.pipeThrough(new CompressionStream("deflate"))).arrayBuffer(),
  );
};

const movieEnvelope = async (body: string, status: number = 0): Promise<Uint8Array> => {
  const prefix = encoder.encode(`02001B8C${String(status).padStart(3, "0")}00621\r\n`);
  const compressed = await compress(encoder.encode(body));
  const result = new Uint8Array(prefix.length + compressed.length);
  result.set(prefix);
  result.set(compressed, prefix.length);
  return result;
};

const authorization = (status: number = 0, permitted: boolean = true): Uint8Array =>
  encoder.encode(`02001006${String(status).padStart(3, "0")}05000\r\n${permitted ? "1" : "0"}`);

const realtimeAuthorization = async (): Promise<Uint8Array> => {
  const body = encoder.encode(
    "AN0001\r\nDB0002\r\nDL0003\r\nUB0004\r\nUL0005\r\nOB0006\r\nOL0007\r\nCB0008\r\n" +
      "IT              \r\n",
  );
  const prefix = encoder.encode("0200100300005001\r\n");
  const compressed = await compress(body);
  const result = new Uint8Array(prefix.length + compressed.length);
  result.set(prefix);
  result.set(compressed, prefix.length);
  return result;
};

const sequenceFetcher = (
  responses: Response[],
): { calls: { body?: BodyInit | null }[]; fetcher: JvFetch } => {
  const calls: { body?: BodyInit | null }[] = [];
  return {
    calls,
    fetcher: async (_input, init) => {
      calls.push({ body: init?.body });
      const next = responses.shift();
      if (next === undefined) throw new Error("Unexpected fetch");
      return next;
    },
  };
};

describe("JVMVOpen/JVMVRead Worker equivalent", () => {
  it("normalizes all three official movie search forms", () => {
    expect(normalizeMovieSearch(QUERY)).toBe("202608300000000000");
    expect(normalizeMovieSearch({ movieType: "12", searchKey: "202608301234567890" })).toBe(
      "202608301234567890",
    );
    expect(normalizeMovieSearch({ movieType: "13", searchKey: "1234567890" })).toBe(
      "000000001234567890",
    );
    expect(() => normalizeMovieSearch({ ...QUERY, softwareId: "bad:" })).toThrow("softwareId");
    expect(() => normalizeMovieSearch({ movieType: "11", searchKey: "bad" })).toThrow(
      "combination",
    );
    expect(() => normalizeMovieSearch({ movieType: "12", searchKey: "20260830" })).toThrow(
      "combination",
    );
    expect(() => normalizeMovieSearch({ movieType: "13", searchKey: "20260830" })).toThrow(
      "combination",
    );
  });

  it("builds APPL=0006 and returns every eighteen-byte movie key", async () => {
    expect(buildMovieAuthorizationBody()).toBe(
      "VER=0200&APPL=0006&RKEY=&UKEY=&JVER=0500&OS=000200100000&JVBIT=1",
    );
    const mock = sequenceFetcher([
      response(await realtimeAuthorization()),
      response(authorization()),
      response(
        await movieEnvelope(
          "\r\n" + "0".repeat(46) + "\r\n202608301234567890\r\n202608309876543210\r\n",
        ),
      ),
    ]);

    const result = await acquireMovieList(SOURCES, QUERY, mock.fetcher);

    expect(result.status).toBe(0);
    expect(result.keys.map((key) => new TextDecoder().decode(key))).toEqual([
      "202608301234567890",
      "202608309876543210",
    ]);
    const gateBody = mock.calls[2]!.body;
    expect(gateBody).toBeTypeOf("string");
    if (typeof gateBody !== "string") throw new Error("Expected a string form body");
    expect(gateBody).toContain(
      "KEY=ABCDE12345678901212345678901234&FLG=2&DATA=02000B8C202608300000000000",
    );
  });

  it("rejects authorization HTTP, framing, body, and status failures", async () => {
    const http = sequenceFetcher([response(new Uint8Array(), 500)]);
    await expect(acquireMovieList(SOURCES, QUERY, http.fetcher)).rejects.toThrow("HTTP error");

    for (const body of [
      encoder.encode("short"),
      encoder.encode("0200100600005000xx0"),
      encoder.encode("0200100600005000\r\n2"),
    ]) {
      const mock = sequenceFetcher([response(await realtimeAuthorization()), response(body)]);
      await expect(acquireMovieList(SOURCES, QUERY, mock.fetcher)).rejects.toThrow("authorization");
    }

    const rejected = sequenceFetcher([
      response(await realtimeAuthorization()),
      response(authorization(1)),
    ]);
    await expect(acquireMovieList(SOURCES, QUERY, rejected.fetcher)).rejects.toThrow("status 1");

    const unauthorized = sequenceFetcher([
      response(await realtimeAuthorization()),
      response(authorization(0, false)),
    ]);
    await expect(acquireMovieList(SOURCES, QUERY, unauthorized.fetcher)).rejects.toThrow(
      "not authorized",
    );
  });

  it("rejects Gate HTTP, envelope, and status failures", async () => {
    const http = sequenceFetcher([
      response(await realtimeAuthorization()),
      response(authorization()),
      response(new Uint8Array(), 500),
    ]);
    await expect(acquireMovieList(SOURCES, QUERY, http.fetcher)).rejects.toThrow("HTTP error");

    for (const body of [encoder.encode("short"), encoder.encode("02001B8C00000620xx")]) {
      const mock = sequenceFetcher([
        response(await realtimeAuthorization()),
        response(authorization()),
        response(body),
      ]);
      await expect(acquireMovieList(SOURCES, QUERY, mock.fetcher)).rejects.toThrow("movie list");
    }

    const rejected = sequenceFetcher([
      response(await realtimeAuthorization()),
      response(authorization()),
      response(await movieEnvelope("\r\n" + "0".repeat(46) + "\r\n", 1)),
    ]);
    await expect(acquireMovieList(SOURCES, QUERY, rejected.fetcher)).rejects.toThrow("status 1");
  });

  it("rejects incomplete, missing, malformed control, and key records", async () => {
    for (const payload of [
      "not-terminated",
      "",
      "nonempty\r\n" + "0".repeat(46) + "\r\n",
      "\r\nshort\r\n",
      "\r\n" + "0".repeat(46) + "\r\ninvalid-key\r\n",
    ]) {
      const mock = sequenceFetcher([
        response(await realtimeAuthorization()),
        response(authorization()),
        response(await movieEnvelope(payload)),
      ]);
      await expect(acquireMovieList(SOURCES, QUERY, mock.fetcher)).rejects.toThrow(/control|key/);
    }
  });
});
