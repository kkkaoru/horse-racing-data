// Run with bun.
import { describe, expect, it } from "vitest";
import type { JvFetch } from "./acquisition";
import { acquireCourse, validateCourseKey } from "./course";
import { TEST_CORE_CONFIG } from "./test-fixtures";

const encoder = new TextEncoder();
const SOURCES = TEST_CORE_CONFIG;
const KEY = "9999999905240011";
const PATH = "/datalab/crsimg/course.gif";
const AUTH_BODY =
  "AN0001\r\nDB0002\r\nDL0003\r\nUB0004\r\nUL0005\r\nOB0006\r\nOL0007\r\nCB0008\r\n" +
  "IT              \r\n";

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

const envelope = async (header: string, body: Uint8Array): Promise<Uint8Array> => {
  const prefix = encoder.encode(`${header}\r\n`);
  const compressed = await compress(body);
  const result = new Uint8Array(prefix.length + compressed.length);
  result.set(prefix);
  result.set(compressed, prefix.length);
  return result;
};

const authorization = (): Promise<Uint8Array> =>
  envelope("0200100300005001", encoder.encode(AUTH_BODY));

const courseRecord = (path: string = PATH, length: number = 6994): Uint8Array => {
  const record = new Uint8Array(length).fill(0x20);
  record.set(encoder.encode(path));
  if (length >= 2) {
    record[length - 2] = 0x0d;
    record[length - 1] = 0x0a;
  }
  return record;
};

const gate = (record: Uint8Array): Promise<Uint8Array> => envelope("02001B7200000621", record);

const sequenceFetcher = (
  responses: Response[],
): { calls: (RequestInfo | URL)[]; fetcher: JvFetch } => {
  const calls: (RequestInfo | URL)[] = [];
  return {
    calls,
    fetcher: async (input) => {
      calls.push(input);
      const next = responses.shift();
      if (next === undefined) throw new Error("Unexpected fetch");
      return next;
    },
  };
};

const gif = (version: "87a" | "89a" = "89a"): Uint8Array => encoder.encode(`GIF${version}payload`);

describe("JVCourseFile Worker equivalent", () => {
  it("validates the official sixteen-digit course key", () => {
    expect(validateCourseKey(KEY)).toBe(KEY);
    expect(() => validateCourseKey("course")).toThrow("sixteen");
  });

  it.each(["87a", "89a"] as const)(
    "returns a GIF%s image and fixed-width explanation",
    async (v) => {
      const mock = sequenceFetcher([
        response(await authorization()),
        response(await gate(courseRecord())),
        response(gif(v)),
      ]);

      const result = await acquireCourse(SOURCES, KEY, mock.fetcher);

      expect(result.path).toBe(PATH);
      expect(result.explanation).toHaveLength(6800);
      expect(result.image).toStrictEqual(gif(v));
      expect(mock.calls).toHaveLength(3);
      expect(mock.calls[2]).toBe("http://datalab.cdn.jra-van.ne.jp/datalab/crsimg/course.gif");
    },
  );

  it("rejects missing, duplicate, short, and unsafe course records", async () => {
    const malformedGateBodies = [
      await envelope("02001B7200000621", new Uint8Array()),
      await envelope("02001B7200000621", encoder.encode("A\r\nB\r\n")),
      await gate(courseRecord(PATH, 100)),
      await gate(courseRecord("/datalab/../secret.gif")),
    ];
    for (const body of malformedGateBodies) {
      const mock = sequenceFetcher([response(await authorization()), response(body)]);
      await expect(acquireCourse(SOURCES, KEY, mock.fetcher)).rejects.toThrow(/record|unsafe/);
    }
  });

  it("rejects HTTP, bounded-size, and signature image failures", async () => {
    for (const imageResponse of [
      response(new Uint8Array(), 500),
      response(encoder.encode("tiny")),
      response(new Uint8Array(2 * 1024 * 1024 + 1)),
      response(encoder.encode("NOTGIFpayload")),
    ]) {
      const mock = sequenceFetcher([
        response(await authorization()),
        response(await gate(courseRecord())),
        imageResponse,
      ]);
      await expect(acquireCourse(SOURCES, KEY, mock.fetcher)).rejects.toThrow(/image|GIF/);
    }
  });
});
