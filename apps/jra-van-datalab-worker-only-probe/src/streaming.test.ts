// Run with bun.
import { describe, expect, it } from "vitest";
import type { JvFetch } from "./acquisition";
import { createJvDataStream } from "./streaming";
import { TEST_CORE_CONFIG } from "./test-fixtures";

const encoder = new TextEncoder();
const BOOTSTRAP_OK = encoder.encode("0200100700005000\r\n0\r\n\r\n");
const SOURCES = TEST_CORE_CONFIG;
const QUERY = { dataSpec: "RACE", from: "20260829000000", to: "20260830235959" };
const FIRST_NAME = "JGAA00000000000000000000.jvd";
const SECOND_NAME = "JGAB00000000000000000000.jvd";

const response = (body: Uint8Array, status: number = 200): Response =>
  new Response(body, { status });

const compress = async (input: Uint8Array): Promise<Uint8Array> => {
  const source = new ReadableStream<Uint8Array>({
    start(controller): void {
      controller.enqueue(input);
      controller.close();
    },
  });
  return new Uint8Array(
    await new Response(source.pipeThrough(new CompressionStream("deflate"))).arrayBuffer(),
  );
};

const swapNibbles = (value: number): number => ((value >>> 4) | (value << 4)) & 0xff;

const makeJvFile = async (length: number): Promise<Uint8Array> => {
  const decoded = new Uint8Array(length + 1).fill(0x20);
  decoded[0] = 0x4a;
  decoded[1] = 0x47;
  decoded[length - 2] = 0x0d;
  decoded[length - 1] = 0x0a;
  decoded[length] = 0x63;
  const transformed = decoded.map(
    (value, index) => swapNibbles(value) ^ (index % 2 === 0 ? 0x36 : 0x31),
  );
  const compressed = await compress(transformed);
  const header = encoder.encode(String(length).padStart(10, " "));
  const result = new Uint8Array(header.length + compressed.length);
  result.set(header);
  result.set(compressed, header.length);
  return result;
};

const fileList = (files: readonly { bytes: number; filename: string }[]): Uint8Array =>
  encoder.encode(
    "0200100200005000\r\nCD20260829\r\nIT20260829112816\r\nRM1\r\nRT2\r\nTO123\r\n" +
      files.map((file) => `FN${file.filename}\r\nFS${file.bytes}\r\n`).join(""),
  );

const sequenceFetcher =
  (responses: Response[]): JvFetch =>
  async () => {
    const next = responses.shift();
    if (next === undefined) throw new Error("Unexpected fetch");
    return next;
  };

const parseLines = async (stream: ReadableStream<Uint8Array>): Promise<Record<string, unknown>[]> =>
  (await new Response(stream).text())
    .trimEnd()
    .split("\n")
    .map((value) => JSON.parse(value) as Record<string, unknown>);

describe("complete JV-Data streaming", () => {
  it("streams every file and lossless base64 record in SDK order", async () => {
    const first = await makeJvFile(80);
    const second = await makeJvFile(9000);
    const fetcher = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(
        fileList([
          { bytes: first.length, filename: FIRST_NAME },
          { bytes: second.length, filename: SECOND_NAME },
        ]),
      ),
      response(first),
      response(second),
    ]);

    const events = await parseLines(await createJvDataStream(SOURCES, QUERY, fetcher));

    expect(events.map(({ event }) => event)).toEqual([
      "open",
      "file",
      "record",
      "file",
      "record",
      "close",
    ]);
    expect(events[0]).toMatchObject({ readCount: 2 });
    expect(events[2]).toMatchObject({ bytes: 80, encoding: "base64", filename: FIRST_NAME });
    expect(Uint8Array.from(atob(String(events[2]!.data)), (value) => value.charCodeAt(0))).toEqual(
      expect.objectContaining({ 0: 0x4a, 1: 0x47, 78: 0x0d, 79: 0x0a }),
    );
    expect(events[5]).toMatchObject({ files: 2, records: 2 });
  });

  it("errors the response stream when a listed file cannot be downloaded", async () => {
    const file = await makeJvFile(80);
    const fetcher = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(fileList([{ bytes: file.length, filename: FIRST_NAME }])),
      response(new Uint8Array(), 500),
    ]);

    const stream = await createJvDataStream(SOURCES, QUERY, fetcher);
    await expect(new Response(stream).text()).rejects.toThrow("data endpoint");
  });
});
