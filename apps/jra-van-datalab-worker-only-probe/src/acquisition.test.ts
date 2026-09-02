// Run with bun.
import { describe, expect, it } from "vitest";
import {
  acquireJvData,
  splitJvRecords,
  validateAcquisitionQuery,
  type JvFetch,
} from "./acquisition";
import { TEST_CORE_CONFIG } from "./test-fixtures";

const encoder = new TextEncoder();
const BOOTSTRAP_OK = encoder.encode("0200100700005000\r\n0\r\n\r\n");
const SHORT_JV_FILE: Uint8Array = new Uint8Array([
  32, 32, 32, 32, 32, 32, 32, 32, 32, 53, 120, 156, 155, 228, 170, 250, 112, 26, 59, 0, 9, 53, 2,
  123,
]);
const SOURCES = TEST_CORE_CONFIG;
const QUERY = { dataSpec: "RACE", from: "20260829000000", to: "20260830235959" };
const FILENAME = "JGAA00000000000000000000.jvd";

interface RecordedCall {
  init: RequestInit | undefined;
  input: RequestInfo | URL;
}

const response = (body: Uint8Array, status: number = 200): Response =>
  new Response(body, { status });

const fileListEntries = (
  entries: readonly { filename: string; size: number }[],
  status: number = 0,
): Uint8Array =>
  encoder.encode(
    `02001002${String(status).padStart(3, "0")}05000\r\n` +
      `CD20260829\r\nIT20260829112816\r\nRM1\r\nRT2\r\nTO12345\r\n` +
      entries.map(({ filename, size }) => `FN${filename}\r\nFS${size}\r\n`).join(""),
  );

const fileList = (filename: string, size: number, status: number = 0): Uint8Array =>
  fileListEntries([{ filename, size }], status);

const sequenceFetcher = (responses: Response[]): { calls: RecordedCall[]; fetcher: JvFetch } => {
  const calls: RecordedCall[] = [];
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

const makeJvFile = async (
  decodedLength: number,
  terminated: boolean = true,
): Promise<Uint8Array> => {
  const decoded = new Uint8Array(decodedLength + 1).fill(0x20);
  decoded[0] = 0x4a;
  decoded[1] = 0x47;
  if (terminated) {
    decoded[decodedLength - 2] = 0x0d;
    decoded[decodedLength - 1] = 0x0a;
  }
  decoded[decodedLength] = 0x63;
  const transformed = decoded.map(
    (value, index) => swapNibbles(value) ^ (index % 2 === 0 ? 0x36 : 0x31),
  );
  const compressed = await compress(transformed);
  const header = encoder.encode(String(decodedLength).padStart(10, " "));
  const file = new Uint8Array(header.length + compressed.length);
  file.set(header);
  file.set(compressed, header.length);
  return file;
};

describe("query validation", () => {
  it("accepts the bounded official query shape", () => {
    expect(validateAcquisitionQuery(QUERY)).toBe(QUERY);
    expect(validateAcquisitionQuery({ ...QUERY, dataSpec: "RACEDIFF", option: 4 })).toMatchObject({
      dataSpec: "RACEDIFF",
      option: 4,
    });
  });

  it("rejects invalid data specs, timestamps, and reversed ranges", () => {
    expect(() => validateAcquisitionQuery({ ...QUERY, dataSpec: "bad" })).toThrow("dataSpec");
    expect(() => validateAcquisitionQuery({ ...QUERY, dataSpec: "RACE".repeat(17) })).toThrow(
      "dataSpec",
    );
    expect(() => validateAcquisitionQuery({ ...QUERY, from: "today" })).toThrow("fourteen");
    expect(() => validateAcquisitionQuery({ ...QUERY, option: 5 as 1 })).toThrow("option");
    expect(() =>
      validateAcquisitionQuery({ ...QUERY, from: "20260831000000", to: "20260830000000" }),
    ).toThrow("later");
  });
});

describe("Worker-native acquisition", () => {
  it("splits every CRLF record without assuming a record prefix or fixed length", () => {
    const decoded = encoder.encode("JG-one\r\nRA-two-longer\r\nSE-three\r\n");
    expect(splitJvRecords(decoded).map((record) => new TextDecoder().decode(record))).toEqual([
      "JG-one\r\n",
      "RA-two-longer\r\n",
      "SE-three\r\n",
    ]);
  });

  it("performs the oracle bootstrap, file-list, CDN, and payload transition sequence", async () => {
    const file = await makeJvFile(80);
    const mock = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(fileList(FILENAME, file.length)),
      response(file),
    ]);

    const result = await acquireJvData(SOURCES, QUERY, mock.fetcher);

    expect(result.record).toHaveLength(80);
    expect(result.record.slice(0, 2)).toStrictEqual(encoder.encode("JG"));
    expect(result.files).toHaveLength(1);
    expect(result.recordCount).toBe(1);
    expect(result.transitions).toEqual([
      "configured",
      "session-encoded",
      "bootstrapped",
      "file-listed",
      "payload-decoded",
    ]);
    expect(mock.calls).toHaveLength(3);
    const authenticationInput = mock.calls[0]!.input;
    const dataInput = mock.calls[2]!.input;
    expect(authenticationInput).toBeTypeOf("string");
    expect(dataInput).toBeTypeOf("string");
    if (typeof authenticationInput !== "string" || typeof dataInput !== "string")
      throw new Error("Expected string URLs");
    expect(authenticationInput).toContain("JVServlet");
    expect(mock.calls[0]!.init?.method).toBe("POST");
    expect(dataInput).toContain(`/JGAA/${FILENAME}`);
    expect(mock.calls[2]!.init?.method).toBe("GET");
  });

  it("downloads every listed file and preserves SDK file and record ordering", async () => {
    const first = await makeJvFile(80);
    const second = await makeJvFile(160);
    const secondName = "JGAB00000000000000000000.jvd";
    const mock = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(
        fileListEntries([
          { filename: FILENAME, size: first.length },
          { filename: secondName, size: second.length },
        ]),
      ),
      response(first),
      response(second),
    ]);

    const result = await acquireJvData(SOURCES, QUERY, mock.fetcher);

    expect(result.files.map(({ filename }) => filename)).toEqual([FILENAME, secondName]);
    expect(result.fileBytes).toBe(first.length + second.length);
    expect(result.decodedBytes).toBe(240);
    expect(result.recordCount).toBe(2);
    expect(mock.calls).toHaveLength(4);
  });

  it("rejects HTTP and protocol status failures", async () => {
    const http = sequenceFetcher([response(new Uint8Array(), 500)]);
    await expect(acquireJvData(SOURCES, QUERY, http.fetcher)).rejects.toThrow("HTTP error");

    const malformedBootstrap = sequenceFetcher([response(encoder.encode("short"))]);
    await expect(acquireJvData(SOURCES, QUERY, malformedBootstrap.fetcher)).rejects.toThrow(
      "bootstrap framing",
    );

    const bootstrap = sequenceFetcher([response(encoder.encode("0200100700105000\r\n0\r\n\r\n"))]);
    await expect(acquireJvData(SOURCES, QUERY, bootstrap.fetcher)).rejects.toThrow("bootstrap");

    const malformedList = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(encoder.encode("short")),
    ]);
    await expect(acquireJvData(SOURCES, QUERY, malformedList.fetcher)).rejects.toThrow(
      "file-list framing",
    );

    const list = sequenceFetcher([response(BOOTSTRAP_OK), response(fileList(FILENAME, 10, 1))]);
    await expect(acquireJvData(SOURCES, QUERY, list.fetcher)).rejects.toThrow("file-list request");
  });

  it("rejects missing and unbounded file entries", async () => {
    const missing = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(encoder.encode("0200100200005000\r\nRM1\r\nRT2\r\n")),
    ]);
    await expect(acquireJvData(SOURCES, QUERY, missing.fetcher)).rejects.toThrow("no files");

    for (const size of [0, 20 * 1024 * 1024 + 1]) {
      const mock = sequenceFetcher([response(BOOTSTRAP_OK), response(fileList(FILENAME, size))]);
      await expect(acquireJvData(SOURCES, QUERY, mock.fetcher)).rejects.toThrow("bounded");
    }
  });

  it("rejects CDN HTTP and size mismatches", async () => {
    const failed = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(fileList(FILENAME, 1)),
      response(new Uint8Array(), 500),
    ]);
    await expect(acquireJvData(SOURCES, QUERY, failed.fetcher)).rejects.toThrow("data endpoint");

    const mismatch = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(fileList(FILENAME, 2)),
      response(new Uint8Array([1])),
    ]);
    await expect(acquireJvData(SOURCES, QUERY, mismatch.fetcher)).rejects.toThrow("size does not");
  });

  it("supports every CRLF-framed record prefix and rejects incomplete framing", async () => {
    const generic = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(fileList("ZZAA00000000000000000000.jvd", SHORT_JV_FILE.length)),
      response(SHORT_JV_FILE),
    ]);
    await expect(acquireJvData(SOURCES, QUERY, generic.fetcher)).resolves.toMatchObject({
      recordCount: 1,
    });

    const incompleteFile = await makeJvFile(80, false);
    const incomplete = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(fileList(FILENAME, incompleteFile.length)),
      response(incompleteFile),
    ]);
    await expect(acquireJvData(SOURCES, QUERY, incomplete.fetcher)).rejects.toThrow("CRLF");

    const emptyFile = await makeJvFile(0);
    const empty = sequenceFetcher([
      response(BOOTSTRAP_OK),
      response(fileList(FILENAME, emptyFile.length)),
      response(emptyFile),
    ]);
    await expect(acquireJvData(SOURCES, QUERY, empty.fetcher)).rejects.toThrow("no records");
  });
});
