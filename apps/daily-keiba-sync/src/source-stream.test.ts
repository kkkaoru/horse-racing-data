import { describe, expect, test } from "vitest";
import { makeRecord } from "./layouts.test";
import {
  createTableStage,
  parseSourceReadable,
  parseSourceStream,
  parseTableStage,
  tableStagingKey,
} from "./source-stream";

const base64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

const stream = (record: Uint8Array, declaredRecords = 1): string =>
  [
    JSON.stringify({ event: "open" }),
    JSON.stringify({ event: "file" }),
    JSON.stringify({
      bytes: record.length,
      data: base64(record),
      encoding: "base64",
      event: "record",
    }),
    JSON.stringify({ event: "close", files: 1, records: declaredRecords }),
    "",
  ].join("\n");

describe("source stream parser", () => {
  test("validates a complete stream and deduplicates records by primary key", () => {
    const record = makeRecord("nvd_ra", { data_kubun: "7", kaisai_nen: "2026" });
    const body = stream(record, 2).replace(
      JSON.stringify({ event: "close", files: 1, records: 2 }),
      `${JSON.stringify({ bytes: record.length, data: base64(record), encoding: "base64", event: "record" })}\n${JSON.stringify({ event: "close", files: 1, records: 2 })}`,
    );
    const parsed = parseSourceStream(body, "nv");

    expect(parsed.files).toBe(1);
    expect(parsed.records).toBe(2);
    expect(parsed.tables).toHaveLength(1);
    expect(parsed.tables[0]?.layout.tableName).toBe("nvd_ra");
    expect(parsed.tables[0]?.records).toHaveLength(1);
  });

  test("counts unsupported records while retaining them only in raw staging", () => {
    const parsed = parseSourceStream(stream(new Uint8Array([74, 71, 13, 10])), "jv");

    expect(parsed).toMatchObject({ files: 1, records: 1, tables: [] });
  });

  test("parses an incrementally chunked R2 body", async () => {
    const record = makeRecord("nvd_ra", { kaisai_nen: "2026" });
    const body = stream(record);
    const encoded = new TextEncoder().encode(body);
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoded.slice(0, 7));
        controller.enqueue(encoded.slice(7, 111));
        controller.enqueue(encoded.slice(111));
        controller.close();
      },
    });
    await expect(parseSourceReadable(readable, "nv")).resolves.toMatchObject({
      files: 1,
      records: 1,
    });
  });

  test("bounds streaming lines and validates its configured limit", async () => {
    const oversized = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('{"event":"open"}\n'));
        controller.close();
      },
    });
    await expect(parseSourceReadable(oversized, "nv", 2)).rejects.toThrow("exceeds limit");
    await expect(
      parseSourceReadable(
        new ReadableStream({ start: (controller) => controller.close() }),
        "nv",
        0,
      ),
    ).rejects.toThrow("line limit");
  });

  test("creates and parses table staging objects", () => {
    const stage = createTableStage("jv", "run-1", "jvd_ra", [{ record_shubetsu_id: "RA" }]);
    expect(parseTableStage(stage)).toEqual(stage);
    expect(tableStagingKey("jv", "20260903", "run-1", "jvd_ra")).toBe(
      "source-staging/v1/jv/20260903/run-1/tables/jvd_ra.json",
    );
  });

  test("rejects incomplete, unsupported, and malformed events", () => {
    const record = makeRecord("nvd_ra");
    expect(() => parseSourceStream(stream(record, 2), "nv")).toThrow("Incomplete");
    expect(() => parseSourceStream('{"event":"mystery"}\n', "nv")).toThrow("Unsupported");
    expect(() => parseSourceStream("{}\n", "nv")).toThrow("Invalid source event");
    expect(() =>
      parseSourceStream(
        `${JSON.stringify({ event: "open" })}\n${JSON.stringify({ event: "close", files: "1", records: 0 })}\n`,
        "nv",
      ),
    ).toThrow("Invalid source close");
  });

  test("rejects malformed record payloads", () => {
    expect(() =>
      parseSourceStream(
        [
          JSON.stringify({ event: "open" }),
          JSON.stringify({ bytes: 2, data: "QQ==", encoding: "base64", event: "record" }),
          JSON.stringify({ event: "close", files: 0, records: 1 }),
        ].join("\n"),
        "nv",
      ),
    ).toThrow("Invalid source record payload");
    expect(() =>
      parseSourceStream(
        [
          JSON.stringify({ event: "open" }),
          JSON.stringify({ bytes: 1, data: "QQ==", encoding: "plain", event: "record" }),
        ].join("\n"),
        "nv",
      ),
    ).toThrow("Invalid source record event");
  });

  test("rejects malformed table staging objects", () => {
    expect(() => parseTableStage(null)).toThrow("Invalid table staging");
    expect(() =>
      parseTableStage({ formatVersion: 1, provider: "x", records: [], runId: "r", tableName: "t" }),
    ).toThrow("Invalid table staging");
    expect(() =>
      parseTableStage({
        formatVersion: 1,
        provider: "nv",
        records: ["bad"],
        runId: "r",
        tableName: "t",
      }),
    ).toThrow("Invalid staged record");
    expect(() =>
      parseTableStage({
        formatVersion: 1,
        provider: "nv",
        records: [{ a: true }],
        runId: "r",
        tableName: "t",
      }),
    ).toThrow("Invalid staged record");
  });
});
