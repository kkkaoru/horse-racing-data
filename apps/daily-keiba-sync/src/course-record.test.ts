// Runs with bun via the package Vitest scripts.
import { encode } from "iconv-lite";
import { expect, test } from "vitest";
import { layoutForRecord, optionalLayoutForRecord, parseFixedRecord, rowKey } from "./layouts";
import { parseSourceStream } from "./source-stream";

// Independent fixture offsets from JV-Data 4.9.0.1, page 22, not generated layout data.
const courseRecord = (header: string): Uint8Array => {
  const bytes = new Uint8Array(6829).fill(32);
  bytes.set(encode(header, "cp932"));
  bytes.set(encode("コース説明", "cp932"), 27);
  bytes.set([13, 10], 6827);
  return bytes;
};

const sourceStream = (records: readonly Uint8Array[]): string =>
  [
    JSON.stringify({ event: "open" }),
    JSON.stringify({ event: "file" }),
    ...records.map((record) =>
      JSON.stringify({
        bytes: record.length,
        data: Buffer.from(record).toString("base64"),
        encoding: "base64",
        event: "record",
      }),
    ),
    JSON.stringify({ event: "close", files: 1, records: records.length }),
    "",
  ].join("\n");

test("decodes official CS offsets, multibyte explanation and four-column key", () => {
  const bytes = courseRecord("CS2202609180516001120240106");
  const layout = layoutForRecord("jv", bytes);
  const row = parseFixedRecord(layout, bytes);
  expect(layout.tableName).toBe("jvd_cs");
  expect(layout.recordBytes).toBe(6829);
  expect(layout.columns).toHaveLength(8);
  expect(layout.primaryKey).toStrictEqual([
    "keibajo_code",
    "kyori",
    "track_code",
    "course_kaishu_nengappi",
  ]);
  expect(row).toMatchObject({
    record_id: "CS",
    data_kubun: "2",
    data_sakusei_nengappi: "20260918",
    keibajo_code: "05",
    kyori: "1600",
    track_code: "11",
    course_kaishu_nengappi: "20240106",
  });
  expect(String(row.course_setsumei).trimEnd()).toBe("コース説明");
  expect(String(row.course_setsumei).length).toBe(6795);
  expect(row.kaisai_nen).toBeUndefined();
  expect(rowKey(layout, row)).toBe("05\u001f1600\u001f11\u001f20240106");
});

test("does not assign the JV course layout to NV input", () => {
  expect(
    optionalLayoutForRecord("nv", courseRecord("CS2202609180516001120240106")),
  ).toBeUndefined();
});

test("rejects truncated recognized CS instead of silently skipping it", () => {
  const bytes = courseRecord("CS2202609180516001120240106").slice(0, 6828);
  expect(() => parseSourceStream(sourceStream([bytes]), "jv")).toThrow(
    "Invalid jvd_cs record length",
  );
});

test("rejects an invalid CS terminator", () => {
  const bytes = courseRecord("CS2202609180516001120240106");
  bytes[6828] = 32;
  expect(() => parseSourceStream(sourceStream([bytes]), "jv")).toThrow(
    "Invalid jvd_cs record terminator",
  );
});

test("stages the latest deletion instruction without losing its identity", () => {
  const parsed = parseSourceStream(
    sourceStream([
      courseRecord("CS1202609170516001120240106"),
      courseRecord("CS2202609180516001120240106"),
      courseRecord("CS0202609190516001120240106"),
    ]),
    "jv",
  );
  expect(parsed.records).toBe(3);
  expect(parsed.tables).toHaveLength(1);
  expect(parsed.tables[0]?.records).toHaveLength(1);
  expect(parsed.tables[0]?.records[0]).toMatchObject({
    data_kubun: "0",
    data_sakusei_nengappi: "20260919",
    course_kaishu_nengappi: "20240106",
  });
});

test("keeps different renovation dates as separate course versions", () => {
  const parsed = parseSourceStream(
    sourceStream([
      courseRecord("CS1202609170516001120240106"),
      courseRecord("CS1202609180516001120260106"),
    ]),
    "jv",
  );
  expect(parsed.tables).toHaveLength(1);
  expect(parsed.tables[0]?.records).toHaveLength(2);
});
