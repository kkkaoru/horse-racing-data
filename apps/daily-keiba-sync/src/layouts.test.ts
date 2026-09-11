import { encode } from "iconv-lite";
import { describe, expect, test } from "vitest";
import {
  catalogDeleteRowKeys,
  isProvisionalJraRunner,
  layoutByTable,
  layoutForRecord,
  optionalLayoutForRecord,
  parseFixedRecord,
  rowKey,
  validateRecordLayouts,
} from "./layouts";

const makeRecord = (
  tableName: string,
  values: Readonly<Record<string, string>> = {},
): Uint8Array => {
  const layout = layoutByTable(tableName);
  const bytes = new Uint8Array(layout.recordBytes);
  bytes.fill(32);
  let offset = 0;
  for (const column of layout.columns) {
    const value = values[column.name];
    if (value !== undefined) {
      const encoded = encode(value, "cp932");
      if (encoded.length > column.width) throw new Error("Test field is too long");
      bytes.set(encoded, offset);
    }
    offset += column.width;
  }
  bytes[0] = layout.recordType.charCodeAt(0);
  bytes[1] = layout.recordType.charCodeAt(1);
  bytes[bytes.length - 2] = 13;
  bytes[bytes.length - 1] = 10;
  return bytes;
};

describe("fixed-record layouts", () => {
  test("loads the generated NV RA schema and decodes CP932 without trimming", () => {
    const bytes = makeRecord("nvd_ra", { data_kubun: "7", kaisai_nen: "2026" });
    const layout = layoutForRecord("nv", bytes);
    const row = parseFixedRecord(layout, bytes);

    expect(layout.tableName).toBe("nvd_ra");
    expect(layout.recordBytes).toBe(1272);
    expect(row.record_id).toBe("RA");
    expect(row.data_kubun).toBe("7");
    expect(row.kaisai_nen).toBe("2026");
    expect(rowKey(layout, row)).toContain("2026");
  });

  test("loads different provider layouts for the same record type", () => {
    expect(layoutForRecord("jv", makeRecord("jvd_wf")).tableName).toBe("jvd_wf");
    expect(layoutForRecord("nv", makeRecord("nvd_wf")).tableName).toBe("nvd_wf");
  });

  test("rejects unknown table and record types", () => {
    expect(() => layoutByTable("evil_table")).toThrow("Unsupported table");
    expect(() => layoutForRecord("nv", new Uint8Array())).toThrow("shorter");
    expect(() => layoutForRecord("nv", new Uint8Array([90, 90]))).toThrow("Unsupported NV");
    expect(optionalLayoutForRecord("jv", new Uint8Array([74, 71]))).toBeUndefined();
  });

  test("rejects invalid record length and terminator", () => {
    const layout = layoutByTable("nvd_ra");
    expect(() => parseFixedRecord(layout, new Uint8Array(10))).toThrow("record length");
    const bytes = makeRecord("nvd_ra");
    bytes[bytes.length - 1] = 0;
    expect(() => parseFixedRecord(layout, bytes)).toThrow("record terminator");
  });

  test("defines the typed netkeiba training Catalog layout", () => {
    const layout = layoutByTable("netkeiba_training_workouts");
    expect(layout.columns.length).toBe(46);
    expect(layout.columns.find((column) => column.name === "workout_index")?.catalogType).toBe(
      "int",
    );
    expect(layout.columns.find((column) => column.name === "fetched_at")?.catalogType).toBe(
      "timestamptz",
    );
    expect(
      rowKey(layout, {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0905",
        keibajo_code: "01",
        ketto_toroku_bango: "2023100001",
        race_bango: "01",
        workout_key: "abc",
      }),
    ).toBe("2026\u001f0905\u001f01\u001f01\u001f2023100001\u001fabc");
  });

  test("maps a confirmed domestic JRA runner to its stale provisional Catalog key", () => {
    const layout = layoutByTable("jvd_se");
    const confirmed = {
      kaisai_nen: "2026",
      kaisai_tsukihi: "0912",
      keibajo_code: "06",
      race_bango: "01",
      umaban: "03",
      ketto_toroku_bango: "2023100001",
    };
    const provisional = { ...confirmed, umaban: "00" };

    expect(isProvisionalJraRunner(layout, provisional)).toBe(true);
    expect(isProvisionalJraRunner(layout, confirmed)).toBe(false);
    expect(catalogDeleteRowKeys(layout, confirmed)).toStrictEqual([
      rowKey(layout, confirmed),
      rowKey(layout, provisional),
    ]);
    expect(
      catalogDeleteRowKeys(layout, { ...confirmed, keibajo_code: "A8", umaban: "19" }),
    ).toHaveLength(1);
  });

  test("rejects a row without a primary-key field", () => {
    const layout = layoutByTable("nvd_ra");
    expect(() => rowKey(layout, {})).toThrow("Missing primary key");
  });

  test.each([
    [null, "record layouts"],
    [{ formatVersion: 2, tables: {} }, "record layouts"],
    [{ formatVersion: 1, tables: [] }, "record layouts"],
    [{ formatVersion: 1, tables: { "Bad-Name": {} } }, "record layout"],
    [
      {
        formatVersion: 1,
        tables: { nvd_xx: { columns: {}, recordBytes: 4, recordType: "XX", primaryKey: ["id"] } },
      },
      "record layout",
    ],
    [
      {
        formatVersion: 1,
        tables: { nvd_xx: { columns: [], recordBytes: "4", recordType: "XX", primaryKey: ["id"] } },
      },
      "metadata",
    ],
    [
      {
        formatVersion: 1,
        tables: {
          nvd_xx: { columns: [null], recordBytes: 4, recordType: "XX", primaryKey: ["id"] },
        },
      },
      "record column",
    ],
    [
      {
        formatVersion: 1,
        tables: {
          nvd_xx: {
            columns: [{ name: "Bad", width: 0 }],
            recordBytes: 2,
            recordType: "XX",
            primaryKey: ["id"],
          },
        },
      },
      "column value",
    ],
    [
      {
        formatVersion: 1,
        tables: { nvd_xx: { columns: [], recordBytes: 2, recordType: "XX", primaryKey: "id" } },
      },
      "primary key",
    ],
    [
      {
        formatVersion: 1,
        tables: { nvd_xx: { columns: [], recordBytes: 2, recordType: "XX", primaryKey: [1] } },
      },
      "primary key",
    ],
    [
      {
        formatVersion: 1,
        tables: { nvd_xx: { columns: [], recordBytes: 2, recordType: "XX", primaryKey: ["Bad"] } },
      },
      "primary key column",
    ],
    [
      {
        formatVersion: 1,
        tables: { nvd_xx: { columns: [], recordBytes: 2, recordType: "XX", primaryKey: [] } },
      },
      "primary key is empty",
    ],
    [
      {
        formatVersion: 1,
        tables: { nvd_xx: { columns: [], recordBytes: 3, recordType: "X", primaryKey: ["id"] } },
      },
      "byte layout",
    ],
  ])("rejects malformed generated metadata %#", (value, message) => {
    expect(() => validateRecordLayouts(value)).toThrow(message);
  });
});

export { makeRecord };
