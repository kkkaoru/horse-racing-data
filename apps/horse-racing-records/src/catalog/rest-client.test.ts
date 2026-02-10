// Run with: bun
// Tests for catalog/rest-client.ts

import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  loadTable,
  commitTable,
  fetchCatalogPrefix,
  buildTableUrl,
  buildConfigUrl,
  buildAuthHeaders,
  stringifySnapshotIds,
  numberifySnapshotIds,
  CONFLICT_STATUS,
} from "./rest-client.ts";
import type { CatalogConfig, TableIdentifier } from "./rest-client.ts";

const TEST_CONFIG: CatalogConfig = {
  catalogUri: "https://catalog.example.com",
  warehouse: "test-warehouse",
  apiToken: "test-token",
};

const TEST_TABLE_ID: TableIdentifier = {
  namespace: "horse_racing",
  table: "horse_info",
};

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("buildTableUrl", () => {
  it("should build correct URL", () => {
    const url = buildTableUrl(TEST_CONFIG, TEST_TABLE_ID);
    expect(url).toStrictEqual(
      "https://catalog.example.com/v1/test-warehouse/namespaces/horse_racing/tables/horse_info",
    );
  });
});

describe("buildConfigUrl", () => {
  it("should build correct config URL with warehouse param", () => {
    const url = buildConfigUrl("https://catalog.example.com", "test-warehouse");
    expect(url).toStrictEqual("https://catalog.example.com/v1/config?warehouse=test-warehouse");
  });
});

describe("fetchCatalogPrefix", () => {
  it("should return prefix from config response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            overrides: { prefix: "uuid-prefix-123" },
          }),
      }),
    );

    const prefix = await fetchCatalogPrefix(
      "https://catalog.example.com",
      "test-warehouse",
      "test-token",
    );
    expect(prefix).toStrictEqual("uuid-prefix-123");
  });

  it("should throw on HTTP error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 401,
        text: () => Promise.resolve("Unauthorized"),
      }),
    );

    await expect(
      fetchCatalogPrefix("https://catalog.example.com", "test-warehouse", "test-token"),
    ).rejects.toThrow("Failed to fetch catalog config: 401 Unauthorized");
  });
});

describe("buildAuthHeaders", () => {
  it("should include Bearer token and content type", () => {
    const headers = buildAuthHeaders("my-token");
    expect(headers).toStrictEqual({
      Authorization: "Bearer my-token",
      "Content-Type": "application/json",
    });
  });
});

describe("stringifySnapshotIds", () => {
  it("should convert snapshot-id integers to strings", () => {
    const input = '{"snapshot-id": 2265529103060093806}';
    const result = stringifySnapshotIds(input);
    expect(result).toStrictEqual('{"snapshot-id":"2265529103060093806"}');
  });

  it("should convert current-snapshot-id integers to strings", () => {
    const input = '{"current-snapshot-id": 9999999999999999999}';
    const result = stringifySnapshotIds(input);
    expect(result).toStrictEqual('{"current-snapshot-id":"9999999999999999999"}');
  });

  it("should convert parent-snapshot-id integers to strings", () => {
    const input = '{"parent-snapshot-id": 12345}';
    const result = stringifySnapshotIds(input);
    expect(result).toStrictEqual('{"parent-snapshot-id":"12345"}');
  });

  it("should handle multiple snapshot IDs in one text", () => {
    const input =
      '{"current-snapshot-id": 100, "snapshots": [{"snapshot-id": 100, "parent-snapshot-id": 99}]}';
    const result = stringifySnapshotIds(input);
    expect(result).toStrictEqual(
      '{"current-snapshot-id":"100", "snapshots": [{"snapshot-id":"100", "parent-snapshot-id":"99"}]}',
    );
  });
});

describe("numberifySnapshotIds", () => {
  it("should convert snapshot-id strings to numbers", () => {
    const input = '{"snapshot-id":"2265529103060093806"}';
    const result = numberifySnapshotIds(input);
    expect(result).toStrictEqual('{"snapshot-id":2265529103060093806}');
  });

  it("should convert parent-snapshot-id strings to numbers", () => {
    const input = '{"parent-snapshot-id":"12345"}';
    const result = numberifySnapshotIds(input);
    expect(result).toStrictEqual('{"parent-snapshot-id":12345}');
  });

  it("should handle negative snapshot IDs", () => {
    const input = '{"snapshot-id":"-1"}';
    const result = numberifySnapshotIds(input);
    expect(result).toStrictEqual('{"snapshot-id":-1}');
  });
});

describe("loadTable", () => {
  it("should return table metadata on success", async () => {
    const rawJson =
      '{"metadata-location":"s3://bucket/metadata.json","metadata":{"format-version":2,"table-uuid":"test-uuid","location":"s3://bucket/table","last-sequence-number":1,"last-updated-ms":1000,"last-column-id":10,"current-schema-id":0,"schemas":[],"default-spec-id":0,"partition-specs":[],"last-partition-id":0,"default-sort-order-id":0,"sort-orders":[],"current-snapshot-id":2265529103060093806}}';

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        text: () => Promise.resolve(rawJson),
      }),
    );

    const result = await loadTable(TEST_CONFIG, TEST_TABLE_ID);
    expect(result["metadata-location"]).toStrictEqual("s3://bucket/metadata.json");
    expect(result.metadata["format-version"]).toStrictEqual(2);
    expect(result.metadata["current-snapshot-id"]).toStrictEqual("2265529103060093806");
  });

  it("should throw on HTTP error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
        text: () => Promise.resolve("Not found"),
      }),
    );

    await expect(loadTable(TEST_CONFIG, TEST_TABLE_ID)).rejects.toThrow(
      "Failed to load table horse_racing.horse_info: 404 Not found",
    );
  });
});

describe("commitTable", () => {
  it("should return success on 200", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
      }),
    );

    const result = await commitTable(TEST_CONFIG, TEST_TABLE_ID, {
      requirements: [{ type: "assert-ref-snapshot-id", ref: "main", "snapshot-id": "1" }],
      updates: [
        {
          action: "add-snapshot",
          snapshot: {
            "snapshot-id": "1",
            "sequence-number": 1,
            "timestamp-ms": 1000,
            summary: { operation: "delete" },
            "manifest-list": "s3://bucket/snap-1.avro",
            "schema-id": 0,
          },
        },
      ],
    });
    expect(result).toStrictEqual({ success: true });
  });

  it("should send snapshot IDs as numbers in JSON body", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal("fetch", fetchMock);

    await commitTable(TEST_CONFIG, TEST_TABLE_ID, {
      requirements: [
        { type: "assert-ref-snapshot-id", ref: "main", "snapshot-id": "2265529103060093806" },
      ],
      updates: [
        { action: "set-snapshot-ref", "ref-name": "main", type: "branch", "snapshot-id": "12345" },
      ],
    });

    const callArgs = fetchMock.mock.calls[0] ?? [];
    const sentBody = (callArgs[1] as RequestInit).body as string;
    expect(sentBody.includes('"snapshot-id":2265529103060093806')).toStrictEqual(true);
    expect(sentBody.includes('"snapshot-id":12345')).toStrictEqual(true);
  });

  it("should return conflict error on 409", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: CONFLICT_STATUS,
      }),
    );

    const result = await commitTable(TEST_CONFIG, TEST_TABLE_ID, {
      requirements: [{ type: "assert-ref-snapshot-id", ref: "main", "snapshot-id": "1" }],
      updates: [
        {
          action: "add-snapshot",
          snapshot: {
            "snapshot-id": "1",
            "sequence-number": 1,
            "timestamp-ms": 1000,
            summary: { operation: "delete" },
            "manifest-list": "s3://bucket/snap-1.avro",
            "schema-id": 0,
          },
        },
      ],
    });
    expect(result).toStrictEqual({
      success: false,
      error: "Conflict: table was modified concurrently",
      status: 409,
    });
  });

  it("should return error on other failures", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        text: () => Promise.resolve("Server Error"),
      }),
    );

    const result = await commitTable(TEST_CONFIG, TEST_TABLE_ID, {
      requirements: [],
      updates: [],
    });
    expect(result).toStrictEqual({
      success: false,
      error: "Commit failed: 500 Server Error",
      status: 500,
    });
  });
});
