// Runs with bun via Vitest; D1 HTTP is mocked.
import { expect, test, vi } from "vitest";
import {
  buildD1SnapshotPageQuery,
  buildD1SnapshotRowEncoding,
  parseD1SnapshotPage,
  queryD1Snapshot,
  type D1SnapshotPageRequest,
} from "./d1-snapshot";

const request: D1SnapshotPageRequest = {
  afterRowId: null,
  columns: [{ name: "amount" }],
  limit: 100,
  table: "events",
};

test("starts inclusively at minimum SQLite int64 without converting to JS number", () => {
  const query = buildD1SnapshotPageQuery(request);
  expect(query.params).toStrictEqual(["-9223372036854775808", 100]);
  expect(query.sql).toMatch(/rowid >= CAST\(\? AS INTEGER\)/);
  expect(query.sql).toMatch(/WHEN 'blob' THEN hex\("amount"\)/);
  expect(query.sql).toMatch(/WHEN 'real' THEN printf\('%!.17g', "amount"\)/);
  expect(query.sql).toMatch(/ELSE CAST\("amount" AS TEXT\)/);
});

test("captures wide tables using bounded JSON argument groups", () => {
  const query = buildD1SnapshotPageQuery({
    ...request,
    columns: Array.from({ length: 47 }, (_, index) => ({ name: `c${index}` })),
  });
  expect(query.sql.match(/json_patch/gu)).toHaveLength(2);
  expect(query.sql).toContain("'c46', json_object('type'");
});

test("reuses exactly typed row encoding for trigger-qualified values", () => {
  const encoded = buildD1SnapshotRowEncoding(request, "NEW");
  expect(encoded.rowId).toBe("NEW.rowid");
  expect(encoded.payloadSql).toContain('typeof(NEW."amount")');
  const boundary = buildD1SnapshotPageQuery({
    ...request,
    columns: Array.from({ length: 17 }, (_, index) => ({ name: `c${index}` })),
  });
  expect(boundary.sql.match(/json_patch/gu)).toHaveLength(1);
});

test("quotes source identifiers and uses an unshadowed rowid alias", () => {
  const query = buildD1SnapshotPageQuery({
    ...request,
    afterRowId: "9007199254740993",
    table: 'a"b',
    columns: [{ name: "ROWID" }, { name: "o'hare" }],
  });
  expect(query.sql).toMatch(/CAST\(_rowid_ AS TEXT\)/);
  expect(query.sql).toMatch(/FROM "a""b"/);
  expect(query.sql).toMatch(/'o''hare'/);
  expect(query.params).toStrictEqual(["9007199254740993", 100]);
});

test.each<Partial<D1SnapshotPageRequest>>([
  { afterRowId: "not-a-number" },
  { afterRowId: "-9223372036854775809" },
  { afterRowId: "9223372036854775808" },
  { limit: 0 },
  { limit: 1001 },
  { limit: 1.5 },
  { table: "" },
  { table: "_cf_KV" },
  { table: "sqlite_sequence" },
  { columns: [] },
  { columns: [{ name: "a" }, { name: "a" }] },
  { columns: Array.from({ length: 1001 }, (_, i) => ({ name: `c${i}` })) },
  { columns: [{ name: "rowid" }, { name: "_rowid_" }, { name: "oid" }] },
])("rejects unsafe query inputs before executing SQL: %j", (overrides) => {
  expect(() => buildD1SnapshotPageQuery({ ...request, ...overrides })).toThrow();
});

test("preserves exact integer and BLOB payloads as source-typed strings", () => {
  expect(
    parseD1SnapshotPage(
      [
        {
          row_key: "-9223372036854775808",
          payload: '{"amount":{"type":"integer","value":"9223372036854775807"}}',
        },
      ],
      request,
    ),
  ).toStrictEqual([
    {
      row_key: "-9223372036854775808",
      payload: '{"amount":{"type":"integer","value":"9223372036854775807"}}',
    },
  ]);
  expect(
    parseD1SnapshotPage(
      [{ row_key: "9007199254740994", payload: '{"amount":{"type":"blob","value":"00FF"}}' }],
      { ...request, afterRowId: "9007199254740993" },
    ),
  ).toStrictEqual([
    { row_key: "9007199254740994", payload: '{"amount":{"type":"blob","value":"00FF"}}' },
  ]);
});

test.each(
  [
    [{ row_key: 1, payload: "{}" }],
    [{ row_key: "x", payload: "{}" }],
    [{ row_key: "1", payload: null }],
    [{ row_key: "9223372036854775808", payload: "{}" }],
    [{ row_key: "1", payload: "null" }],
    [{ row_key: "1", payload: "{}" }],
    [{ row_key: "1", payload: '{"wrong":1}' }],
  ].map((rows) => ({ rows })),
)("rejects malformed pages: %j", ({ rows }) => {
  expect(() => parseD1SnapshotPage(rows, request)).toThrow();
});

test("rejects repeated/out-of-order cursors and excessive pages", () => {
  const rows = [
    { row_key: "2", payload: '{"amount":{}}' },
    { row_key: "2", payload: '{"amount":{}}' },
  ];
  expect(() => parseD1SnapshotPage(rows, request)).toThrow("cursor did not advance");
  expect(() => parseD1SnapshotPage(rows, { ...request, limit: 1 })).toThrow(
    "exceeded requested limit",
  );
});

test("queries D1 with an explicit bounded statement and parameters", async () => {
  const fetchImpl = vi.fn().mockResolvedValue(
    Response.json({
      success: true,
      result: [{ success: true, results: [{ row_key: "1", payload: "{}" }] }],
    }),
  );
  expect(
    await queryD1Snapshot(
      { accountId: "account", databaseId: "database", token: "secret", fetchImpl },
      { sql: "SELECT 1", params: [] },
    ),
  ).toStrictEqual([{ row_key: "1", payload: "{}" }]);
  expect(fetchImpl).toHaveBeenCalledWith(
    "https://api.cloudflare.com/client/v4/accounts/account/d1/database/database/query",
    expect.objectContaining({ method: "POST", body: '{"sql":"SELECT 1","params":[]}' }),
  );
});

test.each([
  null,
  { success: false },
  { success: true, result: [] },
  { success: true, result: [null] },
  { success: true, result: [{ success: false }] },
  { success: true, result: [{ success: true, results: [null] }] },
])("rejects invalid D1 response envelopes: %j", async (body) => {
  const fetchImpl = vi.fn().mockResolvedValue(Response.json(body));
  await expect(
    queryD1Snapshot(
      { accountId: "account", databaseId: "database", token: "secret", fetchImpl },
      { sql: "SELECT 1", params: [] },
    ),
  ).rejects.toThrow();
});

test("does not expose an upstream error body", async () => {
  const fetchImpl = vi
    .fn()
    .mockResolvedValue(new Response("private upstream detail", { status: 403 }));
  await expect(
    queryD1Snapshot(
      { accountId: "account", databaseId: "database", token: "secret", fetchImpl },
      { sql: "SELECT 1", params: [] },
    ),
  ).rejects.toThrow("D1 snapshot query returned HTTP 403");
});
