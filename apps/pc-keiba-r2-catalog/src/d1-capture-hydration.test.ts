// Runs with bun via Vitest; bounded key reads and native lossless D1 encoding.
import { Miniflare } from "miniflare";
import { expect, test } from "vitest";
import {
  buildD1CaptureKeyQuery,
  parseD1CaptureKeyRead,
  type D1CaptureKeyRequest,
} from "./d1-capture-hydration";

const request: D1CaptureKeyRequest = {
  table: "items",
  columns: [{ name: "v" }],
  keys: ["2", "-9223372036854775808", "9007199254740993", "9223372036854775807", "2"],
};

test("deduplicates and numerically orders exact int64 bindings without range scanning", () => {
  const query = buildD1CaptureKeyQuery(request);
  expect(query.params).toStrictEqual([
    "-9223372036854775808",
    "2",
    "9007199254740993",
    "9223372036854775807",
  ]);
  expect(query.sql).toMatch(
    /WHERE rowid IN \(CAST\(\? AS INTEGER\), CAST\(\? AS INTEGER\), CAST\(\? AS INTEGER\), CAST\(\? AS INTEGER\)\) ORDER BY rowid$/u,
  );
});

test.each([
  "01",
  "-0",
  "1\n",
  "9223372036854775808",
  "-9223372036854775809",
  "123456789012345678901",
  "0;DROP TABLE items",
  "+1",
])("rejects noncanonical or out-of-range keys", (key) => {
  expect(() => buildD1CaptureKeyQuery({ ...request, keys: [key] })).toThrow(
    "Invalid bounded capture key list",
  );
});

test("bounds the original key list and escapes identifiers", () => {
  expect(() => buildD1CaptureKeyQuery({ ...request, keys: [] })).toThrow("bounded capture key");
  expect(() =>
    buildD1CaptureKeyQuery({ ...request, keys: Array.from({ length: 101 }, () => "1") }),
  ).toThrow("bounded capture key");
  expect(buildD1CaptureKeyQuery({ ...request, keys: ["0"], table: 'items"x' }).sql).toMatch(
    /FROM "items""x" WHERE/u,
  );
  expect(
    buildD1CaptureKeyQuery({
      ...request,
      keys: Array.from({ length: 100 }, (_, index) => String(index)),
    }).params,
  ).toHaveLength(100);
});

test("reports absence but never verifies a source fence or manufactures tombstones", () => {
  expect(
    parseD1CaptureKeyRead({
      request,
      rows: [{ row_key: "2", payload: '{"v":{"type":"null","value":null}}' }],
    }),
  ).toStrictEqual({
    rows: [{ row_key: "2", payload: '{"v":{"type":"null","value":null}}' }],
    absentAtRead: ["-9223372036854775808", "9007199254740993", "9223372036854775807"],
    sourceFenceVerified: false,
  });
  expect(parseD1CaptureKeyRead({ request: { ...request, keys: ["2"] }, rows: [] })).toStrictEqual({
    rows: [],
    absentAtRead: ["2"],
    sourceFenceVerified: false,
  });
});

test.each(
  [
    [{ row_key: "3", payload: '{"v":null}' }],
    [{ row_key: 2, payload: '{"v":null}' }],
    [{ row_key: "2", payload: '{"wrong":null}' }],
    [
      { row_key: "2", payload: '{"v":null}' },
      { row_key: "2", payload: '{"v":null}' },
    ],
    [
      { row_key: "9007199254740993", payload: '{"v":null}' },
      { row_key: "2", payload: '{"v":null}' },
    ],
  ].map((rows) => ({ rows })),
)("rejects extra, malformed, duplicate, or unordered identities", ({ rows }) => {
  expect(() => parseD1CaptureKeyRead({ request, rows })).toThrow();
});

test("native D1 keeps int64/blob/real/null data with a shadowed rowid column", async () => {
  const mf = new Miniflare({
    modules: true,
    script: "export default {fetch(){return new Response('ok')}}",
    d1Databases: ["DB"],
  });
  try {
    const db = await mf.getD1Database("DB");
    await db.exec("CREATE TABLE items (rowid TEXT, n INTEGER, bytes BLOB, r REAL, missing TEXT)");
    await db.exec(
      "INSERT INTO items (_rowid_,rowid,n,bytes,r) VALUES (9007199254740993,'shadow',9223372036854775807,X'00FF',0.1)",
    );
    const lookup: D1CaptureKeyRequest = {
      table: "items",
      columns: [
        { name: "rowid" },
        { name: "n" },
        { name: "bytes" },
        { name: "r" },
        { name: "missing" },
      ],
      keys: ["9007199254740993", "9007199254740994"],
    };
    const query = buildD1CaptureKeyQuery(lookup);
    expect(query.sql).toMatch(/WHERE _rowid_ IN/u);
    const response = await db
      .prepare(query.sql)
      .bind(...query.params)
      .all<Record<string, unknown>>();
    const observed = parseD1CaptureKeyRead({ request: lookup, rows: response.results });
    expect(observed.absentAtRead).toStrictEqual(["9007199254740994"]);
    expect(observed.rows[0]?.row_key).toBe("9007199254740993");
    expect(observed.rows[0]?.payload).toBe(
      '{"rowid":{"type":"text","value":"shadow"},"n":{"type":"integer","value":"9223372036854775807"},"bytes":{"type":"blob","value":"00FF"},"r":{"type":"real","value":"0.10000000000000001"},"missing":{"type":"null","value":null}}',
    );
    // Reusing a previously absent rowid demonstrates why absence is not a permanent deletion.
    await db.exec("INSERT INTO items (_rowid_,rowid) VALUES (9007199254740994,'reused')");
    const next = await db
      .prepare(query.sql)
      .bind(...query.params)
      .all<Record<string, unknown>>();
    expect(
      parseD1CaptureKeyRead({ request: lookup, rows: next.results }).absentAtRead,
    ).toStrictEqual([]);
  } finally {
    await mf.dispose();
  }
});
