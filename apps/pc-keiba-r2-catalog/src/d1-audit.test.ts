// Runs with bun via Vitest; only immutable revision-keyed audit results are cached.
import { createHash } from "node:crypto";
import { expect, test, vi } from "vitest";
import { boundedAuditFetch, parseD1AuditInput, queryD1Audit, type D1AuditInput } from "./d1-audit";

const payload = {
  kind: "d1-baseline-manifest-v1",
  promoted: false,
  verification: "batch_readback_and_catalog_count",
  copied_rows: 2,
  source_schema: [{ name: "v" }],
};
const manifest = (value: unknown = payload) => {
  const body: string = JSON.stringify(value);
  return {
    batch_id: createHash("sha256")
      .update('["s", "db", "events"]' + JSON.stringify(["0", body]) + "\n")
      .digest("hex"),
    payload: body,
  };
};
const input: D1AuditInput = {
  operation: "query",
  snapshotId: "s",
  databaseName: "db",
  tableName: "events",
  manifestId: manifest().batch_id,
  afterRowId: null,
  limit: 1,
  filters: [],
};
const row = { row_key: "1", payload: '{"v":{"type":"text","value":"hello"}}' };
const dependencies = () => ({
  namespace: "pc_keiba",
  cacheScope: "account/bucket",
  cacheOrigin: "https://audit.example.test",
  cache: {
    match: vi.fn().mockResolvedValue(undefined),
    put: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn(),
  },
  kv: {
    get: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn(),
  },
  query: vi
    .fn()
    .mockResolvedValueOnce([manifest()])
    .mockResolvedValue([row, { ...row, row_key: "2" }]),
});

test("bounds provider bodies and adds a query deadline", async () => {
  const fetcher = vi
    .fn()
    .mockResolvedValueOnce(new Response(null, { status: 204 }))
    .mockResolvedValueOnce(new Response("ok"))
    .mockResolvedValueOnce(new Response(new Uint8Array(8 * 1024 * 1024 + 1)));
  const call = boundedAuditFetch(fetcher);
  expect((await call("https://provider.test")).status).toBe(204);
  expect(await (await call("https://provider.test")).text()).toBe("ok");
  await expect((await call("https://provider.test")).text()).rejects.toThrow("byte limit");
  expect(fetcher.mock.calls[0]?.[1].signal).toBeInstanceOf(AbortSignal);
});

test("parses bounded requests and manifest discovery defaults", () => {
  expect(parseD1AuditInput({ ...input })).toStrictEqual(input);
  expect(
    parseD1AuditInput({
      operation: "manifest",
      snapshotId: "s",
      databaseName: "db",
      tableName: "events",
    }),
  ).toMatchObject({ limit: 25, afterRowId: null, filters: [], manifestId: "" });
  expect(
    parseD1AuditInput({
      ...input,
      afterRowId: "-9223372036854775808",
      filters: [{ column: "v", value: null }],
    }).filters,
  ).toHaveLength(1);
});

test.each([
  { operation: "write" },
  { snapshotId: "bad'" },
  { snapshotId: 1 },
  { manifestId: "a" },
  { afterRowId: 1 },
  { afterRowId: "bad" },
  { afterRowId: "-9223372036854775809" },
  { afterRowId: "9223372036854775808" },
  { limit: "1" },
  { limit: 0 },
  { limit: 101 },
  { limit: 1.5 },
  { filters: {} },
  { filters: Array.from({ length: 9 }, () => ({ column: "v", value: "1" })) },
  { filters: [null] },
  { filters: [{ column: "v", value: 1 }] },
  { filters: [{ column: "v", value: "a".repeat(513) }] },
])("rejects unsafe audit input: %j", (fields) => {
  expect(() => parseD1AuditInput({ ...input, ...fields })).toThrow();
});

test("discovers completed manifests without caching mutable discovery", async () => {
  const deps = dependencies();
  const result: Response = await queryD1Audit({ ...input, operation: "manifest" }, deps);
  expect(await result.json()).toMatchObject({
    copiedRows: 2,
    promoted: false,
    manifestId: input.manifestId,
  });
  expect(deps.cache.match).not.toHaveBeenCalled();
  expect(deps.kv.put).not.toHaveBeenCalled();
});

test("returns a lookahead cursor, writes both cache layers and keeps HTTP private", async () => {
  const deps = dependencies();
  const result: Response = await queryD1Audit(input, deps);
  expect(await result.json()).toMatchObject({ rows: [row], nextAfterRowId: "1", promoted: false });
  expect(result.headers.get("Cache-Control")).toBe("no-store");
  expect(deps.cache.put.mock.calls[0]?.[1].headers.get("Cache-Control")).toBe(
    "public, max-age=600",
  );
  expect(deps.kv.put.mock.calls[0]?.[2]).toStrictEqual({ expirationTtl: 86400 });
  expect(result.headers.get("X-Catalog-Cache")).toBe("origin");
  expect(deps.cache.put.mock.calls[0]?.[0].url).toMatch(
    /^https:\/\/audit\.example\.test\/__internal-cache\//u,
  );
});

test("cache-source headers distinguish edge and KV without exposing cacheable HTTP", async () => {
  const edge = dependencies();
  edge.cache.match.mockResolvedValue(
    Response.json({ manifestId: input.manifestId, promoted: false, rows: [] }),
  );
  expect((await queryD1Audit(input, edge)).headers.get("X-Catalog-Cache")).toBe("edge");
  const kv = dependencies();
  kv.kv.get.mockResolvedValue(
    JSON.stringify({ manifestId: input.manifestId, promoted: false, rows: [] }),
  );
  expect((await queryD1Audit(input, kv)).headers.get("X-Catalog-Cache")).toBe("kv");
});

test("Cache API hits avoid all Catalog/KV reads", async () => {
  const deps = dependencies();
  deps.cache.match.mockResolvedValue(
    Response.json({ manifestId: input.manifestId, promoted: false, rows: [] }),
  );
  const result = await queryD1Audit(input, deps);
  expect(result.headers.get("Cache-Control")).toBe("no-store");
  expect(deps.query).not.toHaveBeenCalled();
  expect(deps.kv.get).not.toHaveBeenCalled();
});

test("KV hits warm the local edge cache", async () => {
  const deps = dependencies();
  deps.kv.get.mockResolvedValue(
    JSON.stringify({ manifestId: input.manifestId, promoted: false, rows: [] }),
  );
  await queryD1Audit(input, deps);
  expect(deps.query).not.toHaveBeenCalled();
  expect(deps.cache.put).toHaveBeenCalledOnce();
});

test.each([
  "garbage",
  "{}",
  JSON.stringify({ manifestId: "wrong", promoted: false, rows: [] }),
  JSON.stringify({ manifestId: input.manifestId, promoted: true, rows: [] }),
  JSON.stringify({ manifestId: input.manifestId, promoted: false, rows: null }),
  "x".repeat(2 * 1024 * 1024 + 1),
])("ignores corrupt or mismatched cache values", async (cached) => {
  const deps = dependencies();
  deps.kv.get.mockResolvedValue(cached);
  expect((await queryD1Audit(input, deps)).status).toBe(200);
  expect(deps.query).toHaveBeenCalledTimes(2);
});

test("cache outages do not break Catalog reads", async () => {
  const deps = dependencies();
  deps.cache.match.mockRejectedValue(new Error("edge"));
  deps.cache.put.mockRejectedValue(new Error("edge"));
  deps.kv.get.mockRejectedValue(new Error("kv"));
  deps.kv.put.mockRejectedValue(new Error("kv"));
  expect((await queryD1Audit(input, deps)).status).toBe(200);
});

test("KV warmup failures do not break a hit", async () => {
  const deps = dependencies();
  deps.kv.get.mockResolvedValue(
    JSON.stringify({ manifestId: input.manifestId, promoted: false, rows: [] }),
  );
  deps.cache.put.mockRejectedValue(new Error("edge"));
  expect((await queryD1Audit(input, deps)).status).toBe(200);
});

test("bad cached HTTP status and failed cached streams are ignored", async () => {
  const deps = dependencies();
  deps.cache.match.mockResolvedValue(new Response("bad", { status: 503 }));
  expect((await queryD1Audit(input, deps)).status).toBe(200);
  const broken = dependencies();
  broken.cache.match.mockResolvedValue(
    new Response(
      new ReadableStream({
        start(controller) {
          controller.error(new Error("broken"));
        },
      }),
    ),
  );
  expect((await queryD1Audit(input, broken)).status).toBe(200);
});

test("scopes cache keys by namespace, warehouse and manifest revision", async () => {
  const first = dependencies();
  await queryD1Audit(input, first);
  const second = dependencies();
  second.cacheScope = "another-account/bucket";
  await queryD1Audit(input, second);
  expect(first.kv.put.mock.calls[0]?.[0]).not.toBe(second.kv.put.mock.calls[0]?.[0]);
});

test("quotes field values, supports typed NULLs and numeric rowid pagination", async () => {
  const deps = dependencies();
  deps.query.mockReset().mockResolvedValueOnce([manifest()]).mockResolvedValue([]);
  const result = await queryD1Audit(
    {
      ...input,
      afterRowId: "7",
      filters: [
        { column: "v", value: "x' OR TRUE" },
        { column: "v", value: null },
      ],
    },
    deps,
  );
  expect(deps.query.mock.calls[1]?.[0]).toContain("'x'' OR TRUE'");
  expect(deps.query.mock.calls[1]?.[0]).toContain("'type') = 'null'");
  expect(deps.query.mock.calls[1]?.[0]).toContain("CAST(row_key AS BIGINT) > CAST('7' AS BIGINT)");
  expect(await result.json()).toMatchObject({ nextAfterRowId: null });
});

test("rejects unknown columns before the data query", async () => {
  const deps = dependencies();
  await expect(
    queryD1Audit({ ...input, filters: [{ column: "unknown", value: "1" }] }, deps),
  ).rejects.toThrow("Unknown baseline column");
  expect(deps.query).toHaveBeenCalledOnce();
});

test("returns not-ready rather than caching an incomplete partition", async () => {
  const deps = dependencies();
  deps.query.mockReset().mockResolvedValue([]);
  expect((await queryD1Audit(input, deps)).status).toBe(409);
  expect(deps.kv.put).not.toHaveBeenCalled();
});

test("rejects ambiguous or fingerprint-invalid manifests", async () => {
  const deps = dependencies();
  deps.query.mockReset().mockResolvedValue([manifest(), manifest()]);
  await expect(queryD1Audit(input, deps)).rejects.toThrow("Ambiguous");
  deps.query.mockResolvedValue([{ batch_id: "invalid", payload: "{}" }]);
  await expect(queryD1Audit(input, deps)).rejects.toThrow("fingerprint");
  deps.query.mockResolvedValue([{}]);
  await expect(queryD1Audit(input, deps)).rejects.toThrow("manifest row");
});

test.each([
  null,
  { ...payload, kind: "other" },
  { ...payload, promoted: true },
  { ...payload, verification: "unverified" },
  { ...payload, copied_rows: "1" },
  { ...payload, copied_rows: 1.5 },
  { ...payload, copied_rows: -1 },
  { ...payload, source_schema: null },
  { ...payload, source_schema: [] },
  { ...payload, source_schema: [null] },
  { ...payload, source_schema: [{ name: 1 }] },
])("rejects malformed or promoted baseline metadata", async (value) => {
  const deps = dependencies();
  deps.query.mockReset().mockResolvedValue([manifest(value)]);
  await expect(queryD1Audit(input, deps)).rejects.toThrow();
});

test("rejects the wrong valid manifest revision and unsafe namespace", async () => {
  const deps = dependencies();
  deps.query.mockReset().mockResolvedValue([manifest({ ...payload, copied_rows: 3 })]);
  await expect(queryD1Audit(input, deps)).rejects.toThrow("revision mismatch");
  deps.namespace = "bad;";
  await expect(queryD1Audit(input, deps)).rejects.toThrow("namespace");
});

test("does not cache oversized row pages", async () => {
  const deps = dependencies();
  deps.query
    .mockReset()
    .mockResolvedValueOnce([manifest()])
    .mockResolvedValue([
      {
        row_key: "1",
        payload: JSON.stringify({ v: { type: "text", value: "x".repeat(2 * 1024 * 1024) } }),
      },
    ]);
  expect((await queryD1Audit(input, deps)).status).toBe(200);
  expect(deps.cache.put).not.toHaveBeenCalled();
  expect(deps.kv.put).not.toHaveBeenCalled();
});
