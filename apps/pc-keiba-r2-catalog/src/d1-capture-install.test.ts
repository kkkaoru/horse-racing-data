// Runs with bun via Vitest; read-only preparation rejects stale or ambiguous installation state.
import { Miniflare } from "miniflare";
import { expect, test, vi } from "vitest";
import {
  applyD1CaptureInstall,
  prepareD1CaptureInstall,
  type D1CaptureInstallInput,
  type D1CaptureInstallDependencies,
} from "./d1-capture-install";
import { discoverD1CaptureSchema, type D1CaptureMetadataQuery } from "./d1-capture-schema";
import { buildD1CapturePlan, D1_CAPTURE_TABLE } from "./d1-change-capture";

const metadata = (objects: readonly Record<string, unknown>[]) =>
  vi.fn<D1CaptureMetadataQuery>(async ({ sql }) => {
    if (sql.startsWith("SELECT type, sql"))
      return [{ type: "table", sql: "CREATE TABLE items (id INTEGER PRIMARY KEY)" }];
    if (sql.startsWith("PRAGMA table_xinfo")) return [{ cid: 0, name: "id", hidden: 0 }];
    if (sql.startsWith("SELECT type, name")) return objects;
    if (sql.startsWith("SELECT rowid") || sql.startsWith("PRAGMA index_list")) return [];
    throw new Error("Unexpected metadata query");
  });
const fixture = async () => {
  const schema = await discoverD1CaptureSchema("items", metadata([]));
  const plan = buildD1CapturePlan({ ...schema, captureId: "install-test" });
  const input: D1CaptureInstallInput = {
    table: "items",
    captureId: "install-test",
    expectedDefinitionHash: schema.definitionHash,
    expectedPlanHash: plan.planHash,
  };
  const objects = [
    { name: D1_CAPTURE_TABLE, type: "table", tbl_name: D1_CAPTURE_TABLE, sql: plan.createOutbox },
    ...plan.triggers.map(({ name, sql }) => ({ name, type: "trigger", tbl_name: "items", sql })),
  ];
  return { input, objects, plan };
};

test("never executes when persisting installation intent fails", async () => {
  const { input } = await fixture();
  const executeBatch = vi.fn<D1CaptureInstallDependencies["executeBatch"]>();
  await expect(
    applyD1CaptureInstall(input, {
      query: metadata([]),
      persistIntent: vi
        .fn<D1CaptureInstallDependencies["persistIntent"]>()
        .mockRejectedValue(new Error("disk failed")),
      executeBatch,
    }),
  ).rejects.toThrow("disk failed");
  expect(executeBatch).not.toHaveBeenCalled();
});

test("does not retry an uncertain acknowledgement or trust an incomplete execution", async () => {
  const { input } = await fixture();
  const persistIntent = vi.fn<D1CaptureInstallDependencies["persistIntent"]>().mockResolvedValue();
  const executeBatch = vi
    .fn<D1CaptureInstallDependencies["executeBatch"]>()
    .mockRejectedValueOnce(new Error("ack lost"))
    .mockResolvedValue();
  await expect(
    applyD1CaptureInstall(input, { query: metadata([]), persistIntent, executeBatch }),
  ).rejects.toThrow("ack lost");
  expect(executeBatch).toHaveBeenCalledTimes(1);
  await expect(
    applyD1CaptureInstall(input, { query: metadata([]), persistIntent, executeBatch }),
  ).rejects.toThrow("incomplete after execution");
  expect(executeBatch).toHaveBeenCalledTimes(2);
});

test("already installed DDL is inspected without re-executing or advancing a watermark", async () => {
  const { input, objects } = await fixture();
  const executeBatch = vi.fn<D1CaptureInstallDependencies["executeBatch"]>();
  const persistIntent = vi.fn<D1CaptureInstallDependencies["persistIntent"]>().mockResolvedValue();
  expect(
    (await applyD1CaptureInstall(input, { query: metadata(objects), persistIntent, executeBatch }))
      .ddlVerified,
  ).toBe(true);
  expect(executeBatch).not.toHaveBeenCalled();
  expect(persistIntent).toHaveBeenCalledWith(expect.objectContaining({ statements: [] }));
});

test("prepares six missing DDL statements without executing any", async () => {
  const { input } = await fixture();
  const query = metadata([]);
  const result = await prepareD1CaptureInstall(input, query);
  expect(result.ddlVerified).toBe(false);
  expect(result.statements).toHaveLength(6);
  expect(
    query.mock.calls.some(([request]) =>
      /^(?:CREATE|INSERT|UPDATE|DELETE|DROP)/u.test(request.sql),
    ),
  ).toBe(false);
});

test("recognizes all exact objects and fills only missing triggers", async () => {
  const { input, objects } = await fixture();
  expect((await prepareD1CaptureInstall(input, metadata(objects))).ddlVerified).toBe(true);
  expect((await prepareD1CaptureInstall(input, metadata(objects))).statements).toStrictEqual([]);
  const partial = await prepareD1CaptureInstall(input, metadata(objects.slice(0, 3)));
  expect(partial.statements).toHaveLength(3);
  expect(partial.ddlVerified).toBe(false);
});

test.each(["expectedDefinitionHash", "expectedPlanHash"])(
  "rejects malformed fingerprints before I/O",
  async (field) => {
    const { input } = await fixture();
    const query = metadata([]);
    await expect(prepareD1CaptureInstall({ ...input, [field]: "bad" }, query)).rejects.toThrow(
      "fingerprints",
    );
    expect(query).not.toHaveBeenCalled();
  },
);

test("rejects source drift and stale generator bytes", async () => {
  const { input } = await fixture();
  await expect(
    prepareD1CaptureInstall({ ...input, expectedDefinitionHash: "0".repeat(64) }, metadata([])),
  ).rejects.toThrow("source definition changed");
  await expect(
    prepareD1CaptureInstall({ ...input, expectedPlanHash: "0".repeat(64) }, metadata([])),
  ).rejects.toThrow("plan changed");
});

test.each([
  { name: null },
  { name: "foreign_trigger" },
  { name: D1_CAPTURE_TABLE, type: "view" },
  { name: D1_CAPTURE_TABLE, type: "table", tbl_name: "wrong" },
  { name: D1_CAPTURE_TABLE, type: "table", tbl_name: D1_CAPTURE_TABLE, sql: "changed" },
])("rejects foreign or changed schema objects", async (row) => {
  const { input } = await fixture();
  await expect(prepareD1CaptureInstall(input, metadata([row]))).rejects.toThrow(
    "capture schema object",
  );
});

test("rejects added journal indexes that could reject source writes or change their cost", async () => {
  const { input, objects } = await fixture();
  const base = metadata(objects);
  const query: D1CaptureMetadataQuery = async (request) =>
    request.sql === `PRAGMA index_list('${D1_CAPTURE_TABLE}')`
      ? [{ name: "foreign_unique" }]
      : base(request);
  await expect(prepareD1CaptureInstall(input, query)).rejects.toThrow("journal index");
});

test("refuses duplicate objects and cannot recreate a lost capture journal", async () => {
  const { input, objects } = await fixture();
  await expect(prepareD1CaptureInstall(input, metadata([...objects, ...objects]))).rejects.toThrow(
    "capture schema object",
  );
  await expect(prepareD1CaptureInstall(input, metadata(objects.slice(1)))).rejects.toThrow(
    "journal missing",
  );
});

test("native SQLite metadata is byte-compatible and supports safe inspection after partial installation", async () => {
  const runtime = new Miniflare({
    modules: true,
    script: "export default {}",
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "install-test" },
  });
  try {
    const db = await runtime.getD1Database("DB");
    await db.prepare("CREATE TABLE items (id INTEGER PRIMARY KEY)").run();
    const query: D1CaptureMetadataQuery = async ({ sql, params }) =>
      (
        await db
          .prepare(sql)
          .bind(...params)
          .all<Record<string, unknown>>()
      ).results;
    const schema = await discoverD1CaptureSchema("items", query);
    const plan = buildD1CapturePlan({ ...schema, captureId: "install-test" });
    const input = {
      table: "items",
      captureId: "install-test",
      expectedDefinitionHash: schema.definitionHash,
      expectedPlanHash: plan.planHash,
    };
    const initial = await prepareD1CaptureInstall(input, query);
    expect(initial.statements).toHaveLength(6);
    await db.prepare(plan.createOutbox).run();
    const pending = await prepareD1CaptureInstall(input, query);
    expect(pending.statements).toHaveLength(5);
    const events: string[] = [];
    expect(
      (
        await applyD1CaptureInstall(input, {
          query,
          persistIntent: async (intent) => {
            events.push("intent");
            expect(intent.statements).toHaveLength(5);
          },
          executeBatch: async (statements) => {
            events.push("execute");
            await db.batch(statements.map((sql) => db.prepare(sql)));
          },
        })
      ).ddlVerified,
    ).toBe(true);
    expect(events).toStrictEqual(["intent", "execute"]);
    expect((await prepareD1CaptureInstall(input, query)).ddlVerified).toBe(true);
    await db
      .prepare("CREATE TRIGGER foreign_trigger AFTER INSERT ON items BEGIN SELECT 1; END")
      .run();
    await expect(prepareD1CaptureInstall(input, query)).rejects.toThrow("capture schema object");
  } finally {
    await runtime.dispose();
  }
});
