// Runs with bun via Vitest; native DML barriers, not a deployment or input-buffer proof.
import { Miniflare } from "miniflare";
import { expect, test } from "vitest";
import {
  buildD1WriteFencePlan,
  buildD1FreezeQuery,
  type D1WriteFenceInput,
} from "./d1-write-fence";

const input: D1WriteFenceInput = {
  databaseId: "00000000-0000-0000-0000-000000000001",
  owner: "handoff-1",
  epoch: "9223372036854775807",
  tables: ["items"],
};

test.each([
  { databaseId: "invalid" },
  { databaseId: "00000000-0000-0000-0000-000000000001\n" },
  { owner: "" },
  { owner: "bad owner" },
  { owner: "handoff-1\n" },
  { epoch: "0" },
  { epoch: "01" },
  { epoch: "1\n" },
  { epoch: "-1" },
  { epoch: "9223372036854775808" },
])("rejects invalid ownership/epoch on both plan and activation", (override) => {
  expect(() => buildD1WriteFencePlan({ ...input, ...override })).toThrow("fence identity");
  expect(() => buildD1FreezeQuery({ ...input, ...override })).toThrow("fence identity");
});

test.each([
  { tables: [] },
  { tables: ["items", "ITEMS"] },
  { tables: [""] },
  { tables: ["sqlite_master"] },
  { tables: ["_cf_KV"] },
  { tables: ["__pc_keiba_catalog_cdc_v1"] },
  { tables: ["x\0y"] },
  { tables: ["x".repeat(257)] },
  { tables: Array.from({ length: 101 }, (_, n) => String(n)) },
])("rejects unsafe table sets", (override) => {
  expect(() => buildD1WriteFencePlan({ ...input, ...override })).toThrow("fence table set");
});

test("quotes table names and pins complete initialization/trigger content", () => {
  const plan = buildD1WriteFencePlan({ ...input, tables: ['items"x'] });
  expect(plan.triggers).toHaveLength(3);
  expect(plan.triggers[0]?.sql).toMatch(/BEFORE INSERT ON "items""x"/u);
  expect(plan.initialize.params).toStrictEqual([
    "00000000-0000-0000-0000-000000000001",
    "handoff-1",
    "9223372036854775807",
  ]);
  expect(plan.planHash).toMatch(/^[a-f0-9]{64}$/u);
  expect(buildD1FreezeQuery(input).sql).toMatch(
    /owner = \? AND epoch = CAST\(\? AS INTEGER\) AND mode = 'open' RETURNING CAST\(epoch AS TEXT\) AS fence_epoch$/u,
  );
});

test("native open mode does not inflate claims; owner/epoch CAS freezes exact int64", async () => {
  const mf = new Miniflare({
    modules: true,
    script: "export default {fetch(){return new Response('ok')}}",
    d1Databases: ["DB"],
  });
  try {
    const db = await mf.getD1Database("DB");
    await db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)");
    const plan = buildD1WriteFencePlan(input);
    await db.batch([
      db.prepare(plan.createFence),
      db.prepare(plan.initialize.sql).bind(...plan.initialize.params),
      ...plan.triggers.map((trigger) => db.prepare(trigger.sql)),
    ]);
    const claim = await db
      .prepare("INSERT INTO items VALUES (1,'original') RETURNING 1 AS changed")
      .all<{ changed: number }>();
    expect(claim.results).toStrictEqual([{ changed: 1 }]);
    expect(claim.meta.changes).toBe(1);
    const wrong = buildD1FreezeQuery({ ...input, owner: "other-owner" });
    expect(
      (
        await db
          .prepare(wrong.sql)
          .bind(...wrong.params)
          .all()
      ).results,
    ).toStrictEqual([]);
    const stale = buildD1FreezeQuery({ ...input, epoch: "1" });
    expect(
      (
        await db
          .prepare(stale.sql)
          .bind(...stale.params)
          .all()
      ).results,
    ).toStrictEqual([]);
    const freeze = buildD1FreezeQuery(input);
    expect(
      (
        await db
          .prepare(freeze.sql)
          .bind(...freeze.params)
          .all()
      ).results,
    ).toStrictEqual([{ fence_epoch: "9223372036854775807" }]);
    expect(
      (
        await db
          .prepare(freeze.sql)
          .bind(...freeze.params)
          .all()
      ).results,
    ).toStrictEqual([]);
    await expect(
      db.prepare("INSERT INTO items VALUES (2,'late-old-handler')").run(),
    ).rejects.toThrow("source is fenced");
    expect((await db.prepare("SELECT value FROM items").all()).results).toStrictEqual([
      { value: "original" },
    ]);
  } finally {
    await mf.dispose();
  }
});

test.each([
  "INSERT OR REPLACE INTO items VALUES (1,'replacement')",
  "UPDATE items SET value='changed' WHERE id=1",
  "DELETE FROM items WHERE id=1",
])(
  "native frozen source rejects mutation without changing original rows: %s",
  async (statement) => {
    const mf = new Miniflare({
      modules: true,
      script: "export default {fetch(){return new Response('ok')}}",
      d1Databases: ["DB"],
    });
    try {
      const db = await mf.getD1Database("DB");
      await db.exec(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES (1,'original')",
      );
      const plan = buildD1WriteFencePlan(input);
      const freeze = buildD1FreezeQuery(input);
      await db.batch([
        db.prepare(plan.createFence),
        db.prepare(plan.initialize.sql).bind(...plan.initialize.params),
        ...plan.triggers.map((trigger) => db.prepare(trigger.sql)),
        db.prepare(freeze.sql).bind(...freeze.params),
      ]);
      await expect(db.prepare(statement).run()).rejects.toThrow("source is fenced");
      expect((await db.prepare("SELECT value FROM items").all()).results).toStrictEqual([
        { value: "original" },
      ]);
    } finally {
      await mf.dispose();
    }
  },
);

test("missing control row fails closed and a batch cannot partially write before a fenced mutation", async () => {
  const mf = new Miniflare({
    modules: true,
    script: "export default {fetch(){return new Response('ok')}}",
    d1Databases: ["DB"],
  });
  try {
    const db = await mf.getD1Database("DB");
    await db.exec(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT); CREATE TABLE other (value TEXT)",
    );
    const plan = buildD1WriteFencePlan(input);
    // No initialize statement: a missing or wrong-database control record must not reopen writes.
    await db.batch([
      db.prepare(plan.createFence),
      ...plan.triggers.map((trigger) => db.prepare(trigger.sql)),
    ]);
    await expect(
      db.batch([
        db.prepare("INSERT INTO other VALUES ('must-roll-back')"),
        db.prepare("INSERT INTO items VALUES (1,'blocked')"),
      ]),
    ).rejects.toThrow("source is fenced");
    expect((await db.prepare("SELECT value FROM other").all()).results).toStrictEqual([]);
    await db
      .prepare(plan.initialize.sql)
      .bind("00000000-0000-0000-0000-000000000002", "handoff-1", "1")
      .run();
    await expect(
      db.prepare("INSERT INTO items VALUES (1,'wrong-database-control')").run(),
    ).rejects.toThrow("source is fenced");
  } finally {
    await mf.dispose();
  }
});
