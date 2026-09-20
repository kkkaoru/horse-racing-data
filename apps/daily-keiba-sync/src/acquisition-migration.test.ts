// Runs with bun; real local SQLite verifies the pre-existing capture-trigger migration hazard.
import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";
import { expect, test } from "vitest";
import {
  buildD1CapturePlan,
  D1_CAPTURE_TABLE,
} from "../../pc-keiba-r2-catalog/src/d1-change-capture";
import { discoverD1CaptureSchema } from "../../pc-keiba-r2-catalog/src/d1-capture-schema";

test("legacy runs retain RACE, but existing capture triggers must be refreshed after migration", async () => {
  const miniflare = new Miniflare({
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "acquisition-migration-test" },
    modules: true,
    script: "export default {}",
  });
  try {
    const db = await miniflare.getD1Database("DB");
    const initial = await Promise.all(
      ["0001_initial.sql", "0002_catalog_index.sql", "0003_acquisition_window.sql"].map(
        async (name) => await readFile(new URL(`../migrations/${name}`, import.meta.url), "utf8"),
      ),
    );
    await db.exec(
      initial
        .join("\n")
        .split("\n")
        .filter((line) => !line.trimStart().startsWith("--"))
        .join(" "),
    );
    const before = buildD1CapturePlan({
      ...(await discoverD1CaptureSchema(
        "sync_runs",
        async ({ sql, params }) =>
          (
            await db
              .prepare(sql)
              .bind(...params)
              .all<Record<string, unknown>>()
          ).results,
      )),
      captureId: "migration-test",
    });
    await db.batch(
      [before.createOutbox, ...before.triggers.map(({ sql }) => sql)].map((sql) => db.prepare(sql)),
    );
    await db
      .prepare(`insert into sync_runs
      (run_id, dedupe_key, provider, run_date, trigger_kind, lookback_days, status, created_at, updated_at)
      values ('legacy', 'legacy', 'jv', '20260904', 'manual', 2, 'queued', '2026-09-04', '2026-09-04')`)
      .run();
    const migration = await readFile(
      new URL("../migrations/0005_acquisition_data_spec.sql", import.meta.url),
      "utf8",
    );
    await db.exec(
      migration
        .split("\n")
        .filter((line) => !line.trimStart().startsWith("--"))
        .join(" "),
    );
    expect(
      await db
        .prepare("select data_spec, advance_cursor from sync_runs where run_id = 'legacy'")
        .first(),
    ).toStrictEqual({ data_spec: "RACE", advance_cursor: 0 });
    const after = buildD1CapturePlan({
      ...(await discoverD1CaptureSchema(
        "sync_runs",
        async ({ sql, params }) =>
          (
            await db
              .prepare(sql)
              .bind(...params)
              .all<Record<string, unknown>>()
          ).results,
      )),
      captureId: "migration-test",
    });
    expect(new Set([before.schemaHash, after.schemaHash]).size).toBe(2);
    await db.prepare(`delete from ${D1_CAPTURE_TABLE}`).run();
    await db.prepare("update sync_runs set data_spec = 'COMM' where run_id = 'legacy'").run();
    // Old trigger predicates do not include the added column: the update is silently absent.
    expect(await db.prepare(`select count(*) as n from ${D1_CAPTURE_TABLE}`).first()).toStrictEqual(
      { n: 0 },
    );
    // Local-only replacement models the trigger-refresh part, not a complete production cutover.
    await db.batch([
      ...before.triggers.map(({ name }) => db.prepare(`DROP TRIGGER "${name}"`)),
      ...after.triggers.map(({ sql }) => db.prepare(sql)),
    ]);
    await db.prepare("update sync_runs set data_spec = 'RACECOMM' where run_id = 'legacy'").run();
    expect(await db.prepare(`select operation from ${D1_CAPTURE_TABLE}`).all()).toMatchObject({
      results: [{ operation: "update" }],
    });
    expect(
      await db
        .prepare(`select count(*) as n from ${D1_CAPTURE_TABLE} where schema_hash = ?`)
        .bind(after.schemaHash)
        .first(),
    ).toStrictEqual({ n: 1 });
  } finally {
    await miniflare.dispose();
  }
});
