// Runs with bun via Vitest; this mapping has no external I/O.
import { expect, test } from "vitest";
import {
  CORNER_MIGRATION_SQL,
  cornerMigrationCursor,
  cornerMigrationParameters,
  mapCornerMigrationRow,
} from "./vector-migration";

const row: Record<string, unknown> = {
  source: "jra",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0913",
  keibajo_code: "06",
  race_bango: "01",
  ketto_toroku_bango: "2023100001",
  race_date: "20260913",
  track_code: "11",
  kyori: 1800,
  feature_vector: "[0.5,0.8,0.2,0.1,0.3,0,0.06,0.08]",
  finish_position: 1,
  finish_norm: "0",
  corner1_norm: "0.2",
  corner2_norm: null,
  corner3_norm: "0.3",
  corner4_norm: 0.4,
};

test("maps source features without recomputing their normalization", async () => {
  const vector: VectorizeVector = await mapCornerMigrationRow(row, "corner-20260916");
  expect(vector.id).toMatch(/^[a-f0-9]{64}$/);
  expect(vector.namespace).toBe("corner-20260916");
  expect(vector.values).toStrictEqual([0.5, 0.8, 0.2, 0.1, 0.3, 0, 0.06, 0.08]);
  expect(vector.metadata).toStrictEqual({
    source: "jra",
    raceDate: "20260913",
    venue: "06",
    trackPrefix: "1",
    distance: 1800,
    hasFinish: true,
    finishPosition: 1,
    finishNorm: 0,
    corner1: 0.2,
    corner3: 0.3,
    corner4: 0.4,
  });
});

test("uses stable generation-isolated identifiers", async () => {
  const first: VectorizeVector = await mapCornerMigrationRow(row, "first");
  const retry: VectorizeVector = await mapCornerMigrationRow(row, "first");
  const next: VectorizeVector = await mapCornerMigrationRow(row, "next");
  expect(new Set([first.id, retry.id]).size).toBe(1);
  expect(new Set([first.id, next.id]).size).toBe(2);
});

test("preserves missing finish metadata and empty track for NAR", async () => {
  const vector: VectorizeVector = await mapCornerMigrationRow(
    {
      ...row,
      source: "nar",
      track_code: null,
      kyori: null,
      finish_norm: null,
      finish_position: undefined,
    },
    "corner-v1",
  );
  expect(vector.metadata).toStrictEqual({
    source: "nar",
    raceDate: "20260913",
    venue: "06",
    trackPrefix: "",
    hasFinish: false,
    corner1: 0.2,
    corner3: 0.3,
    corner4: 0.4,
  });
});

test("undefined finish values are not eligible for finish neighbors", async () => {
  const vector: VectorizeVector = await mapCornerMigrationRow(
    { ...row, finish_norm: undefined },
    "corner-v1",
  );
  expect(vector.metadata?.hasFinish).toBe(false);
});

test("keyset cursor follows the complete PostgreSQL primary key order", () => {
  expect(cornerMigrationParameters(cornerMigrationCursor(row))).toStrictEqual([
    "jra",
    "2026",
    "0913",
    "06",
    "01",
    "2023100001",
  ]);
  expect(CORNER_MIGRATION_SQL).toMatch(/> \(\$1, \$2, \$3, \$4, \$5, \$6\)/);
  expect(CORNER_MIGRATION_SQL).toMatch(/limit \$7$/);
});

test.each([
  { source: "unknown" },
  { kaisai_nen: undefined },
  { keibajo_code: " " },
  { feature_vector: "{}" },
  { feature_vector: "[]" },
  { feature_vector: '["x",0,0,0,0,0,0,0]' },
  { kyori: true },
  { finish_norm: "" },
  { corner1_norm: "NaN" },
])("fails closed for un-migratable rows: %j", async (overrides) => {
  await expect(mapCornerMigrationRow({ ...row, ...overrides }, "corner-v1")).rejects.toThrow();
});
