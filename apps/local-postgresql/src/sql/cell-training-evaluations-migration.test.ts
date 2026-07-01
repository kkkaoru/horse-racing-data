import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const migrationPath = resolve(
  import.meta.dirname,
  "../../sql/20260701000000_add_prediction_target_to_cell_training_evaluations.sql",
);
const subgroupMigrationPath = resolve(
  import.meta.dirname,
  "../../sql/20260702000000_add_subgroup_to_cell_training_evaluations.sql",
);

it("adds prediction_target and backfills existing finish-position rows", () => {
  const sql = readFileSync(migrationPath, "utf8");
  const normalized = sql.replaceAll(/\s+/g, " ").trim().toLowerCase();

  expect(
    normalized.match(
      /alter table cell_training_evaluations add column if not exists prediction_target text/,
    )?.[0],
  ).toStrictEqual(
    "alter table cell_training_evaluations add column if not exists prediction_target text",
  );
  expect(
    normalized.match(/alter column prediction_target set default 'finish_position'/)?.[0],
  ).toStrictEqual("alter column prediction_target set default 'finish_position'");
  expect(
    normalized.match(
      /set prediction_target = 'finish_position' where prediction_target is null/,
    )?.[0],
  ).toStrictEqual("set prediction_target = 'finish_position' where prediction_target is null");
  expect(normalized.match(/alter column prediction_target set not null/)?.[0]).toStrictEqual(
    "alter column prediction_target set not null",
  );
});

it("rebuilds the legacy primary key with prediction_target first", () => {
  const sql = readFileSync(migrationPath, "utf8");
  const normalized = sql.replaceAll(/\s+/g, " ").trim().toLowerCase();

  expect(
    normalized.match(
      /if pk_cols = array\[ 'feature_set_hash', 'category', 'surface', 'distance_band', 'class_label', 'season', 'venue' \] or pk_cols = array\[ 'prediction_target', 'feature_set_hash', 'category', 'surface', 'distance_band', 'class_label', 'season', 'venue' \] then/,
    )?.[0],
  ).toStrictEqual(
    "if pk_cols = array[ 'feature_set_hash', 'category', 'surface', 'distance_band', 'class_label', 'season', 'venue' ] or pk_cols = array[ 'prediction_target', 'feature_set_hash', 'category', 'surface', 'distance_band', 'class_label', 'season', 'venue' ] then",
  );
  expect(normalized.match(/drop constraint cell_training_evaluations_pkey/)?.[0]).toStrictEqual(
    "drop constraint cell_training_evaluations_pkey",
  );
  expect(
    normalized.match(
      /add primary key \( prediction_target, feature_set_hash, category, surface, distance_band, class_label, season, venue, subgroup \)/,
    )?.[0],
  ).toStrictEqual(
    "add primary key ( prediction_target, feature_set_hash, category, surface, distance_band, class_label, season, venue, subgroup )",
  );
});

it("adds subgroup for full cell-key persistence", () => {
  const sql = readFileSync(migrationPath, "utf8");
  const normalized = sql.replaceAll(/\s+/g, " ").trim().toLowerCase();

  expect(
    normalized.match(
      /alter table cell_training_evaluations add column if not exists subgroup text not null default ''/,
    )?.[0],
  ).toStrictEqual(
    "alter table cell_training_evaluations add column if not exists subgroup text not null default ''",
  );
});

it("creates target-aware indexes with new names", () => {
  const sql = readFileSync(migrationPath, "utf8");
  const normalized = sql.replaceAll(/\s+/g, " ").trim().toLowerCase();

  expect(
    normalized.match(
      /create index if not exists cell_training_evaluations_target_category_season_idx on cell_training_evaluations \(prediction_target, category, season\)/,
    )?.[0],
  ).toStrictEqual(
    "create index if not exists cell_training_evaluations_target_category_season_idx on cell_training_evaluations (prediction_target, category, season)",
  );
  expect(
    normalized.match(
      /create index if not exists cell_training_evaluations_target_category_venue_idx on cell_training_evaluations \(prediction_target, category, venue\)/,
    )?.[0],
  ).toStrictEqual(
    "create index if not exists cell_training_evaluations_target_category_venue_idx on cell_training_evaluations (prediction_target, category, venue)",
  );
  expect(
    normalized.match(
      /create index if not exists cell_training_evaluations_target_feature_hash_idx on cell_training_evaluations \(prediction_target, feature_set_hash\)/,
    )?.[0],
  ).toStrictEqual(
    "create index if not exists cell_training_evaluations_target_feature_hash_idx on cell_training_evaluations (prediction_target, feature_set_hash)",
  );
  expect(
    normalized.match(
      /create index if not exists cell_training_evaluations_target_category_season_venue_idx on cell_training_evaluations \(prediction_target, category, season, venue\)/,
    )?.[0],
  ).toStrictEqual(
    "create index if not exists cell_training_evaluations_target_category_season_venue_idx on cell_training_evaluations (prediction_target, category, season, venue)",
  );
  expect(
    normalized.match(
      /create index if not exists cell_training_evaluations_target_category_top1_idx on cell_training_evaluations \(prediction_target, category, top1_accuracy desc\)/,
    )?.[0],
  ).toStrictEqual(
    "create index if not exists cell_training_evaluations_target_category_top1_idx on cell_training_evaluations (prediction_target, category, top1_accuracy desc)",
  );
});

it("has a follow-up migration for already migrated databases", () => {
  const sql = readFileSync(subgroupMigrationPath, "utf8");
  const normalized = sql.replaceAll(/\s+/g, " ").trim().toLowerCase();

  expect(
    normalized.match(
      /alter table cell_training_evaluations add column if not exists subgroup text not null default ''/,
    )?.[0],
  ).toStrictEqual(
    "alter table cell_training_evaluations add column if not exists subgroup text not null default ''",
  );
  expect(
    normalized.match(
      /add primary key \( prediction_target, feature_set_hash, category, surface, distance_band, class_label, season, venue, subgroup \)/,
    )?.[0],
  ).toStrictEqual(
    "add primary key ( prediction_target, feature_set_hash, category, surface, distance_band, class_label, season, venue, subgroup )",
  );
});
