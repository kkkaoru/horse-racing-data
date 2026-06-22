import { expect, test } from "vitest";

import {
  ACTIVE_MODELS_TABLE,
  buildActivateModelSql,
  buildActivatePerClassModelSql,
  buildActiveModelsSubclassUniqueIndexSql,
  buildActiveModelsTableDdl,
  buildAddSubclassColumnSql,
  buildBatchInsertSql,
  buildDropLegacyPkSql,
  buildPredictionsLookupIndexSql,
  buildPredictionsTableDdl,
  INSERT_COLUMNS,
  PREDICTIONS_TABLE,
  PRIMARY_KEY_COLUMNS,
  UPDATABLE_COLUMNS,
} from "./import-predictions-sql";

test("PREDICTIONS_TABLE names the predictions table", () => {
  expect(PREDICTIONS_TABLE).toBe("race_finish_position_model_predictions");
});

test("ACTIVE_MODELS_TABLE names the active model registry", () => {
  expect(ACTIVE_MODELS_TABLE).toBe("finish_position_active_models");
});

test("PRIMARY_KEY_COLUMNS lists the seven identifier columns", () => {
  expect(PRIMARY_KEY_COLUMNS).toStrictEqual([
    "model_version",
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
  ]);
});

test("INSERT_COLUMNS contains 18 fields", () => {
  expect(INSERT_COLUMNS.length).toBe(18);
});

test("INSERT_COLUMNS lists the primary key, prediction and subgroup columns in order", () => {
  expect(INSERT_COLUMNS).toStrictEqual([
    "model_version",
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
    "ketto_toroku_bango",
    "umaban",
    "predicted_score",
    "predicted_rank",
    "predicted_top1_prob",
    "predicted_top3_prob",
    "predicted_finish_position",
    "distance_band",
    "field_size_band",
    "season_band",
    "class_code",
    "surface",
  ]);
});

test("UPDATABLE_COLUMNS lists the mutable prediction and subgroup columns", () => {
  expect(UPDATABLE_COLUMNS).toStrictEqual([
    "umaban",
    "predicted_score",
    "predicted_rank",
    "predicted_top1_prob",
    "predicted_top3_prob",
    "predicted_finish_position",
    "distance_band",
    "field_size_band",
    "season_band",
    "class_code",
    "surface",
  ]);
});

test("UPDATABLE_COLUMNS excludes every primary key column", () => {
  const leakedKeys = PRIMARY_KEY_COLUMNS.filter((key) => UPDATABLE_COLUMNS.includes(key));
  expect(leakedKeys).toStrictEqual([]);
});

test("buildPredictionsTableDdl declares numeric and integer fields", () => {
  const ddl = buildPredictionsTableDdl();
  expect(ddl).toContain("predicted_score numeric not null");
  expect(ddl).toContain("predicted_rank integer not null");
  expect(ddl).toContain("prediction_generated_at timestamptz not null default now()");
  expect(ddl).toContain("primary key (model_version, source, kaisai_nen");
});

test("buildPredictionsTableDdl declares the five nullable subgroup columns", () => {
  const ddl = buildPredictionsTableDdl();
  expect(ddl).toContain("distance_band text,");
  expect(ddl).toContain("field_size_band text,");
  expect(ddl).toContain("season_band text,");
  expect(ddl).toContain("class_code text,");
  expect(ddl).toContain("surface text,");
});

test("buildActiveModelsTableDdl exposes category, subclass and model_version", () => {
  const ddl = buildActiveModelsTableDdl();
  expect(ddl).toContain("category text not null");
  expect(ddl).toContain("subclass text");
  expect(ddl).toContain("model_version text not null");
});

test("buildAddSubclassColumnSql adds the subclass column idempotently", () => {
  const sql = buildAddSubclassColumnSql();
  expect(sql).toContain("alter table finish_position_active_models");
  expect(sql).toContain("add column if not exists subclass text");
});

test("buildDropLegacyPkSql drops the legacy category-only primary key", () => {
  const sql = buildDropLegacyPkSql();
  expect(sql).toContain("alter table finish_position_active_models");
  expect(sql).toContain("drop constraint if exists finish_position_active_models_pkey");
});

test("buildActiveModelsSubclassUniqueIndexSql enforces uniqueness over (category, coalesce(subclass, ''))", () => {
  const sql = buildActiveModelsSubclassUniqueIndexSql();
  expect(sql).toContain(
    "create unique index if not exists finish_position_active_models_category_subclass_idx",
  );
  expect(sql).toContain("on finish_position_active_models (category, coalesce(subclass, ''))");
});

test("buildPredictionsLookupIndexSql covers the race tuple", () => {
  expect(buildPredictionsLookupIndexSql()).toContain(
    "(source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)",
  );
});

test("buildBatchInsertSql produces one placeholder block per row", () => {
  const sql = buildBatchInsertSql(2);
  expect(sql).toContain(
    "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18",
  );
  expect(sql).toContain(
    "$19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36",
  );
});

test("buildBatchInsertSql includes the conflict clause", () => {
  const sql = buildBatchInsertSql(1);
  expect(sql).toContain(
    "on conflict (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)",
  );
  expect(sql).toContain("predicted_score = excluded.predicted_score");
  expect(sql).toContain("prediction_generated_at = now()");
});

test("buildBatchInsertSql updates the subgroup columns on conflict", () => {
  const sql = buildBatchInsertSql(1);
  expect(sql).toContain("distance_band = excluded.distance_band");
  expect(sql).toContain("field_size_band = excluded.field_size_band");
  expect(sql).toContain("season_band = excluded.season_band");
  expect(sql).toContain("class_code = excluded.class_code");
  expect(sql).toContain("surface = excluded.surface");
});

test("buildActivateModelSql upserts the category fallback (NULL subclass) row", () => {
  const sql = buildActivateModelSql();
  expect(sql).toContain("insert into finish_position_active_models");
  expect(sql).toContain("(category, subclass, model_version)");
  expect(sql).toContain("values ($1, null, $2)");
  expect(sql).toContain("on conflict (category, coalesce(subclass, ''))");
  expect(sql).toContain("model_version = excluded.model_version");
});

test("buildActivatePerClassModelSql upserts a per-class (category, subclass) row", () => {
  const sql = buildActivatePerClassModelSql();
  expect(sql).toContain("insert into finish_position_active_models");
  expect(sql).toContain("(category, subclass, model_version)");
  expect(sql).toContain("values ($1, $2, $3)");
  expect(sql).toContain("on conflict (category, coalesce(subclass, ''))");
  expect(sql).toContain("model_version = excluded.model_version");
});
