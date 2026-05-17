// Run with: bun run test src/scripts/finish-position-features/import-corner-position-sql.test.ts

import { describe, expect, test } from "vitest";

import {
  ACTIVE_MODELS_TABLE,
  buildActivateModelSql,
  buildActiveModelsTableDdl,
  buildBatchInsertSql,
  buildEvaluationsTableDdl,
  buildEvaluationUpsertSql,
  buildPredictionsLookupIndexSql,
  buildPredictionsTableDdl,
  EVALUATION_METRIC_COLUMNS,
  EVALUATIONS_TABLE,
  INSERT_COLUMNS,
  PREDICTION_COLUMNS,
  PREDICTIONS_TABLE,
  PRIMARY_KEY_COLUMNS,
} from "./import-corner-position-sql";

describe("import-corner-position-sql constants", () => {
  test("predictions table is named race_corner_position_model_predictions", () => {
    expect(PREDICTIONS_TABLE).toBe("race_corner_position_model_predictions");
  });

  test("active models table is named corner_position_active_models", () => {
    expect(ACTIVE_MODELS_TABLE).toBe("corner_position_active_models");
  });

  test("evaluations table is named corner_position_model_evaluations", () => {
    expect(EVALUATIONS_TABLE).toBe("corner_position_model_evaluations");
  });

  test("primary key covers seven columns including model_version and canonical 6 keys", () => {
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

  test("prediction columns cover three corners", () => {
    expect(PREDICTION_COLUMNS).toStrictEqual(["corner_1_pred", "corner_3_pred", "corner_4_pred"]);
  });

  test("insert columns include primary keys plus umaban and prediction columns", () => {
    expect(INSERT_COLUMNS).toStrictEqual([
      "model_version",
      "source",
      "kaisai_nen",
      "kaisai_tsukihi",
      "keibajo_code",
      "race_bango",
      "ketto_toroku_bango",
      "umaban",
      "corner_1_pred",
      "corner_3_pred",
      "corner_4_pred",
    ]);
  });

  test("evaluation metric columns cover three per-corner mae plus mean and agreement", () => {
    expect(EVALUATION_METRIC_COLUMNS).toStrictEqual([
      "corner_1_mae",
      "corner_3_mae",
      "corner_4_mae",
      "mean_mae",
      "corner_1_top3_agreement",
    ]);
  });
});

describe("buildPredictionsTableDdl", () => {
  test("declares all required columns with idempotent create", () => {
    const ddl = buildPredictionsTableDdl();
    expect(ddl).toContain("create table if not exists race_corner_position_model_predictions");
    expect(ddl).toContain("corner_1_pred numeric");
    expect(ddl).toContain("corner_3_pred numeric");
    expect(ddl).toContain("corner_4_pred numeric");
    expect(ddl).toContain("prediction_generated_at timestamptz not null default now()");
    expect(ddl).toContain(
      "primary key (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)",
    );
  });
});

describe("buildActiveModelsTableDdl", () => {
  test("uses category as primary key", () => {
    const ddl = buildActiveModelsTableDdl();
    expect(ddl).toContain("create table if not exists corner_position_active_models");
    expect(ddl).toContain("category text primary key");
    expect(ddl).toContain("model_version text not null");
    expect(ddl).toContain("activated_at timestamptz not null default now()");
  });
});

describe("buildEvaluationsTableDdl", () => {
  test("declares per-corner MAE columns and primary key on window", () => {
    const ddl = buildEvaluationsTableDdl();
    expect(ddl).toContain("create table if not exists corner_position_model_evaluations");
    expect(ddl).toContain("corner_1_mae numeric");
    expect(ddl).toContain("corner_3_mae numeric");
    expect(ddl).toContain("corner_4_mae numeric");
    expect(ddl).toContain("mean_mae numeric");
    expect(ddl).toContain("corner_1_top3_agreement numeric");
    expect(ddl).toContain(
      "primary key (model_version, category, evaluation_window_from, evaluation_window_to)",
    );
  });
});

describe("buildPredictionsLookupIndexSql", () => {
  test("creates idempotent index on canonical race lookup columns", () => {
    const sql = buildPredictionsLookupIndexSql();
    expect(sql).toContain("create index if not exists race_corner_position_model_predictions_race_lookup_idx");
    expect(sql).toContain("source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango");
  });
});

describe("buildBatchInsertSql", () => {
  test("emits placeholders for a single row", () => {
    const sql = buildBatchInsertSql(1);
    expect(sql).toContain("insert into race_corner_position_model_predictions");
    expect(sql).toContain("($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)");
    expect(sql).toContain("on conflict (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)");
  });

  test("multi-row batch uses sequential placeholders", () => {
    const sql = buildBatchInsertSql(2);
    expect(sql).toContain("($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)");
    expect(sql).toContain("($12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)");
  });

  test("update clause sets corner predictions on conflict", () => {
    const sql = buildBatchInsertSql(1);
    expect(sql).toContain("corner_1_pred = excluded.corner_1_pred");
    expect(sql).toContain("corner_3_pred = excluded.corner_3_pred");
    expect(sql).toContain("corner_4_pred = excluded.corner_4_pred");
    expect(sql).toContain("prediction_generated_at = now()");
  });
});

describe("buildActivateModelSql", () => {
  test("upserts the active model for a given category", () => {
    const sql = buildActivateModelSql();
    expect(sql).toContain("insert into corner_position_active_models (category, model_version)");
    expect(sql).toContain("values ($1, $2)");
    expect(sql).toContain("on conflict (category)");
    expect(sql).toContain("model_version = excluded.model_version");
  });
});

describe("buildEvaluationUpsertSql", () => {
  test("upserts a row into the evaluation table with all metric columns", () => {
    const sql = buildEvaluationUpsertSql();
    expect(sql).toContain("insert into corner_position_model_evaluations");
    expect(sql).toContain("model_version, category, evaluation_window_from, evaluation_window_to");
    expect(sql).toContain("on conflict (model_version, category, evaluation_window_from, evaluation_window_to)");
    expect(sql).toContain("corner_1_mae = excluded.corner_1_mae");
    expect(sql).toContain("mean_mae = excluded.mean_mae");
    expect(sql).toContain("corner_1_top3_agreement = excluded.corner_1_top3_agreement");
    expect(sql).toContain("evaluated_at = now()");
  });
});
