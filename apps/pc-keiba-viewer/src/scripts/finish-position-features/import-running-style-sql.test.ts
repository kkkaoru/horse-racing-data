// Run with: bun run test src/scripts/finish-position-features/import-running-style-sql.test.ts

import { describe, expect, test } from "vitest";

import {
  ACTIVE_MODELS_TABLE,
  buildActivateModelSql,
  buildActiveModelsTableDdl,
  buildBatchInsertSql,
  buildEvaluationsTableDdl,
  buildEvaluationUpsertSql,
  buildHorseLookupIndexSql,
  buildPredictionsLookupIndexSql,
  buildPredictionsTableDdl,
  EVALUATION_METRIC_COLUMNS,
  EVALUATIONS_TABLE,
  INSERT_COLUMNS,
  LABEL_COLUMNS,
  PREDICTIONS_TABLE,
  PROBABILITY_COLUMNS,
} from "./import-running-style-sql";

describe("import-running-style-sql constants", () => {
  test("predictions table is race_running_style_model_predictions", () => {
    expect(PREDICTIONS_TABLE).toBe("race_running_style_model_predictions");
  });

  test("active models table is running_style_active_models", () => {
    expect(ACTIVE_MODELS_TABLE).toBe("running_style_active_models");
  });

  test("evaluations table is running_style_model_evaluations", () => {
    expect(EVALUATIONS_TABLE).toBe("running_style_model_evaluations");
  });

  test("probability columns are listed in nige/senkou/sashi/oikomi order", () => {
    expect(PROBABILITY_COLUMNS).toStrictEqual(["p_nige", "p_senkou", "p_sashi", "p_oikomi"]);
  });

  test("label columns are predicted_label and predicted_class", () => {
    expect(LABEL_COLUMNS).toStrictEqual(["predicted_label", "predicted_class"]);
  });

  test("insert columns combine primary keys plus probabilities and labels", () => {
    expect(INSERT_COLUMNS).toStrictEqual([
      "model_version",
      "source",
      "kaisai_nen",
      "kaisai_tsukihi",
      "keibajo_code",
      "race_bango",
      "ketto_toroku_bango",
      "umaban",
      "p_nige",
      "p_senkou",
      "p_sashi",
      "p_oikomi",
      "predicted_label",
      "predicted_class",
    ]);
  });

  test("evaluation metric columns cover accuracy, macro_f1, per-class precision/recall/support and kyakushitsuhantei", () => {
    expect(EVALUATION_METRIC_COLUMNS).toStrictEqual([
      "accuracy",
      "macro_f1",
      "precision_nige",
      "precision_senkou",
      "precision_sashi",
      "precision_oikomi",
      "recall_nige",
      "recall_senkou",
      "recall_sashi",
      "recall_oikomi",
      "support_nige",
      "support_senkou",
      "support_sashi",
      "support_oikomi",
      "kyakushitsuhantei_agreement",
    ]);
  });
});

describe("buildPredictionsTableDdl", () => {
  test("declares probability + label columns and primary key", () => {
    const ddl = buildPredictionsTableDdl();
    expect(ddl).toContain("create table if not exists race_running_style_model_predictions");
    expect(ddl).toContain("p_nige numeric not null");
    expect(ddl).toContain("p_senkou numeric not null");
    expect(ddl).toContain("p_sashi numeric not null");
    expect(ddl).toContain("p_oikomi numeric not null");
    expect(ddl).toContain("predicted_label text not null");
    expect(ddl).toContain("predicted_class integer not null");
    expect(ddl).toContain(
      "primary key (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)",
    );
  });
});

describe("buildActiveModelsTableDdl", () => {
  test("uses category as primary key", () => {
    const ddl = buildActiveModelsTableDdl();
    expect(ddl).toContain("create table if not exists running_style_active_models");
    expect(ddl).toContain("category text primary key");
  });
});

describe("buildEvaluationsTableDdl", () => {
  test("declares accuracy, macro_f1 and per-class precision/recall/support", () => {
    const ddl = buildEvaluationsTableDdl();
    expect(ddl).toContain("create table if not exists running_style_model_evaluations");
    expect(ddl).toContain("accuracy numeric");
    expect(ddl).toContain("macro_f1 numeric");
    expect(ddl).toContain("precision_nige numeric");
    expect(ddl).toContain("recall_oikomi numeric");
    expect(ddl).toContain("support_nige integer");
    expect(ddl).toContain("kyakushitsuhantei_agreement numeric");
  });
});

describe("buildPredictionsLookupIndexSql", () => {
  test("creates idempotent race lookup index", () => {
    const sql = buildPredictionsLookupIndexSql();
    expect(sql).toContain("create index if not exists race_running_style_model_predictions_race_lookup_idx");
    expect(sql).toContain("source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango");
  });
});

describe("buildHorseLookupIndexSql", () => {
  test("creates idempotent horse lookup index ordered by prediction time desc", () => {
    const sql = buildHorseLookupIndexSql();
    expect(sql).toContain("race_running_style_model_predictions_horse_lookup_idx");
    expect(sql).toContain("ketto_toroku_bango, prediction_generated_at desc");
  });
});

describe("buildBatchInsertSql", () => {
  test("emits sequential placeholders for a single row of 14 columns", () => {
    const sql = buildBatchInsertSql(1);
    expect(sql).toContain("insert into race_running_style_model_predictions");
    expect(sql).toContain("($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)");
  });

  test("multi-row batch keeps placeholder numbering correct", () => {
    const sql = buildBatchInsertSql(2);
    expect(sql).toContain("($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)");
    expect(sql).toContain(
      "($15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)",
    );
  });

  test("update clause overwrites probabilities and labels on conflict", () => {
    const sql = buildBatchInsertSql(1);
    expect(sql).toContain("p_nige = excluded.p_nige");
    expect(sql).toContain("predicted_label = excluded.predicted_label");
    expect(sql).toContain("predicted_class = excluded.predicted_class");
  });
});

describe("buildActivateModelSql", () => {
  test("upserts the active model for a given category", () => {
    const sql = buildActivateModelSql();
    expect(sql).toContain("insert into running_style_active_models (category, model_version)");
    expect(sql).toContain("on conflict (category)");
  });
});

describe("buildEvaluationUpsertSql", () => {
  test("upserts a row covering accuracy, macro_f1, per-class metrics and kyakushitsuhantei", () => {
    const sql = buildEvaluationUpsertSql();
    expect(sql).toContain("insert into running_style_model_evaluations");
    expect(sql).toContain("on conflict (model_version, category, evaluation_window_from, evaluation_window_to)");
    expect(sql).toContain("accuracy = excluded.accuracy");
    expect(sql).toContain("macro_f1 = excluded.macro_f1");
    expect(sql).toContain("kyakushitsuhantei_agreement = excluded.kyakushitsuhantei_agreement");
    expect(sql).toContain("evaluated_at = now()");
  });
});
