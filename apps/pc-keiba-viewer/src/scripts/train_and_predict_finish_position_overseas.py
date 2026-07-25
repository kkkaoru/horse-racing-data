#!/usr/bin/env python3
# pyright: reportUnknownParameterType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportMissingParameterType=false, reportArgumentType=false, reportAttributeAccessIssue=false, reportIndexIssue=false, reportCallIssue=false, reportOperatorIssue=false
"""Train an overseas finish-position LightGBM model and predict tonight's race.

Standalone script — no project-internal imports.

Usage:
  cd apps/pc-keiba-viewer
  set -a; . ../local-postgresql/.env; set +a
  uv run python src/scripts/train_and_predict_finish_position_overseas.py
"""
from __future__ import annotations

import argparse
import os
import pickle
import sys
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import psycopg
from sklearn.metrics import mean_absolute_error, roc_auc_score
from sklearn.model_selection import train_test_split

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
APP_DIR = SCRIPT_DIR.parent.parent  # apps/pc-keiba-viewer
REPO_ROOT = APP_DIR.parent.parent  # repo root
MODEL_DIR = APP_DIR / "tmp" / "models" / "overseas-fp-v1"

FEATURE_COLUMNS = [
    "weight_kg",
    "age",
    "sex",
    "has_jockey",
    "has_trainer",
    "has_horse_reg",
    "distance",
    "track_code",
    "grade_code",
    "field_size",
    "weight_per_field",
]

GRADE_MAP = {"A": 1, "B": 2, "C": 3}

TRAINING_SQL = """\
SELECT s.umaban,
       trim(s.kakutei_chakujun) AS finish_position,
       trim(s.futan_juryo)      AS weight_raw,
       trim(s.barei)            AS age_raw,
       s.seibetsu_code          AS sex,
       s.kishu_code,
       s.chokyoshi_code,
       s.ketto_toroku_bango,
       r.kyori,
       r.track_code,
       trim(r.grade_code)       AS grade,
       trim(r.shusso_tosu)      AS field_size_raw
FROM jvd_se s
JOIN jvd_ra r
  ON s.kaisai_nen     = r.kaisai_nen
 AND s.kaisai_tsukihi = r.kaisai_tsukihi
 AND s.keibajo_code   = r.keibajo_code
 AND s.race_bango     = r.race_bango
WHERE s.data_kubun = 'B'
  AND trim(s.kakutei_chakujun) NOT IN ('', '00')
  AND trim(s.futan_juryo) NOT IN ('', '000')
"""

PREDICT_SQL_TEMPLATE = """\
SELECT s.umaban,
       trim(s.kakutei_chakujun) AS finish_position,
       trim(s.futan_juryo)      AS weight_raw,
       trim(s.barei)            AS age_raw,
       s.seibetsu_code          AS sex,
       s.kishu_code,
       s.chokyoshi_code,
       s.ketto_toroku_bango,
       r.kyori,
       r.track_code,
       trim(r.grade_code)       AS grade,
       trim(r.shusso_tosu)      AS field_size_raw
FROM jvd_se s
JOIN jvd_ra r
  ON s.kaisai_nen     = r.kaisai_nen
 AND s.kaisai_tsukihi = r.kaisai_tsukihi
 AND s.keibajo_code   = r.keibajo_code
 AND s.race_bango     = r.race_bango
WHERE s.data_kubun = 'B'
  AND s.kaisai_nen     = %(kaisai_nen)s
  AND s.kaisai_tsukihi = %(kaisai_tsukihi)s
  AND s.keibajo_code   = %(keibajo_code)s
  AND s.race_bango     = %(race_bango)s
"""

CREATE_PREDICTIONS_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS race_finish_position_model_predictions (
    model_version              text        NOT NULL,
    source                     text        NOT NULL,
    kaisai_nen                 text        NOT NULL,
    kaisai_tsukihi             text        NOT NULL,
    keibajo_code               text        NOT NULL,
    race_bango                 text        NOT NULL,
    ketto_toroku_bango         text        NOT NULL,
    umaban                     integer     NOT NULL,
    predicted_score            numeric,
    predicted_rank             integer,
    predicted_top1_prob        numeric,
    predicted_top3_prob        numeric,
    predicted_finish_position  numeric,
    prediction_generated_at    timestamp with time zone,
    distance_band              text,
    field_size_band            text,
    season_band                text,
    class_code                 text,
    surface                    text,
    PRIMARY KEY (model_version, source, kaisai_nen, kaisai_tsukihi,
                 keibajo_code, race_bango, ketto_toroku_bango)
);
"""

CREATE_ACTIVE_MODELS_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS finish_position_active_models (
    category      text PRIMARY KEY,
    model_version text NOT NULL,
    activated_at  timestamp with time zone NOT NULL
);
"""

UPSERT_PREDICTION_SQL = """\
INSERT INTO race_finish_position_model_predictions (
    model_version, source, kaisai_nen, kaisai_tsukihi,
    keibajo_code, race_bango, ketto_toroku_bango, umaban,
    predicted_score, predicted_rank, predicted_top1_prob,
    predicted_top3_prob, predicted_finish_position,
    prediction_generated_at, distance_band, field_size_band,
    season_band, class_code, surface
) VALUES (
    %(model_version)s, %(source)s, %(kaisai_nen)s, %(kaisai_tsukihi)s,
    %(keibajo_code)s, %(race_bango)s, %(ketto_toroku_bango)s, %(umaban)s,
    %(predicted_score)s, %(predicted_rank)s, %(predicted_top1_prob)s,
    %(predicted_top3_prob)s, %(predicted_finish_position)s,
    %(prediction_generated_at)s, %(distance_band)s, %(field_size_band)s,
    %(season_band)s, %(class_code)s, %(surface)s
)
ON CONFLICT (model_version, source, kaisai_nen, kaisai_tsukihi,
             keibajo_code, race_bango, ketto_toroku_bango)
DO UPDATE SET
    umaban                    = EXCLUDED.umaban,
    predicted_score           = EXCLUDED.predicted_score,
    predicted_rank            = EXCLUDED.predicted_rank,
    predicted_top1_prob       = EXCLUDED.predicted_top1_prob,
    predicted_top3_prob       = EXCLUDED.predicted_top3_prob,
    predicted_finish_position = EXCLUDED.predicted_finish_position,
    prediction_generated_at   = EXCLUDED.prediction_generated_at,
    distance_band             = EXCLUDED.distance_band,
    field_size_band           = EXCLUDED.field_size_band,
    season_band               = EXCLUDED.season_band,
    class_code                = EXCLUDED.class_code,
    surface                   = EXCLUDED.surface
"""

UPSERT_ACTIVE_MODEL_SQL = """\
INSERT INTO finish_position_active_models (category, model_version, activated_at)
VALUES (%(category)s, %(model_version)s, %(activated_at)s)
ON CONFLICT (category)
DO UPDATE SET model_version = EXCLUDED.model_version,
              activated_at  = EXCLUDED.activated_at
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[overseas-fp] {msg}", flush=True)


def read_env_file(path: Path) -> dict[str, str]:
    """Parse a simple KEY=VALUE .env file (no shell expansion)."""
    env: dict[str, str] = {}
    if not path.is_file():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        env[key] = value
    return env


def default_pg_url() -> str:
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    db = os.environ.get("POSTGRES_DB", "horse_racing")
    return f"postgresql://{user}:{password}@127.0.0.1:15432/{db}"


def default_neon_url() -> str | None:
    # Try repo root .env
    env = read_env_file(REPO_ROOT / ".env")
    url = env.get("NEON_PRIMARY_URL")
    if url:
        return url
    # Fallback: environment variable
    return os.environ.get("NEON_PRIMARY_URL")


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create model features from raw columns."""
    out = pd.DataFrame()
    out["weight_kg"] = df["weight_raw"].astype(float) / 10.0
    out["age"] = df["age_raw"].astype(int)
    out["sex"] = df["sex"].astype(int)
    out["has_jockey"] = (df["kishu_code"].astype(str).str.strip() != "00000").astype(int)
    out["has_trainer"] = (df["chokyoshi_code"].astype(str).str.strip() != "00000").astype(int)
    out["has_horse_reg"] = (df["ketto_toroku_bango"].astype(str).str.strip() != "0000000000").astype(int)
    out["distance"] = df["kyori"].astype(int)
    out["track_code"] = df["track_code"].astype(int)
    out["grade_code"] = df["grade"].map(lambda g: GRADE_MAP.get(str(g).strip(), 4)).astype(int)
    out["field_size"] = df["field_size_raw"].astype(int)
    out["weight_per_field"] = out["weight_kg"] / out["field_size"]
    return out


def save_model(model: object, name: str) -> Path:
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    path = MODEL_DIR / f"{name}.pkl"
    with open(path, "wb") as f:
        pickle.dump(model, f)
    log(f"Saved model: {path}")
    return path


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
def step1_extract_training_data(pg_url: str) -> pd.DataFrame:
    log("STEP 1: Extracting training data from local PG ...")
    df = pd.read_sql(TRAINING_SQL, pg_url)
    log(f"  Extracted {len(df)} overseas runner rows with finish positions")
    return df


def step2_engineer_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    log("STEP 2: Engineering features ...")
    features = engineer_features(df)
    target = df["finish_position"].astype(int)
    log(f"  Features shape: {features.shape}")
    log(f"  Target range: {target.min()} - {target.max()}, mean={target.mean():.2f}")
    return features, target


def step3_train_models(
    features: pd.DataFrame, target: pd.Series
) -> tuple[lgb.LGBMRegressor, lgb.LGBMClassifier, lgb.LGBMClassifier]:
    log("STEP 3: Training LightGBM models ...")

    x_train, x_test, y_train, y_test = train_test_split(
        features, target, test_size=0.2, random_state=42
    )
    log(f"  Train: {len(x_train)} rows, Test: {len(x_test)} rows")

    # --- Regressor: predict continuous finish position ---
    regressor = lgb.LGBMRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=-1,
    )
    regressor.fit(x_train, y_train)
    reg_pred_train = regressor.predict(x_train)
    reg_pred_test = regressor.predict(x_test)
    mae_train = mean_absolute_error(y_train, reg_pred_train)
    mae_test = mean_absolute_error(y_test, reg_pred_test)
    log(f"  Regressor MAE  — train: {mae_train:.4f}, test: {mae_test:.4f}")

    # --- Classifier: top-1 (win) ---
    y_top1_train = (y_train == 1).astype(int)
    y_top1_test = (y_test == 1).astype(int)
    clf_top1 = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=-1,
    )
    clf_top1.fit(x_train, y_top1_train)
    top1_prob_test = clf_top1.predict_proba(x_test)
    # Handle case where only one class might be present
    if top1_prob_test.shape[1] == 2:
        auc_top1 = roc_auc_score(y_top1_test, top1_prob_test[:, 1])
    else:
        auc_top1 = float("nan")
    log(f"  Top-1 Classifier AUC — test: {auc_top1:.4f}")

    # --- Classifier: top-3 (place) ---
    y_top3_train = (y_train <= 3).astype(int)
    y_top3_test = (y_test <= 3).astype(int)
    clf_top3 = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=-1,
    )
    clf_top3.fit(x_train, y_top3_train)
    top3_prob_test = clf_top3.predict_proba(x_test)
    if top3_prob_test.shape[1] == 2:
        auc_top3 = roc_auc_score(y_top3_test, top3_prob_test[:, 1])
    else:
        auc_top3 = float("nan")
    log(f"  Top-3 Classifier AUC — test: {auc_top3:.4f}")

    # Save models
    save_model(regressor, "regressor")
    save_model(clf_top1, "classifier_top1")
    save_model(clf_top3, "classifier_top3")

    return regressor, clf_top1, clf_top3


def step4_predict_race(
    pg_url: str,
    regressor: lgb.LGBMRegressor,
    clf_top1: lgb.LGBMClassifier,
    clf_top3: lgb.LGBMClassifier,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> pd.DataFrame:
    log("STEP 4: Predicting tonight's race ...")
    conn = psycopg.connect(pg_url)
    try:
        cur = conn.cursor()
        cur.execute(
            PREDICT_SQL_TEMPLATE,
            {
                "kaisai_nen": kaisai_nen,
                "kaisai_tsukihi": kaisai_tsukihi,
                "keibajo_code": keibajo_code,
                "race_bango": race_bango,
            },
        )
        desc = cur.description
        columns = [d[0] for d in desc] if desc else []
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()

    if df.empty:
        log("  ERROR: No runners found for the target race!")
        sys.exit(1)

    log(f"  Found {len(df)} runners")

    features = engineer_features(df)

    # Regressor: predicted finish position (lower = better)
    pred_finish = regressor.predict(features)
    # Score: negate so higher = better
    pred_score = -pred_finish

    # Rank by score (descending → rank 1 = best)
    ranks = pd.Series(pred_score).rank(ascending=False, method="first").astype(int)

    # Top-1 probability
    top1_proba = clf_top1.predict_proba(features)
    if top1_proba.shape[1] == 2:
        top1_prob = top1_proba[:, 1]
    else:
        top1_prob = np.zeros(len(features))

    # Top-3 probability
    top3_proba = clf_top3.predict_proba(features)
    if top3_proba.shape[1] == 2:
        top3_prob = top3_proba[:, 1]
    else:
        top3_prob = np.zeros(len(features))

    result = pd.DataFrame(
        {
            "umaban": df["umaban"].astype(int),
            "ketto_toroku_bango": df["ketto_toroku_bango"].astype(str),
            "predicted_finish_position": pred_finish,
            "predicted_score": pred_score,
            "predicted_rank": ranks,
            "predicted_top1_prob": top1_prob,
            "predicted_top3_prob": top3_prob,
            "grade": df["grade"].astype(str).str.strip(),
            "track_code": df["track_code"].astype(str),
        }
    )
    # PK does not include umaban; give unknown horses a unique synthetic key
    unknown_mask = result["ketto_toroku_bango"] == "0000000000"
    result.loc[unknown_mask, "ketto_toroku_bango"] = (
        "UMABAN_" + result.loc[unknown_mask, "umaban"].astype(str).str.zfill(2)
    )
    result = result.sort_values("predicted_rank").reset_index(drop=True)

    log("  Predictions:")
    log(f"  {'umaban':>6} {'rank':>5} {'finish_pos':>10} {'top1_prob':>10} {'top3_prob':>10}")
    for _, row in result.iterrows():
        log(
            f"  {row['umaban']:>6} {row['predicted_rank']:>5} "
            f"{row['predicted_finish_position']:>10.2f} "
            f"{row['predicted_top1_prob']:>10.4f} "
            f"{row['predicted_top3_prob']:>10.4f}"
        )

    return result


def step5_write_local_pg(
    pg_url: str,
    predictions: pd.DataFrame,
    model_version: str,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> None:
    log("STEP 5: Writing predictions to local PG ...")
    now = datetime.now(timezone.utc)

    conn = psycopg.connect(pg_url)
    try:
        with conn.cursor() as cur:
            for _, row in predictions.iterrows():
                cur.execute(
                    UPSERT_PREDICTION_SQL,
                    {
                        "model_version": model_version,
                        "source": "overseas",
                        "kaisai_nen": kaisai_nen,
                        "kaisai_tsukihi": kaisai_tsukihi,
                        "keibajo_code": keibajo_code,
                        "race_bango": race_bango,
                        "ketto_toroku_bango": row["ketto_toroku_bango"],
                        "umaban": int(row["umaban"]),
                        "predicted_score": float(row["predicted_score"]),
                        "predicted_rank": int(row["predicted_rank"]),
                        "predicted_top1_prob": float(row["predicted_top1_prob"]),
                        "predicted_top3_prob": float(row["predicted_top3_prob"]),
                        "predicted_finish_position": float(row["predicted_finish_position"]),
                        "prediction_generated_at": now,
                        "distance_band": "2000-2400",
                        "field_size_band": "09",
                        "season_band": "summer",
                        "class_code": row["grade"],
                        "surface": row["track_code"],
                    },
                )

            cur.execute(
                UPSERT_ACTIVE_MODEL_SQL,
                {
                    "category": "overseas",
                    "model_version": model_version,
                    "activated_at": now,
                },
            )
        conn.commit()
        log(f"  Upserted {len(predictions)} prediction rows + 1 active model row")
    finally:
        conn.close()


def step6_sync_neon(
    neon_url: str | None,
    predictions: pd.DataFrame,
    model_version: str,
    kaisai_nen: str,
    kaisai_tsukihi: str,
    keibajo_code: str,
    race_bango: str,
) -> None:
    log("STEP 6: Syncing to Neon ...")
    if not neon_url:
        log("  WARNING: NEON_PRIMARY_URL not found — skipping Neon sync")
        return

    try:
        conn = psycopg.connect(neon_url, connect_timeout=15, autocommit=True)
    except Exception as exc:
        log(f"  WARNING: Cannot connect to Neon ({type(exc).__name__}) — skipping sync")
        return

    try:
        now = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            # Ensure tables exist (autocommit needed for DDL on some Neon endpoints)
            cur.execute(CREATE_PREDICTIONS_TABLE_SQL)
            cur.execute(CREATE_ACTIVE_MODELS_TABLE_SQL)

            # Delete existing rows for this model+race, then insert fresh
            cur.execute(
                "DELETE FROM race_finish_position_model_predictions"
                " WHERE model_version = %s AND source = %s"
                " AND kaisai_nen = %s AND kaisai_tsukihi = %s"
                " AND keibajo_code = %s AND race_bango = %s",
                (model_version, "overseas", kaisai_nen, kaisai_tsukihi,
                 keibajo_code, race_bango),
            )

            insert_sql = """\
INSERT INTO race_finish_position_model_predictions (
    model_version, source, kaisai_nen, kaisai_tsukihi,
    keibajo_code, race_bango, ketto_toroku_bango, umaban,
    predicted_score, predicted_rank, predicted_top1_prob,
    predicted_top3_prob, predicted_finish_position,
    prediction_generated_at, distance_band, field_size_band,
    season_band, class_code, surface
) VALUES (
    %(model_version)s, %(source)s, %(kaisai_nen)s, %(kaisai_tsukihi)s,
    %(keibajo_code)s, %(race_bango)s, %(ketto_toroku_bango)s, %(umaban)s,
    %(predicted_score)s, %(predicted_rank)s, %(predicted_top1_prob)s,
    %(predicted_top3_prob)s, %(predicted_finish_position)s,
    %(prediction_generated_at)s, %(distance_band)s, %(field_size_band)s,
    %(season_band)s, %(class_code)s, %(surface)s
)
"""
            for _, row in predictions.iterrows():
                cur.execute(
                    insert_sql,
                    {
                        "model_version": model_version,
                        "source": "overseas",
                        "kaisai_nen": kaisai_nen,
                        "kaisai_tsukihi": kaisai_tsukihi,
                        "keibajo_code": keibajo_code,
                        "race_bango": race_bango,
                        "ketto_toroku_bango": row["ketto_toroku_bango"],
                        "umaban": int(row["umaban"]),
                        "predicted_score": float(row["predicted_score"]),
                        "predicted_rank": int(row["predicted_rank"]),
                        "predicted_top1_prob": float(row["predicted_top1_prob"]),
                        "predicted_top3_prob": float(row["predicted_top3_prob"]),
                        "predicted_finish_position": float(row["predicted_finish_position"]),
                        "prediction_generated_at": now,
                        "distance_band": "2000-2400",
                        "field_size_band": "09",
                        "season_band": "summer",
                        "class_code": row["grade"],
                        "surface": row["track_code"],
                    },
                )

            # Upsert active model (DELETE + INSERT to avoid constraint mismatch)
            cur.execute(
                "DELETE FROM finish_position_active_models WHERE category = %s",
                ("overseas",),
            )
            cur.execute(
                "INSERT INTO finish_position_active_models (category, model_version, activated_at)"
                " VALUES (%s, %s, %s)",
                ("overseas", model_version, now),
            )
        log(f"  Neon sync complete: {len(predictions)} predictions + 1 active model")
    except Exception as exc:
        log(f"  WARNING: Neon sync failed ({type(exc).__name__}: {exc}) — local data is safe")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train overseas finish-position model and predict target race"
    )
    parser.add_argument("--pg-url", default=None, help="Local PG connection URL")
    parser.add_argument("--neon-url", default=None, help="Neon PG connection URL")
    parser.add_argument("--model-version", default="overseas-lgbm-fp-v1")
    parser.add_argument("--kaisai-nen", default="2026")
    parser.add_argument("--kaisai-tsukihi", default="0725")
    parser.add_argument("--keibajo-code", default="A6")
    parser.add_argument("--race-bango", default="05")
    args = parser.parse_args()

    pg_url = args.pg_url or default_pg_url()
    neon_url = args.neon_url or default_neon_url()

    log(f"Model version: {args.model_version}")
    log(f"Target race: {args.kaisai_nen}/{args.kaisai_tsukihi}/{args.keibajo_code}/R{args.race_bango}")

    # STEP 1 + 2: Extract & engineer
    raw_df = step1_extract_training_data(pg_url)
    features, target = step2_engineer_features(raw_df)

    # STEP 3: Train
    regressor, clf_top1, clf_top3 = step3_train_models(features, target)

    # STEP 4: Predict
    predictions = step4_predict_race(
        pg_url,
        regressor,
        clf_top1,
        clf_top3,
        args.kaisai_nen,
        args.kaisai_tsukihi,
        args.keibajo_code,
        args.race_bango,
    )

    # STEP 5: Write to local PG
    step5_write_local_pg(
        pg_url,
        predictions,
        args.model_version,
        args.kaisai_nen,
        args.kaisai_tsukihi,
        args.keibajo_code,
        args.race_bango,
    )

    # STEP 6: Sync to Neon
    step6_sync_neon(
        neon_url,
        predictions,
        args.model_version,
        args.kaisai_nen,
        args.kaisai_tsukihi,
        args.keibajo_code,
        args.race_bango,
    )

    log("Done.")


if __name__ == "__main__":
    main()
