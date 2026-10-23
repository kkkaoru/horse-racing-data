"""Retrain production finish-position models under the win-odds ban.

Each model keeps its own recipe (feature list, CatBoost ``flat_params``, training
scope from ``metadata.json``) except:
  * every banned market column (``learning.feature_selection_policy``) is removed;
  * the four 馬連/3連複 deviation columns are appended.
Artifacts are written to ``models/finish-position/<category>/<new_version>/``
under this campaign directory, never over the production bytes.
"""
from __future__ import annotations

import argparse
import ast
import json
import sys
import time
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

REPO = Path(__file__).resolve().parents[4]
CAMPAIGN = REPO / ".cache/tansho-ban-retrain"  # run from the git-ignored campaign folder
CONTAINER = REPO / "apps/finish-position-predict-container"
sys.path.insert(0, str(REPO / "apps/pc-keiba-viewer/src/scripts"))
sys.path.insert(0, str(CONTAINER / "src"))
from learning.feature_selection_policy import MARKET_DEVIATION_FEATURES, is_banned_market_feature
from predict_lib import cell_router as cr

PROD_MODELS = CONTAINER / "models/finish-position"
OUT_MODELS = CAMPAIGN / "models/finish-position"
DEVIATION = sorted(MARKET_DEVIATION_FEATURES)
VERSION_SUFFIX = "-nowin1"
META_KEEP = ("source", "kaisai_nen", "kaisai_tsukihi", "keibajo_code", "race_bango", "ketto_toroku_bango",
             "umaban", "finish_position", "race_year", "race_id", "kyoso_joken_code", "grade_code",
             "track_code", "kyori", "shusso_tosu", "shusso_tosu_1", "kyosomei_hondai", "tansho_ninkijun",
             "nar_subclass", "current_baba_condition", "babajotai_code_shiba", "babajotai_code_dirt")


def new_version(old: str) -> str:
    return old + VERSION_SUFFIX


def flat_params(model_json: Path) -> dict:
    info = json.loads(model_json.read_text())["model_info"]["params"]
    if isinstance(info, dict):
        return dict(info["flat_params"])
    i = info.find("'flat_params': ")
    return ast.literal_eval(info[i + 15 : info.find("}", i) + 1])


def new_feature_list(old: list[str]) -> list[str]:
    kept = [c for c in old if not is_banned_market_feature(c)]
    return kept + [c for c in DEVIATION if c not in kept]


def load_store(category: str, years: list[int], columns: list[str]) -> pd.DataFrame:
    frames = []
    for y in years:
        final = CAMPAIGN / f"features/{category}-{y}/FINAL"
        if not final.exists():
            raise SystemExit(f"feature store missing for {category}-{y}")
        d = ds.dataset(final.read_text().strip(), format="parquet")
        cols = [c for c in dict.fromkeys(columns) if c in d.schema.names]
        t = d.to_table(columns=cols)
        frames.append(t.to_pandas())
    df = pd.concat(frames, ignore_index=True)
    if category == "nar":  # Ban-ei raced at 81/82/84 until 2006; those rows are not NAR.
        df = df[~df["keibajo_code"].astype(str).isin(["81", "82", "83", "84"])]
    for c in columns:
        if c not in df.columns:
            df[c] = np.nan
    df["race_key"] = df["kaisai_nen"].astype(str) + df["kaisai_tsukihi"].astype(str) + df["keibajo_code"].astype(str) + df["race_bango"].astype(str)
    df["race_date"] = (df["kaisai_nen"].astype(str) + df["kaisai_tsukihi"].astype(str)).astype(int)
    df["finish_position"] = pd.to_numeric(df["finish_position"], errors="coerce")
    # The feature store carries current_baba_condition as float (1.0); routing rules
    # match the serving string form ("1"), so normalize or every condition cell is empty.
    if "current_baba_condition" in df.columns:
        df["current_baba_condition"] = pd.to_numeric(df["current_baba_condition"], errors="coerce").map(
            lambda v: None if pd.isna(v) else str(int(v)))
    return df.sort_values(["race_date", "race_key", "umaban"], kind="stable").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Training scope / labels / weights per model family (from metadata.json)
# ---------------------------------------------------------------------------

ROUTER = cr.load_cell_router(CONTAINER / "src/predict_lib/cell_routing.json")


def race_variants(df: pd.DataFrame, category: str) -> pd.Series:
    """Production cell-router variant per race (first entry, declared-runner count)."""
    firsts = df.groupby("race_key", sort=False).head(1)
    sizes = df.groupby("race_key", sort=False).size()
    out = {}
    routing = ROUTER.routing_for(category)
    for row in firsts.to_dict("records"):
        entry = {k: (None if (isinstance(v, float) and np.isnan(v)) else v) for k, v in row.items()}
        out[row["race_key"]] = ROUTER.resolve_variant(category, [entry] * int(sizes[row["race_key"]])) if routing else "sim"
    return df["race_key"].map(out)


def rule_conditions_for(category: str, variant: str) -> list[tuple]:
    return [rule.conditions for rule in ROUTER.routing_for(category).rules if rule.variant == variant]


def race_matches(df: pd.DataFrame, category: str, conditions_list: list[tuple], drop_dims: frozenset[str] = frozenset()) -> pd.Series:
    firsts = df.groupby("race_key", sort=False).head(1)
    sizes = df.groupby("race_key", sort=False).size()
    hits = {}
    for row in firsts.to_dict("records"):
        entry = {k: (None if (isinstance(v, float) and np.isnan(v)) else v) for k, v in row.items()}
        ok = False
        for conds in conditions_list:
            kept = tuple(c for c in conds if c.dimension not in drop_dims)
            if cr.all_conditions_match(entry, kept, category, field_size=int(sizes[row["race_key"]])):
                ok = True
                break
        hits[row["race_key"]] = ok
    return df["race_key"].map(hits).astype(bool)


def relevance_321(fp: pd.Series) -> np.ndarray:
    return fp.map({1: 3, 2: 2, 3: 1}).fillna(0).to_numpy(dtype=float)


def winner_only(fp: pd.Series) -> np.ndarray:
    return (fp == 1).astype(float).to_numpy()


def race_names(df: pd.DataFrame) -> pd.Series:
    """JV-Data race names are full-width (中京２歳, ＢＳＮ賞); model tokens are NFKC."""
    return df["kyosomei_hondai"].fillna("").astype(str).map(lambda s: unicodedata.normalize("NFKC", s))


def named_scope(df: pd.DataFrame, token: str, venue: str) -> pd.Series:
    """All starts of horses that ever ran the named race at ``venue`` (before the eval year)."""
    name = race_names(df)
    named = (name.str.contains(unicodedata.normalize("NFKC", token), regex=False)) & (df["keibajo_code"].astype(str) == venue)
    horses = set(df.loc[named, "ketto_toroku_bango"].astype(str))
    return df["ketto_toroku_bango"].astype(str).isin(horses) | named


def training_window(meta: dict, train_end: int) -> tuple[int, int]:
    """Inclusive YYYYMMDD window: the model's own recorded range, else [-inf, train_end)."""
    rng = meta.get("train_date_range")
    if isinstance(rng, list) and len(rng) == 2:
        return int(rng[0]), int(rng[1])
    if isinstance(rng, str) and "-" in rng:
        start, end = rng.split("-")
        return int(start), int(end)
    through = meta.get("train_through_date")
    if isinstance(through, str):
        return 0, int(through.replace("-", ""))
    return 0, train_end * 10000 + 100


def build_training_frame(df: pd.DataFrame, category: str, old_version: str, meta: dict, train_end: int):
    """Return (frame, labels, group_weights or None) for one model."""
    start, end = training_window(meta, train_end)
    d = df[(df["race_date"] >= start) & (df["race_date"] <= end) & df["finish_position"].notna() & (df["finish_position"] > 0)]
    weights = None
    if old_version.startswith(("jra-joken-",)):
        codes = set(meta.get("training_kyoso_joken_codes") or [])
        d = d[d["kyoso_joken_code"].astype(str).isin(codes)]
        if meta.get("grade_code_required_blank"):
            d = d[d["grade_code"].fillna("").astype(str).str.strip() == ""]
        objective = str(meta.get("objective", ""))
        if "winner-only" in objective:
            labels = winner_only(d["finish_position"])
            variant = next((v for v, s in ROUTER.routing_for(category).variants.items() if s.model_version .rsplit("-nowin", 1)[0] == old_version), None)
            target_w = float(meta.get("target_cell_weight") or 1.0)
            if variant is not None and target_w != 1.0:
                conds = rule_conditions_for(category, variant)
                target = race_matches(d, category, conds)
                w = np.where(target, target_w, 1.0)
                if meta.get("related_cell_weight"):
                    related = race_matches(d, category, conds, drop_dims=frozenset({"season", "month"})) & ~target
                    w = np.where(related, float(meta["related_cell_weight"]), w)
                weights = w
        else:
            labels = relevance_321(d["finish_position"])
    elif old_version.startswith("jra-named-"):
        d = d[named_scope(d, meta["race_name_token"], meta["venue"])]
        labels = relevance_321(d["finish_position"])
        if "upsample" in str(meta.get("training_scope", "")):
            named = race_names(d).str.contains(unicodedata.normalize("NFKC", meta["race_name_token"]), regex=False)
            weights = np.where(named, 3.0, 1.0)
    else:
        labels = relevance_321(d["finish_position"])
    return d, labels, weights


def group_weight_per_race(d: pd.DataFrame, row_weights: np.ndarray) -> np.ndarray:
    """CatBoost ranking pools take one weight per group; use the race's row weight."""
    return pd.Series(row_weights, index=d.index).groupby(d["race_key"], sort=False).transform("first").to_numpy()


def train_catboost(category: str, old_version: str, df: pd.DataFrame, train_end: int) -> Path:
    from catboost import CatBoost, Pool

    src = PROD_MODELS / category / old_version
    meta = json.loads((src / "metadata.json").read_text())
    params = flat_params(src / "model.json")
    params.update({"thread_count": 6, "verbose": 0, "allow_writing_files": False})
    params.pop("eval_metric", None)
    features = new_feature_list(list(meta["feature_names"]))
    d, labels, weights = build_training_frame(df, category, old_version, meta, train_end)
    x = d[features].apply(pd.to_numeric, errors="coerce").astype("float32")
    group_ids = pd.factorize(d["race_key"])[0]
    pool = Pool(x, label=labels, group_id=group_ids,
                group_weight=None if weights is None else group_weight_per_race(d, weights))
    started = time.time()
    model = CatBoost(params)
    model.fit(pool)
    version = new_version(old_version)
    out = OUT_MODELS / category / version
    out.mkdir(parents=True, exist_ok=True)
    model.save_model(str(out / "model.json"), format="json")
    new_meta = {k: v for k, v in meta.items() if k not in ("feature_names", "feature_columns")}
    new_meta.update({
        "model_version": version,
        "feature_names": features,
        "feature_count": len(features),
        "based_on": old_version,
        "win_odds_banned": True,
        "removed_market_features": [c for c in meta["feature_names"] if is_banned_market_feature(c)],
        "added_market_deviation_features": DEVIATION,
        "n_train_rows": int(len(d)),
        "n_train_races": int(d["race_key"].nunique()),
        "train_date_range": [str(int(d["race_date"].min())), str(int(d["race_date"].max()))],
        "hyperparams": {k: params[k] for k in ("loss_function", "iterations", "learning_rate", "depth", "l2_leaf_reg", "random_seed")},
        "train_duration_sec": round(time.time() - started, 1),
    })
    (out / "metadata.json").write_text(json.dumps(new_meta, ensure_ascii=False, indent=2))
    return out


# ---------------------------------------------------------------------------
# XGBoost (NAR) families
# ---------------------------------------------------------------------------

NAR_SCOPE_DIMS = {
    None: None,  # full routing-rule conditions
    "venue_class_distance": {"venue", "nar_subclass", "canonical_distance_band"},
    "class_distance_all_venues": {"nar_subclass", "canonical_distance_band"},
    "venue_class_distance_condition_medium": {
        "venue", "nar_subclass", "canonical_distance_band", "current_baba_condition", "canonical_field_size_band"},
}


def nar_cell_scope(df: pd.DataFrame, variant: str, meta: dict) -> pd.Series:
    scope = meta.get("training_scope")
    conds = rule_conditions_for("nar", variant)
    if not conds:  # consensus panel members have no rule of their own
        conds = rule_conditions_for("nar", "c50_tc2_consensus")
    if scope in NAR_SCOPE_DIMS:
        keep = NAR_SCOPE_DIMS[scope]
        drop = frozenset() if keep is None else frozenset(c.dimension for c in conds[0] if c.dimension not in keep)
        return race_matches(df, "nar", conds, drop_dims=drop)
    if "MUKATSU" in str(scope):
        return (df["nar_subclass"].astype(str) == "MUKATSU") & race_matches(
            df, "nar", conds, drop_dims=frozenset({"venue", "season", "current_baba_condition", "canonical_field_size_band", "canonical_distance_band"}))
    # consensus: C class, sprint, summer, track conditions 1+2, all venues and field sizes
    base = race_matches(df, "nar", conds, drop_dims=frozenset({"venue", "canonical_field_size_band", "current_baba_condition"}))
    return base & df["current_baba_condition"].astype(str).isin({"1", "2"})


def xgb_learner_params(model_json: Path) -> dict:
    m = json.loads(model_json.read_text())["learner"]
    trees = m["gradient_booster"]["model"]["trees"]
    depth = 0
    for t in trees:
        left = t["left_children"]
        d = {0: 0}
        for i, l in enumerate(left):
            if l != -1:
                d[l] = d[i] + 1
                d[t["right_children"][i]] = d[i] + 1
        depth = max(depth, max(d.values()))
    obj = m["objective"]
    params = {"objective": obj["name"], "n_estimators": len(trees), "max_depth": depth}
    spw = obj.get("reg_loss_param", {}).get("scale_pos_weight")
    if spw is not None:
        params["scale_pos_weight"] = float(spw)
    return params


def train_xgboost(category: str, old_version: str, df: pd.DataFrame, train_end: int) -> Path:
    import xgboost as xgb

    src = PROD_MODELS / category / old_version
    meta = json.loads((src / "metadata.json").read_text())
    features = new_feature_list(list(meta["feature_names"]))
    start, end = training_window(meta, train_end)
    d = df[(df["race_date"] >= start) & (df["race_date"] <= end) & df["finish_position"].notna() & (df["finish_position"] > 0)]
    hp = meta.get("hyperparams")
    learned = xgb_learner_params(src / "model.json")
    variant = next((v for v, sp in ROUTER.routing_for(category).variants.items() if sp.model_version .rsplit("-nowin", 1)[0] == old_version), None)
    if variant is not None and variant != ROUTER.routing_for(category).default_variant:
        d = d[nar_cell_scope(d, variant, meta)]
    objective = str(meta.get("objective", learned["objective"]))
    x = d[features].apply(pd.to_numeric, errors="coerce").astype("float32")
    if hp:  # category default: exact recorded HPO params, tree count of the served booster
        params = {k: v for k, v in hp.items() if k in ("learning_rate", "max_depth", "min_child_weight", "reg_lambda", "subsample", "colsample_bytree", "tree_method")}
        params.update(objective=hp["objective"], n_estimators=learned["n_estimators"], random_state=int(hp.get("seed", 42)))
        model = xgb.XGBRanker(**params)
        model.fit(x, relevance_321(d["finish_position"]), qid=pd.factorize(d["race_key"])[0])
    elif learned["objective"].startswith("rank"):
        model = xgb.XGBRanker(objective=learned["objective"], n_estimators=learned["n_estimators"], max_depth=learned["max_depth"], learning_rate=0.05, tree_method="hist", random_state=42)
        model.fit(x, relevance_321(d["finish_position"]), qid=pd.factorize(d["race_key"])[0])
    else:
        target = 2 if "==2" in objective or "top2" in old_version else 1
        y = (d["finish_position"] == target).astype(int)
        model = xgb.XGBClassifier(objective="binary:logistic", n_estimators=learned["n_estimators"], max_depth=learned["max_depth"], learning_rate=0.05,
                                  scale_pos_weight=learned.get("scale_pos_weight", 1.0), tree_method="hist", random_state=42)
        model.fit(x, y)
    version = new_version(old_version)
    out = OUT_MODELS / category / version
    out.mkdir(parents=True, exist_ok=True)
    model.get_booster().feature_names = None
    model.save_model(str(out / "model.json"))
    new_meta = {k: v for k, v in meta.items() if k not in ("feature_names", "feature_columns")}
    new_meta.update({
        "model_version": version, "feature_names": features, "feature_count": len(features), "based_on": old_version,
        "win_odds_banned": True,
        "removed_market_features": [c for c in meta["feature_names"] if is_banned_market_feature(c)],
        "added_market_deviation_features": DEVIATION,
        "n_train_rows": int(len(d)), "n_train_races": int(d["race_key"].nunique()),
        "train_date_range": [str(int(d["race_date"].min())), str(int(d["race_date"].max()))],
        "retrain_params": {k: v for k, v in (model.get_params().items()) if k in ("objective", "n_estimators", "max_depth", "learning_rate", "scale_pos_weight", "random_state")},
    })
    (out / "metadata.json").write_text(json.dumps(new_meta, ensure_ascii=False, indent=2, default=str))
    return out


def selected_versions(category: str) -> list[str]:
    from predict_lib import artifact_integrity as ai

    out = []
    for key in sorted(ai.derive_selected_artifact_keys()):
        parts = key.split("/")
        if parts[0] == "finish-position" and parts[1] == category and parts[-1] == "metadata.json":
            out.append(parts[2])
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", required=True)
    ap.add_argument("--models", default="catboost-selected")
    ap.add_argument("--from-year", type=int, default=2013)
    ap.add_argument("--train-end-year", type=int, default=2026)
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--suffix", default="-nowin1")
    a = ap.parse_args()
    globals()["VERSION_SUFFIX"] = a.suffix
    versions = selected_versions(a.category) if a.models == "catboost-selected" else a.models.split(",")
    jobs = []
    # Shadow-only collectors (never change served ranks; market-based by design and
    # rejected by the serving guard) and the Stage-1 fallback (disabled once every
    # served model is win-odds free) are not retrained.
    skip_prefixes = ("jra-dynamic-market-shadow-", "jra-joint-group-dro-", "jra-cb-high-payout-",
                     "jra-cb-stage1-", "iter12-nar-xgb-hpo-v8-stage1-", "jra-dirt-small-005-hybrid")
    for v in versions:
        if v.startswith(skip_prefixes):
            print("skip (shadow/stage1/no-trainer)", v); continue
        src = PROD_MODELS / a.category / v
        if not (src / "model.json").exists() or not (src / "metadata.json").exists():
            print("skip (no catboost artifact)", v); continue
        is_xgb = "learner" in json.loads((src / "model.json").read_text())
        if not is_xgb:
            try:
                flat_params(src / "model.json")
            except Exception:
                print("skip (unknown architecture)", v); continue
        meta = json.loads((src / "metadata.json").read_text())
        if meta.get("shadow_only"):
            print("skip (shadow-only)", v); continue
        if a.skip_existing and (OUT_MODELS / a.category / new_version(v) / "model.json").exists():
            print("exists", v); continue
        jobs.append((v, meta, is_xgb))
    columns = list(META_KEEP)
    for _v, meta, _x in jobs:
        columns += new_feature_list(list(meta["feature_names"]))
    # A model's own recorded window may run into the current year; load through it.
    last_year = max([a.train_end_year - 1] + [training_window(m, a.train_end_year)[1] // 10000 for _v, m, _x in jobs])
    years = list(range(a.from_year, last_year + 1))
    df = load_store(a.category, years, list(dict.fromkeys(columns)))
    print("store rows", len(df), "races", df["race_key"].nunique(), flush=True)
    for v, _meta, is_xgb in jobs:
        t = time.time()
        trainer = train_xgboost if is_xgb else train_catboost
        out = trainer(a.category, v, df, a.train_end_year)
        print("trained", v, "->", out.name, f"{time.time() - t:.0f}s", flush=True)


if __name__ == "__main__":
    main()
