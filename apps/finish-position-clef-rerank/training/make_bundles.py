"""Build per-race clef rerank bundles for one race day and write a wrangler KV bulk file."""
import sys, json, pandas as pd, numpy as np, lightgbm as lgb, duckdb
sys.path.insert(0, "/Users/kkk4oru/models/clef-work")
from clef_state import build_request_v5
W = "/Users/kkk4oru/models/clef-work/data"
ymd = sys.argv[1]; out = sys.argv[2]
weights_cfg = json.load(open(f"{W}/fuse_weights.json"))
con = duckdb.connect(f"{W}/keiba.duckdb", read_only=True)
entries, index = [], []
z = lambda s: (s - s.mean()) / (s.std() if s.std() > 0 else 1)
shuka = pd.read_parquet(f"{W}/shuka_scored.parquet")[["race_id", "umaban", "spec"]]
shuka = shuka.merge(pd.read_parquet(f"{W}/shuka_v4_scored.parquet"), on=["race_id", "umaban"], how="left")  # shuka_v5_final.py
for cat in weights_cfg["categories"]:
    df = pd.read_parquet(f"{W}/{cat}_frame.parquet")
    df = df[df.race_id.str.startswith(ymd)].copy()
    if df.empty: continue
    approved = json.load(open(f"{W}/per_model_{cat}.json"))["approved"]  # written by eval_per_model.py
    feats = json.load(open(f"{W}/{cat}_sfeat_feats.json"))
    df["sfeat"] = lgb.Booster(model_file=f"{W}/{cat}_sfeat.txt").predict(df[feats], num_threads=10)
    f2 = pd.read_parquet(f"{W}/{cat}_frame2.parquet"); f2 = f2[f2.race_id.str.startswith(ymd)].copy()
    f2["sfeat2"] = lgb.Booster(model_file=f"{W}/{cat}_sfeat2.txt").predict(f2[json.load(open(f"{W}/{cat}_sfeat2_feats.json"))], num_threads=10)
    df = df.merge(f2[["race_id", "umaban", "sfeat2"]], on=["race_id", "umaban"], how="left").merge(shuka, on=["race_id", "umaban"], how="left")
    g = df.groupby("race_id")
    df["general"] = g["sfeat"].transform(z) + g["sfeat2"].transform(z)
    df["shuka"] = g["spec"].transform(z) + g["sfeat2"].transform(z)
    df["shuka_v4"] = g["v4"].transform(z)
    df["frank"] = df.groupby("race_id")["general"].rank(ascending=False, method="first")
    df["career_raw"] = np.expm1(df["career_n"]).round()
    df["brank"] = None
    for rid, g in df.groupby("race_id"):
        kj, rb = rid[8:10], rid[10:12]
        key = f"{kj}:{rb}"
        cfg = weights_cfg.get(f"{cat}:{ymd}:{key}") or weights_cfg[cat]
        if cfg.get("skip"): continue
        scores = g[cfg["featureColumn"]].astype(float)
        if not np.isfinite(scores).all():
            # A NaN featureScore (runner missing from frame2 / shuka_v4) made an invalid
            # bundle the worker silently 404'd; skip the race visibly instead.
            print(f"skip {cat}:{ymd}:{key}: non-finite {cfg['featureColumn']} for umaban {g.umaban[~np.isfinite(scores)].tolist()}", file=sys.stderr)
            continue
        r0 = g.iloc[0]
        req = build_request_v5({"surface": r0.surface, "dist": r0.dist, "going": r0.going, "field": len(g), "grade_c": r0.grade_c, "joken": r0.joken}, g.to_dict("records"))
        bundle = {"modelVersion": cfg["modelVersion"], "label": cfg["label"], "baseModels": cfg.get("baseModels", approved), "raceKey": f"{cat}:{ymd[:4]}:{ymd[4:]}:{kj}:{rb}",
                  "weights": cfg["weights"], "runners": [{"horseNumber": int(u), "featureScore": float(s)} for u, s in zip(g.umaban, g[cfg["featureColumn"]])],
                  "request": {"state": req["state"], "questions": req["questions"]}}
        entries.append({"key": f"clefrr:v1:bundle:{ymd}:{kj}:{rb}", "value": json.dumps(bundle, ensure_ascii=False, allow_nan=False, default=lambda o: None if o != o else (int(o) if float(o).is_integer() else float(o))), "expiration_ttl": 259200})
        index.append(key)
entries.append({"key": f"clefrr:v1:index:{ymd}", "value": json.dumps(sorted(index)), "expiration_ttl": 259200})
json.dump(entries, open(out, "w"), ensure_ascii=False)
print(len(index), "races")
