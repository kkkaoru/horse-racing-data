"""Compare the clef rerank with the served order per served base model and list the models it improves.

Usage: eval_per_model.py <cat>. Reads cv_<cat>.parquet written by eval_cv.py (out-of-month PL predictions)
and <cat>_basescore (served model per race). A model is approved only when it has at least MIN_RACES CV
races, box5 / box3 / exacta are each no worse than the served order, and their sum improves.
"""
import sys, json, duckdb, pandas as pd, numpy as np
W = "/Users/kkk4oru/models/clef-work/data"
MIN_RACES = 30
METRICS = ["box5", "box3", "exacta"]
cat = sys.argv[1]; col = sys.argv[2] if len(sys.argv) > 2 else "PL b+f+clef"
T = pd.read_parquet(f"{W}/cv_{cat}.parquet"); T["race_id"] = T.race_id.astype(str)
bs = duckdb.connect(f"{W}/keiba.duckdb", read_only=True).execute(f"select distinct race_id, bmv from {cat}_basescore").df()
bs["race_id"] = bs.race_id.astype(str)
T = T.merge(bs.drop_duplicates("race_id"), on="race_id")
def per_race(x, score):
    rows = []
    for rid, r in x.groupby("race_id"):
        u = r.sort_values([score, "umaban"], ascending=[False, True]).umaban.tolist(); fin = dict(zip(r.umaban, r.fin))
        a5 = {k for k, v in fin.items() if 1 <= v <= 5}; a3 = {k for k, v in fin.items() if 1 <= v <= 3}
        rows.append((rid, r.bmv.iloc[0], set(u[:5]) == a5, set(u[:3]) == a3, fin[u[0]] == 1 and fin[u[1]] == 2))
    return pd.DataFrame(rows, columns=["race_id", "bmv"] + METRICS).set_index("race_id")
base = per_race(T, "bz"); fused = per_race(T, col)
rng = np.random.default_rng(0)
out = []
for bmv, ids in base.groupby("bmv").groups.items():
    b = base.loc[ids, METRICS].astype(float); f = fused.loc[ids, METRICS].astype(float)
    delta = (f - b).mean() * 100; diff = (f.sum(axis=1) - b.sum(axis=1)).to_numpy()
    approved = len(ids) >= MIN_RACES and delta.sum() > 0 and bool((delta >= 0).all())
    out.append(dict(model=bmv, races=len(ids), **{f"base_{k}": round(b[k].mean() * 100, 2) for k in METRICS},
                    **{f"d_{k}": round(delta[k], 2) for k in METRICS}, d_sum=round(delta.sum(), 2),
                    p_no_gain=round(float(np.mean([rng.choice(diff, len(diff)).mean() <= 0 for _ in range(2000)])), 3),
                    approved=approved))
o = pd.DataFrame(out).sort_values("races", ascending=False)
pd.set_option("display.width", 250)
print(o.to_string(index=False))
json.dump({"criterion": f"races>={MIN_RACES}, each of {METRICS} delta>=0, sum>0", "column": col,
           "models": o.to_dict("records"), "approved": sorted(o[o.approved].model.tolist())},
          open(f"{W}/per_model_{cat}.json", "w"), ensure_ascii=False, indent=1)
