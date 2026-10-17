import lightgbm as lgb
import pandas as pd, numpy as np, json, duckdb
W="/Users/kkk4oru/models/clef-work/data"; cat="nar"
b = pd.read_parquet(f"{W}/{cat}_frame2.parquet"); b = b[b.d >= pd.Timestamp("2026-01-01")].copy()
F2 = json.load(open(f"{W}/{cat}_sfeat2_feats.json"))
b["s2"] = lgb.Booster(model_file=f"{W}/{cat}_sfeat2.txt").predict(b[F2])
bs = duckdb.connect(f"{W}/keiba.duckdb", read_only=True).execute(f"select race_id, umaban, brank from {cat}_basescore").df()
d = b.merge(bs, on=["race_id","umaban"]); d = d[d.groupby("race_id")["valid"].transform("sum")>0].copy()
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1); g = d.groupby("race_id")
d["bz"] = g["brank"].transform(lambda s: z(-s.astype(float))); d["z2"] = g["s2"].transform(z)
d["n_in"] = g.umaban.transform("count"); d["brank_norm"] = (d.brank-1)/(d.n_in-1).clip(lower=1)
d = d.sort_values(["d","race_id","umaban"])
def met(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(out,float).mean(0).round(4)
h1 = d[d.d < pd.Timestamp("2026-07-01")]; h2 = d[d.d >= pd.Timestamp("2026-07-01")]
print("h2 base", met(h2,"bz"))
for name, feats in [("small", ["bz","z2","brank","brank_norm","n_in","venue_c"]), ("mid", ["bz","z2","brank","brank_norm","n_in","venue_c"] + [c for c in F2 if c.endswith("_rz")][:30] + ["ts_avg5","last1","c4_avg3","l3r_avg3","cls_margin","days_since"])]:
    for lv, rounds in [(7,150),(15,300)]:
        p = dict(objective="lambdarank", learning_rate=0.03, num_leaves=lv, min_data_in_leaf=100, feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=10, verbose=-1, num_threads=8, seed=5)
        m = lgb.train(p, lgb.Dataset(h1[feats], h1.label, group=h1.groupby("race_id", sort=False).size().values), rounds)
        print(name, lv, rounds, "h2", met(h2.assign(s=m.predict(h2[feats])),"s"), flush=True)
