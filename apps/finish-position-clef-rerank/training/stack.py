import pandas as pd, numpy as np, lightgbm as lgb, json, sys
W="/Users/kkk4oru/models/clef-work/data"
cat = sys.argv[1]
f = pd.read_parquet(f"{W}/{cat}_2026_scored.parquet")
f = f[f.brank.notna() & (f.groupby("race_id")["valid"].transform("sum")>0)].copy()
g = f.groupby("race_id")
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1)
f["n_in"] = g.umaban.transform("count")
f["bz"] = g["brank"].transform(lambda s: z(-s)); f["fz"] = g["sfeat"].transform(z)
f["brank_norm"] = (f.brank-1)/(f.n_in-1).clip(lower=1)
f["frank_norm"] = (f.frank-1)/(f.n_in-1).clip(lower=1)
rz = [c for c in f.columns if c.endswith("_rz")]
FE = ["bz","fz","brank_norm","frank_norm","field"] + rz
def met(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(out,float).mean(0).round(4)
f = f.sort_values(["d","race_id","umaban"])
h1 = f[f.d < pd.Timestamp("2026-07-01")]; h2 = f[f.d >= pd.Timestamp("2026-07-01")]
print("h2 base", met(h2,"bz"), "lin(1,2)", met(h2.assign(s=h2.bz+2*h2.fz),"s"))
for feats_name, feats in [("small", ["bz","fz","brank_norm","frank_norm","field"]), ("full", FE)]:
    for leaves, rounds in [(7,100),(15,200),(7,300)]:
        p = dict(objective="lambdarank", learning_rate=0.03, num_leaves=leaves, min_data_in_leaf=50, feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=10, verbose=-1, num_threads=8, seed=3)
        m = lgb.train(p, lgb.Dataset(h1[feats], h1.label, group=h1.groupby("race_id", sort=False).size().values), rounds)
        print(feats_name, leaves, rounds, "h2 stack", met(h2.assign(s=m.predict(h2[feats])),"s"), flush=True)
