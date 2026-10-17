import lightgbm as lgb
import xgboost as xgb
import os
import json, pandas as pd, numpy as np, sys
W="/Users/kkk4oru/models/clef-work/data"
cat = sys.argv[1]; frame = sys.argv[2]; srcs = sys.argv[3:]
f = pd.read_parquet(f"{W}/{frame}")
frames = []
for src in srcs:
    recs = [r for r in (json.loads(l) for l in open(src)) if "answers" in r]
    frames.append(pd.DataFrame([dict(race_id=r["race_id"], umaban=int(k), **v) for r in recs for k, v in r["answers"].items()]))
c = pd.concat(frames).groupby(["race_id","umaban"], as_index=False).mean(numeric_only=True)
d = f.merge(c, on=["race_id","umaban"]).copy()
d = d[d.groupby("race_id")["fin"].transform(lambda s: s.between(1,5).sum()) >= 5]
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1); eps=1e-4
g = d.groupby("race_id")
d["bz"] = g["brank"].transform(lambda s: z(-s.astype(float))); d["fz"] = g["general"].transform(z)
lg = lambda s: np.log(s.clip(eps,1-eps)/(1-s.clip(eps,1-eps)))
for k in ["clef_win","clef_second"]: d[k+"_z"] = g[k].transform(lambda s: z(np.log(s.clip(eps,1))))
for k in ["clef_top3","clef_top5"]: d[k+"_z"] = g[k].transform(lambda s: z(lg(s)))
for k in ["clef_win","clef_second","clef_top3","clef_top5"]: d[k+"_r"] = g[k].rank(ascending=False)
d["n_in"] = g.umaban.transform("count"); d["brank_norm"] = (d.brank-1)/(d.n_in-1).clip(lower=1)
d["frank2"] = g["general"].rank(ascending=False)
d["month"] = d.d.dt.month
d = d.sort_values(["d","race_id","umaban"])
BASE = ["bz","fz","brank","frank2","brank_norm","n_in"]
CLEF = ["clef_win","clef_second","clef_top3","clef_top5","clef_win_z","clef_second_z","clef_top3_z","clef_top5_z","clef_win_r","clef_second_r","clef_top3_r","clef_top5_r"]
P = dict(objective="binary", learning_rate=0.03, num_leaves=7, min_data_in_leaf=80, feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=10, verbose=-1, num_threads=8, seed=7)
XP = dict(objective="binary:logistic", eta=0.03, max_depth=3, min_child_weight=20, subsample=0.8, colsample_bytree=0.8, reg_lambda=10, nthread=8, seed=7)
TARGETS = {"p5": lambda x: x.fin.between(1,5), "p3": lambda x: x.fin.between(1,3), "p1": lambda x: x.fin==1, "p2": lambda x: x.fin==2}
def compose(r, pref):
    r5 = r.sort_values([pref+"p5","umaban"], ascending=[False,True]).head(5)
    r3 = r5.sort_values([pref+"p3","umaban"], ascending=[False,True]).head(3)
    first = r3.sort_values([pref+"p1","umaban"], ascending=[False,True]).iloc[0].umaban
    rest3 = r3[r3.umaban != first]
    second = rest3.sort_values([pref+"p2","umaban"], ascending=[False,True]).iloc[0].umaban
    third = [u for u in r3.umaban if u not in (first, second)]
    rest5 = [u for u in r5.sort_values(pref+"p5", ascending=False).umaban if u not in r3.umaban.tolist()]
    return [first, second] + third + rest5
def metrics_order(x, orderer):
    out=[]
    for rid, r in x.groupby("race_id"):
        u = orderer(r); fin = dict(zip(r.umaban, r.fin))
        a5 = {k for k,v in fin.items() if 1<=v<=5}; a3 = {k for k,v in fin.items() if 1<=v<=3}
        out.append((set(u[:5])==a5, set(u[:3])==a3, fin[u[0]]==1 and fin[u[1]]==2, fin[u[0]]==1, len(set(u[:5])&a5)/5, len(set(u[:3])&a3)/3))
    return np.array(out,float).mean(0)
by = lambda col: (lambda r: r.sort_values([col,"umaban"], ascending=[False,True]).umaban.tolist())
NAMES = ["box5","box3","exacta","win1","ov5","ov3"]
def show(tag, m): return f"{tag:24s} " + " ".join(f"{n}={v*100:5.2f}" for n, v in zip(NAMES, m)) + f"  sum3={sum(m[:3])*100:5.2f}"
months = sorted(d.month.unique()); test_months = [m for m in months if m >= 3]
parts = []
for m in test_months:
    tr = d[d.month < m]; te = d[d.month == m].copy()
    if tr.race_id.nunique() < 150 or te.empty: continue
    for pref, cols in [("n_", BASE), ("c_", BASE+CLEF)]:
        for t, fn in TARGETS.items():
            if os.environ.get("XGB"):
                mdl = xgb.train(XP, xgb.DMatrix(tr[cols].to_numpy(np.float32), label=fn(tr).astype(int).to_numpy()), 200)
                te[pref+t] = mdl.predict(xgb.DMatrix(te[cols].to_numpy(np.float32)), output_margin=True)
            else:
                mdl = lgb.train(P, lgb.Dataset(tr[cols], fn(tr).astype(int)), 200)
                te[pref+t] = mdl.predict(te[cols])
    parts.append(te)
T = pd.concat(parts)
print(cat, "test races", T.race_id.nunique(), "months", sorted(T.month.unique()))
print(show("base", metrics_order(T, by("bz"))))
print(show("base+feat(1,1)", metrics_order(T.assign(s=T.bz+T.fz), by("s"))))
print(show("HIER no-clef", metrics_order(T, lambda r: compose(r, "n_"))))
print(show("HIER with clef", metrics_order(T, lambda r: compose(r, "c_"))))
print(show("p5-only no-clef", metrics_order(T, by("n_p5"))))
print(show("p5-only clef", metrics_order(T, by("c_p5"))))
T.to_parquet(f"{W}/hier_{cat}.parquet")
