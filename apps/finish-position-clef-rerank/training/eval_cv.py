import lightgbm as lgb
import json, pandas as pd, numpy as np, sys
import torch
W="/Users/kkk4oru/models/clef-work/data"
cat = sys.argv[1]; srcs = sys.argv[2:]
f = pd.read_parquet(f"{W}/{cat}_v5.parquet")
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
d["n_in"] = g.umaban.transform("count"); d["brank_norm"] = (d.brank-1)/(d.n_in-1).clip(lower=1)
CL = ["clef_win_z","clef_second_z","clef_top3_z","clef_top5_z"]
d["lab"] = np.where(d.fin.between(1,5), 6-d.fin, 0)
d["month"] = d.d.dt.month
d = d.sort_values(["d","race_id","umaban"])
def metrics(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        o = r.sort_values([col,"umaban"], ascending=[False,True]); u = o.umaban.tolist(); fin = dict(zip(r.umaban, r.fin))
        a5 = {k for k,v in fin.items() if 1<=v<=5}; a3 = {k for k,v in fin.items() if 1<=v<=3}
        out.append((set(u[:5])==a5, set(u[:3])==a3, fin[u[0]]==1 and fin[u[1]]==2, fin[u[0]]==1, len(set(u[:5])&a5)/5, len(set(u[:3])&a3)/3))
    return np.array(out,float).mean(0)
NAMES = ["box5","box3","exacta","win1","ov5","ov3"]
def show(tag, m): return f"{tag:28s} " + " ".join(f"{n}={v*100:5.2f}" for n, v in zip(NAMES, m)) + f"  sum3={sum(m[:3])*100:5.2f}"
def pl_fit(tr, cols, k=5, l2=0.01):
    data = []
    for _, r in tr.groupby("race_id"):
        x = torch.tensor(r[cols].values, dtype=torch.float32)
        data.append((x, list(np.argsort(np.where(r.fin.between(1,18), r.fin, 99))[:k])))
    w = torch.zeros(len(cols), requires_grad=True); opt = torch.optim.LBFGS([w], max_iter=60)
    def cl():
        opt.zero_grad(); loss = 0
        for x, order in data:
            s = x @ w; mask = torch.ones(len(s), dtype=torch.bool)
            for j in order:
                loss = loss - (s[j] - torch.logsumexp(s[mask], 0)); mask = mask.clone(); mask[j] = False
        loss = loss/len(data) + l2*(w**2).sum(); loss.backward(); return loss
    opt.step(cl); return w.detach().numpy()
LGBP = dict(objective="lambdarank", learning_rate=0.03, num_leaves=7, min_data_in_leaf=50, feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=10, verbose=-1, num_threads=8, seed=5, lambdarank_truncation_level=6)
CONF = {
  "PL b+f": ("pl", ["bz","fz"]),
  "PL b+f+clef": ("pl", ["bz","fz"]+CL),
  "PL b+f+top3/5": ("pl", ["bz","fz","clef_top3_z","clef_top5_z"]),
  "LGB b+f": ("lgb", ["bz","fz","brank_norm","n_in"]),
  "LGB b+f+clef": ("lgb", ["bz","fz","brank_norm","n_in","clef_win","clef_second","clef_top3","clef_top5"]+CL),
}
test_months = [m for m in sorted(d.month.unique()) if m >= 3]
preds = {k: [] for k in CONF}
for m in test_months:
    tr = d[d.month < m]; te = d[d.month == m].copy()
    for name, (kind, cols) in CONF.items():
        if kind == "pl":
            w = pl_fit(tr, cols); te[name] = te[cols].values @ w
        else:
            mdl = lgb.train(LGBP, lgb.Dataset(tr[cols], tr.lab, group=tr.groupby("race_id", sort=False).size().values), 150)
            te[name] = mdl.predict(te[cols])
    for name in CONF: preds[name].append(te[["race_id","umaban",name]])
    print("month", m, "done", flush=True)
T = d[d.month.isin(test_months)].copy()
for name in CONF: T = T.merge(pd.concat(preds[name]), on=["race_id","umaban"])
print(cat, "test races", T.race_id.nunique())
print(show("base", metrics(T,"bz")))
print(show("base+feat(1,1)", metrics(T.assign(s=T.bz+T.fz),"s")))
for name in CONF: print(show(name, metrics(T, name)))
for w in [0.1, 0.2, 0.3]:
    print(show(f"b+f+{w}*clef(t3+t5+sec)", metrics(T.assign(s=T.bz+T.fz+w*(T.clef_top3_z+T.clef_top5_z+T.clef_second_z)),"s")))
T.to_parquet(f"{W}/cv_{cat}.parquet")
