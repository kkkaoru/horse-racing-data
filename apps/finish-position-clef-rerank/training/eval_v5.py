import lightgbm as lgb
import json, pandas as pd, numpy as np, sys
import torch
W="/Users/kkk4oru/models/clef-work/data"
cat, src = sys.argv[1], sys.argv[2]
f = pd.read_parquet(f"{W}/{cat}_v5.parquet")
recs = [r for r in (json.loads(l) for l in open(src)) if "answers" in r]
c = pd.DataFrame([dict(race_id=r["race_id"], umaban=int(k), **v) for r in recs for k, v in r["answers"].items()])
d = f.merge(c, on=["race_id","umaban"]).copy()
d = d[d.groupby("race_id")["fin"].transform(lambda s: s.between(1,5).sum()) >= 5]
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1); eps=1e-4
g = d.groupby("race_id")
d["bz"] = g["brank"].transform(lambda s: z(-s.astype(float))); d["fz"] = g["general"].transform(z)
lg = lambda s: np.log(s.clip(eps,1-eps)/(1-s.clip(eps,1-eps)))
for k in ["clef_win","clef_second"]: d[k+"_z"] = g[k].transform(lambda s: z(np.log(s.clip(eps,1))))
for k in ["clef_top3","clef_top5"]: d[k+"_z"] = g[k].transform(lambda s: z(lg(s)))
CL = ["clef_win_z","clef_second_z","clef_top3_z","clef_top5_z"]
def metrics(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        o = r.sort_values([col,"umaban"], ascending=[False,True]); u = o.umaban.tolist(); fin = dict(zip(r.umaban, r.fin))
        a5 = {k for k,v in fin.items() if 1<=v<=5}; a3 = {k for k,v in fin.items() if 1<=v<=3}
        out.append((set(u[:5])==a5, set(u[:3])==a3, fin[u[0]]==1 and fin[u[1]]==2, fin[u[0]]==1, len(set(u[:5])&a5)/5, len(set(u[:3])&a3)/3))
    return np.array(out,float).mean(0)
NAMES = ["box5","box3","exacta","win1","ov5","ov3"]
def show(tag, m): return f"{tag}: " + " ".join(f"{n}={v*100:.2f}" for n, v in zip(NAMES, m))
def pl_fit(tr, cols, k=5, l2=0.01):
    races = [r for _, r in tr.groupby("race_id")]
    data = []
    for r in races:
        x = torch.tensor(r[cols].values, dtype=torch.float32)
        order = np.argsort(np.where(r.fin.between(1,18), r.fin, 99))[:k]
        data.append((x, torch.tensor(order)))
    w = torch.zeros(len(cols), requires_grad=True); opt = torch.optim.LBFGS([w], max_iter=300)
    def cl():
        opt.zero_grad(); loss = 0
        for x, order in data:
            s = x @ w; mask = torch.ones(len(s), dtype=torch.bool)
            for j in order:
                loss = loss - (s[j] - torch.logsumexp(s[mask], 0)); mask = mask.clone(); mask[j] = False
        loss = loss/len(data) + l2*(w**2).sum(); loss.backward(); return loss
    opt.step(cl); return w.detach().numpy()
d = d.sort_values(["d","race_id","umaban"])
h1 = d[d.d < pd.Timestamp("2026-07-01")]; h2 = d[d.d >= pd.Timestamp("2026-07-01")]
print(cat, "races", d.race_id.nunique(), "h1", h1.race_id.nunique(), "h2", h2.race_id.nunique())
res = {}
for nm, h in [("H2", h2), ("ALL", d)]:
    print(show(f"{nm} base", metrics(h,"bz")))
    print(show(f"{nm} base+feat(1,1)", metrics(h.assign(s=h.bz+h.fz),"s")))
for nm, cols in [("PL base+feat", ["bz","fz"]), ("PL base+feat+clef", ["bz","fz"]+CL)]:
    w = pl_fit(h1, cols); res[nm] = (cols, w)
    print(nm, dict(zip(cols, w.round(3)))); print(show("  H2", metrics(h2.assign(s=h2[cols].values@w),"s")))
# LightGBM ranker with clef
FE = ["bz","fz","brank","frank","clef_win","clef_second","clef_top3","clef_top5"] + CL
d["lab"] = np.where(d.fin.between(1,5), 6-d.fin, 0)
h1 = d[d.d < pd.Timestamp("2026-07-01")]; h2 = d[d.d >= pd.Timestamp("2026-07-01")]
for lv, rounds in [(7,150),(15,300)]:
    p = dict(objective="lambdarank", learning_rate=0.03, num_leaves=lv, min_data_in_leaf=50, feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=10, verbose=-1, num_threads=8, seed=5, lambdarank_truncation_level=6)
    m = lgb.train(p, lgb.Dataset(h1[FE], h1.lab, group=h1.groupby("race_id", sort=False).size().values), rounds)
    print(show(f"LGB clef {lv}/{rounds} H2", metrics(h2.assign(s=m.predict(h2[FE])),"s")))
    m0 = lgb.train(p, lgb.Dataset(h1[["bz","fz","brank","frank"]], h1.lab, group=h1.groupby("race_id", sort=False).size().values), rounds)
    print(show(f"LGB noclef {lv}/{rounds} H2", metrics(h2.assign(s=m0.predict(h2[["bz","fz","brank","frank"]])),"s")))
json.dump({k: dict(zip(v[0], [float(x) for x in v[1]])) for k, v in res.items()}, open(f"{W}/v5_weights_{cat}.json","w"))
