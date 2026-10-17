import json, pandas as pd, numpy as np, torch, sys
W="/Users/kkk4oru/models/clef-work/data"
cat, src = sys.argv[1], sys.argv[2]
sfx = sys.argv[3] if len(sys.argv) > 3 else ""
f = pd.read_parquet(f"{W}/{cat}_2026_scored.parquet")
if sfx:
    import lightgbm as lgb
    fr = pd.read_parquet(f"{W}/{cat}_frame2.parquet", columns=None); fr = fr[fr.d >= pd.Timestamp("2026-01-01")]
    feats = json.load(open(f"{W}/{cat}_sfeat2_feats.json"))
    fr["sfeat2"] = lgb.Booster(model_file=f"{W}/{cat}_sfeat2.txt").predict(fr[feats], num_threads=8)
    f = f.merge(fr[["race_id","umaban","sfeat2"]], on=["race_id","umaban"], how="left"); f["sfeat"] = f["sfeat2"]
recs = [r for r in (json.loads(l) for l in open(src)) if "answers" in r]
K = sorted(next(iter(recs[0]["answers"].values())).keys())
c = pd.DataFrame([dict(race_id=r["race_id"], umaban=int(k), **v) for r in recs for k, v in r["answers"].items()])
d = f.merge(c, on=["race_id","umaban"]).copy()
d = d[d.brank.notna() & (d.groupby("race_id")["valid"].transform("sum")>0)]
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1)
g = d.groupby("race_id"); eps=1e-4
d["bz"] = g["brank"].transform(lambda s: z(-s.astype(float))); d["fz"] = g["sfeat"].transform(z)
Z = ["bz","fz"]
for k in K:
    d[k+"_z"] = g[k].transform(lambda s: z(np.log(s.clip(eps,1-eps)/(1-s.clip(eps,1-eps))))); Z.append(k+"_z")
def met(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(out,float).mean(0).round(4)
def fit(tr, cols, l2=0.05):
    races = [r for _, r in tr.groupby("race_id")]
    X = [torch.tensor(r[cols].values, dtype=torch.float32) for r in races]
    Y = [torch.tensor((r.fin.values==1).astype(np.float32)) for r in races]
    T = [torch.tensor(r.fin.between(1,3).values.astype(np.float32)) for r in races]
    w = torch.zeros(len(cols), requires_grad=True); opt = torch.optim.LBFGS([w], max_iter=300)
    def cl():
        opt.zero_grad(); loss = sum(-(torch.log_softmax(x@w,0)*y).sum() - 0.5*(torch.log_softmax(x@w,0)*t).sum()/3 for x,y,t in zip(X,Y,T))/len(X) + l2*(w**2).sum(); loss.backward(); return loss
    opt.step(cl); return w.detach().numpy()
h1 = d[d.d < pd.Timestamp("2026-07-01")]; h2 = d[d.d >= pd.Timestamp("2026-07-01")]
print("races", d.race_id.nunique(), h1.race_id.nunique(), h2.race_id.nunique())
for k in Z: print(k, "alone h2", met(h2, k), "all", met(d, k))
for nm, cols in [("base+feat", ["bz","fz"]), ("base+feat+clef", Z)]:
    w = fit(h1, cols); print(nm, dict(zip(cols, w.round(3))), "h2", met(h2.assign(s=h2[cols].values@w), "s"))
    wa = fit(d, cols); print(nm, "fit-all", dict(zip(cols, wa.round(3))))
