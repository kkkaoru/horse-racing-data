import lightgbm as lgb
import json, pandas as pd, numpy as np, sys
import torch
W="/Users/kkk4oru/models/clef-work/data"
srcs = sys.argv[1:]
a = pd.read_parquet(f"{W}/jra_frame.parquet"); a = a[a.d >= pd.Timestamp("2026-01-01")].copy()
b = pd.read_parquet(f"{W}/jra_frame2.parquet"); b = b[b.d >= pd.Timestamp("2026-01-01")].copy()
a["s1"] = lgb.Booster(model_file=f"{W}/jra_sfeat.txt").predict(a[json.load(open(f"{W}/jra_sfeat_feats.json"))])
b["s2"] = lgb.Booster(model_file=f"{W}/jra_sfeat2.txt").predict(b[json.load(open(f"{W}/jra_sfeat2_feats.json"))])
f = pd.read_parquet(f"{W}/jra_2026_scored.parquet")[["race_id","umaban","brank"]].merge(a[["race_id","umaban","s1","fin","valid","d"]], on=["race_id","umaban"]).merge(b[["race_id","umaban","s2"]], on=["race_id","umaban"])
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1); eps=1e-4
def met(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(out,float).mean(0).round(4)
def fit(tr, cols, l2=0.02):
    races = [r for _, r in tr.groupby("race_id")]
    X = [torch.tensor(r[cols].values, dtype=torch.float32) for r in races]
    Y = [torch.tensor((r.fin.values==1).astype(np.float32)) for r in races]
    T = [torch.tensor(r.fin.between(1,3).values.astype(np.float32)) for r in races]
    w = torch.zeros(len(cols), requires_grad=True); opt = torch.optim.LBFGS([w], max_iter=300)
    def cl():
        opt.zero_grad(); loss = sum(-(torch.log_softmax(x@w,0)*y).sum() - 0.5*(torch.log_softmax(x@w,0)*t).sum()/3 for x,y,t in zip(X,Y,T))/len(X) + l2*(w**2).sum(); loss.backward(); return loss
    opt.step(cl); return w.detach().numpy()
for src in srcs:
    recs = [r for r in (json.loads(l) for l in open(src)) if "answers" in r]
    c = pd.DataFrame([(r["race_id"], int(k), v["clef_win"], v["clef_top3"]) for r in recs for k, v in r["answers"].items()], columns=["race_id","umaban","cw","ct"])
    d = f.merge(c, on=["race_id","umaban"]); d = d[d.brank.notna() & (d.groupby("race_id")["valid"].transform("sum")>0)].copy()
    g = d.groupby("race_id")
    d["bz"] = g["brank"].transform(lambda s: z(-s.astype(float))); d["fz"] = g["s1"].transform(z) + g["s2"].transform(z)
    d["tz"] = g["ct"].transform(lambda s: z(np.log(s.clip(eps,1-eps)/(1-s.clip(eps,1-eps))))); d["wz"] = g["cw"].transform(lambda s: z(np.log(s.clip(eps,1))))
    h1 = d[d.d < pd.Timestamp("2026-07-01")]; h2 = d[d.d >= pd.Timestamp("2026-07-01")]
    print(src.split("/")[-1], "races", d.race_id.nunique(), h1.race_id.nunique(), h2.race_id.nunique())
    print("  fixed b+f (1,1)", "h2", met(h2.assign(s=h2.bz+h2.fz),"s"), "all", met(d.assign(s=d.bz+d.fz),"s"))
    for cols in [["bz","fz"], ["bz","fz","tz","wz"]]:
        w1 = fit(h1, cols); wa = fit(d, cols)
        print("  ", cols, "fit-h1", w1.round(3), "h2", met(h2.assign(s=h2[cols].values@w1),"s"), "| fit-all", wa.round(3), "all(in-sample)", met(d.assign(s=d[cols].values@wa),"s"))
