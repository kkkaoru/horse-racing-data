import json, pandas as pd, numpy as np, sys, torch
W="/Users/kkk4oru/models/clef-work/data"
cat, frame = sys.argv[1], sys.argv[2]; srcs = sys.argv[3:]
f = pd.read_parquet(f"{W}/{frame}")
fr = []
for src in srcs:
    recs = [r for r in (json.loads(l) for l in open(src)) if "answers" in r]
    fr.append(pd.DataFrame([dict(race_id=r["race_id"], umaban=int(k), **v) for r in recs for k, v in r["answers"].items()]))
cnt = pd.concat(fr).groupby("race_id").umaban.count()
c = pd.concat(fr).groupby(["race_id","umaban"], as_index=False).mean(numeric_only=True)
if len(srcs) > 1:  # keep races answered by every source
    full = pd.concat([x.groupby("race_id").size() for x in fr], axis=1).dropna().index
    c = c[c.race_id.isin(full)]
d = f.merge(c, on=["race_id","umaban"]).copy()
d = d[d.groupby("race_id")["fin"].transform(lambda s: s.between(1,5).sum()) >= 5]
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1); eps=1e-4; g = d.groupby("race_id")
d["bz"] = g["brank"].transform(lambda s: z(-s.astype(float))); d["fz"] = g["general"].transform(z)
lg = lambda s: np.log(s.clip(eps,1-eps)/(1-s.clip(eps,1-eps)))
for k in ["clef_win","clef_second"]: d[k+"_z"] = g[k].transform(lambda s: z(np.log(s.clip(eps,1))))
for k in ["clef_top3","clef_top5"]: d[k+"_z"] = g[k].transform(lambda s: z(lg(s)))
d["month"] = d.d.dt.month
CL = ["clef_win_z","clef_second_z","clef_top3_z","clef_top5_z"]
def prep(x, cols):
    out = []
    for _, r in x.groupby("race_id"):
        out.append((torch.tensor(r[cols].values, dtype=torch.float32), list(np.argsort(np.where(r.fin.between(1,18), r.fin, 99))[:5])))
    return out
def pl_loss(data, w):
    loss = 0
    for x, order in data:
        s = x @ w; mask = torch.ones(len(s), dtype=torch.bool)
        for j in order:
            loss = loss - (s[j] - torch.logsumexp(s[mask], 0)); mask = mask.clone(); mask[j] = False
    return loss / len(data)
def fit(data, n, l2=0.01):
    w = torch.zeros(n, requires_grad=True); opt = torch.optim.LBFGS([w], max_iter=60)
    def cl():
        opt.zero_grad(); l = pl_loss(data, w) + l2*(w**2).sum(); l.backward(); return l
    opt.step(cl); return w.detach()
res = {}
for nm, cols in [("b+f", ["bz","fz"]), ("b+f+clef", ["bz","fz"]+CL), ("b+f+top3/5", ["bz","fz","clef_top3_z","clef_top5_z"])]:
    lls = []; n = 0
    for m in sorted(d.month.unique()):
        if m < 3: continue
        tr, te = prep(d[d.month < m], cols), prep(d[d.month == m], cols)
        w = fit(tr, len(cols)); lls.append(float(pl_loss(te, w)) * len(te)); n += len(te)
    res[nm] = sum(lls)/n
    print(cat, nm, "held-out top5 PL NLL per race", round(res[nm], 4), "races", n, "last w", fit(prep(d, cols), len(cols)).numpy().round(3), flush=True)
