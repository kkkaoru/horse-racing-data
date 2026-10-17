import json, pandas as pd, numpy as np, itertools, sys
W="/Users/kkk4oru/models/clef-work/data"
cat, src = sys.argv[1], sys.argv[2]; FW = 2.0
f = pd.read_parquet(f"{W}/{cat}_2026_scored.parquet")
recs = [r for r in (json.loads(l) for l in open(src)) if "answers" in r]
c = pd.DataFrame([(r["race_id"], int(k), v["clef_win"], v["clef_top3"], v["clef_pair"]) for r in recs for k, v in r["answers"].items()], columns=["race_id","umaban","cw","ct","cp"])
d = f[f.race_id.isin(c.race_id) & f.brank.notna()].copy()
d = d[d.groupby("race_id")["valid"].transform("sum")>0]
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1)
g = d.groupby("race_id"); d["fused"] = g["brank"].transform(lambda s: z(-s.astype(float))) + FW*g["sfeat"].transform(z)
d = d.merge(c, on=["race_id","umaban"], how="left")
eps=1e-4
cand = d[d.cp.notna()].copy(); gc = cand.groupby("race_id")
cand["fz"] = gc["fused"].transform(z); cand["pz"] = gc["cp"].transform(z)
cand["wz"] = gc["cw"].transform(lambda s: z(np.log(s.clip(eps,1)))); cand["tz"] = gc["ct"].transform(lambda s: z(np.log(s.clip(eps,1-eps)/(1-s.clip(eps,1-eps)))))
def met(dd, cc, w):
    out=[]
    cc = cc.assign(s=cc.fz + w[0]*cc.pz + w[1]*cc.wz + w[2]*cc.tz)
    for rid, r in dd.groupby("race_id"):
        top = cc[cc.race_id==rid].sort_values("s", ascending=False).umaban.tolist()
        rest = r[~r.umaban.isin(top)].sort_values("fused", ascending=False).umaban.tolist()
        order = top + rest; fin = dict(zip(r.umaban, r.fin))
        a3 = {u for u in r.umaban if 1 <= fin[u] <= 3}
        out.append((fin[order[0]]==1, 1 <= fin[order[0]] <= 3, len(a3 & set(order[:3]))/3))
    return np.array(out,float).mean(0).round(4)
h1r = set(d[d.d < pd.Timestamp("2026-07-01")].race_id);
D1, D2 = d[d.race_id.isin(h1r)], d[~d.race_id.isin(h1r)]; C1, C2 = cand[cand.race_id.isin(h1r)], cand[~cand.race_id.isin(h1r)]
print("races", d.race_id.nunique(), "h1", D1.race_id.nunique(), "h2", D2.race_id.nunique())
print("fused only all", met(d, cand, (0,0,0)), "h2", met(D2, C2, (0,0,0)))
for nm, w in [("pair",(1,0,0)),("win",(0,1,0)),("top3",(0,0,1))]: print(nm, "alone-ish all", met(d,cand,(w[0]*5,w[1]*5,w[2]*5)))
grid = list(itertools.product([0,0.25,0.5,1],[0,0.25,0.5,1],[0,0.25,0.5]))
r1 = sorted(((m[0]+m[2], w, m) for w in grid for m in [met(D1,C1,w)]), reverse=True)[:5]
for sc, w, m in r1: print("h1", w, m, "-> h2", met(D2,C2,w))
ra = sorted(((m[0]+m[2], w, m) for w in grid for m in [met(d,cand,w)]), reverse=True)[:4]
for sc, w, m in ra: print("all", w, m)
