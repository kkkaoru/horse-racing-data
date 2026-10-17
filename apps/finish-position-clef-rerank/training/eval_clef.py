import json, pandas as pd, numpy as np, itertools, sys
W="/Users/kkk4oru/models/clef-work/data"
cat = sys.argv[1]; src = sys.argv[2]
f = pd.read_parquet(f"{W}/{cat}_2026_scored.parquet")
recs = [r for r in (json.loads(l) for l in open(src)) if "answers" in r]
rows = [(r["race_id"], int(k), v["clef_win"], v["clef_top3"]) for r in recs for k, v in r["answers"].items()]
c = pd.DataFrame(rows, columns=["race_id","umaban","cw","ct"])
d = f.merge(c, on=["race_id","umaban"]).copy()
d = d[d.brank.notna() & (d.groupby("race_id")["valid"].transform("sum")>0)]
z = lambda s: (s-s.mean())/(s.std() if s.std() > 0 else 1)
g = d.groupby("race_id")
d["bz"] = g["brank"].transform(lambda s: z(-s)); d["fz"] = g["sfeat"].transform(z)
eps=1e-4
d["tz"] = g["ct"].transform(lambda s: z(np.log(s.clip(eps,1-eps)/(1-s.clip(eps,1-eps)))))
d["wz"] = g["cw"].transform(lambda s: z(np.log(s.clip(eps,1-eps))))
def met(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(out,float).mean(0).round(4)
h1 = d[d.d < pd.Timestamp("2026-07-01")]; h2 = d[d.d >= pd.Timestamp("2026-07-01")]
print("races", d.race_id.nunique(), "h1", h1.race_id.nunique(), "h2", h2.race_id.nunique())
for nm, col in [("base","bz"),("sfeat","fz"),("clef_top3","tz"),("clef_win","wz")]:
    print(nm, "all", met(d,col), "h2", met(h2,col))
grid = list(itertools.product([1.0],[0,0.5,1,2],[0,0.25,0.5,1,1.5],[0,0.25,0.5,1]))
def score(x, w):
    x = x.assign(s=w[0]*x.bz + w[1]*x.fz + w[2]*x.tz + w[3]*x.wz); m = met(x,"s"); return m[0]+m[2], m
res = sorted(((score(h1,w)[0], w) for w in grid), reverse=True)[:5]
for sc, w in res: print("h1 best", w, score(h1,w)[1], "-> h2", score(h2,w)[1])
noclef = sorted(((score(h1,w)[0], w) for w in grid if w[2]==0 and w[3]==0), reverse=True)[0][1]
print("no-clef best", noclef, "h2", score(h2,noclef)[1])
# fit on all for production
best_all = sorted(((score(d,w)[0], w) for w in grid), reverse=True)[:3]
for sc, w in best_all: print("all best", w, score(d,w)[1])
