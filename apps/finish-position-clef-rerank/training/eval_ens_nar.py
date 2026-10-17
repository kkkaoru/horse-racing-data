import pandas as pd, numpy as np, lightgbm as lgb, json, duckdb, sys
W="/Users/kkk4oru/models/clef-work/data"; cat=sys.argv[1]
a = pd.read_parquet(f"{W}/{cat}_frame.parquet"); a = a[a.d >= pd.Timestamp("2026-01-01")].copy()
b = pd.read_parquet(f"{W}/{cat}_frame2.parquet"); b = b[b.d >= pd.Timestamp("2026-01-01")].copy()
a["s1"] = lgb.Booster(model_file=f"{W}/{cat}_sfeat.txt").predict(a[json.load(open(f"{W}/{cat}_sfeat_feats.json"))])
b["s2"] = lgb.Booster(model_file=f"{W}/{cat}_sfeat2.txt").predict(b[json.load(open(f"{W}/{cat}_sfeat2_feats.json"))])
d = a.merge(b[["race_id","umaban","s2"]], on=["race_id","umaban"])
bs = duckdb.connect(f"{W}/keiba.duckdb", read_only=True).execute(f"select race_id, umaban, brank from {cat}_basescore").df()
d = d.merge(bs, on=["race_id","umaban"]); d = d[d.groupby("race_id")["valid"].transform("sum")>0]
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1); g = d.groupby("race_id")
d["bz"] = g["brank"].transform(lambda s: z(-s.astype(float))); d["z1"] = g["s1"].transform(z); d["z2"] = g["s2"].transform(z)
def met(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(out,float).mean(0).round(4)
for nm, h in [("H1", d[d.d < pd.Timestamp("2026-07-01")]), ("H2", d[d.d >= pd.Timestamp("2026-07-01")]), ("ALL", d)]:
    print(nm, h.race_id.nunique(), "base", met(h,"bz"), *[(a, met(h.assign(s=h.bz+a*h.z2),"s")) for a in (0.25,0.5,0.75,1.0)], ("s1+s2 .5", met(h.assign(s=h.bz+0.5*(h.z1+h.z2)),"s")))
