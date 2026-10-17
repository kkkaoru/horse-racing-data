import lightgbm as lgb
import pandas as pd, numpy as np, json, duckdb
W="/Users/kkk4oru/models/clef-work/data"; cat="nar"
b = pd.read_parquet(f"{W}/{cat}_frame2.parquet"); b = b[b.d >= pd.Timestamp("2026-01-01")].copy()
b["s2"] = lgb.Booster(model_file=f"{W}/{cat}_sfeat2.txt").predict(b[json.load(open(f"{W}/{cat}_sfeat2_feats.json"))])
bs = duckdb.connect(f"{W}/keiba.duckdb", read_only=True).execute(f"select race_id, umaban, brank from {cat}_basescore").df()
d = b.merge(bs, on=["race_id","umaban"]); d = d[d.groupby("race_id")["valid"].transform("sum")>0].copy()
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1); g = d.groupby("race_id")
d["bz"] = g["brank"].transform(lambda s: z(-s.astype(float))); d["z2"] = g["s2"].transform(z)
d["venue"] = d.race_id.str[8:10]
def per_race(x, a):
    x = x.assign(s=x.bz + a*x.z2); out=[]
    for rid, r in x.groupby("race_id"):
        r=r.sort_values("s", ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(out,float)
AL = [0, 0.25, 0.5, 1.0]
h1 = d[d.d < pd.Timestamp("2026-07-01")]; h2 = d[d.d >= pd.Timestamp("2026-07-01")]
chosen = {}; base2=[]; new2=[]
for v, hv in h1.groupby("venue"):
    sc = {a: per_race(hv, a) for a in AL}
    best = max(AL, key=lambda a: sc[a][:,0].mean()+sc[a][:,2].mean())
    gain = (sc[best][:,0].mean()+sc[best][:,2].mean()) - (sc[0][:,0].mean()+sc[0][:,2].mean())
    chosen[v] = best if (gain > 0.02 and len(sc[0]) >= 150) else 0
    t = h2[h2.venue==v]
    if len(t):
        base2.append(per_race(t, 0)); new2.append(per_race(t, chosen[v]))
        print(v, len(sc[0]), "chosen", chosen[v], "h1 gain", round(gain,3), "h2 base", per_race(t,0).mean(0).round(3), "h2 new", per_race(t,chosen[v]).mean(0).round(3))
print("H2 overall base", np.vstack(base2).mean(0).round(4), "per-venue", np.vstack(new2).mean(0).round(4), len(np.vstack(base2)))
json.dump(chosen, open(f"{W}/nar_venue_alpha.json","w"))
