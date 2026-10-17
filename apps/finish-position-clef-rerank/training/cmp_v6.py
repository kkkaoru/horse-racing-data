import json, pandas as pd, numpy as np
W="/Users/kkk4oru/models/clef-work/data"
f = pd.read_parquet(f"{W}/jra_v5.parquet")
def load(p):
    recs=[r for r in (json.loads(l) for l in open(p)) if "answers" in r]
    return pd.DataFrame([dict(race_id=r["race_id"], umaban=int(k), **v) for r in recs for k,v in r["answers"].items()])
a, b = load(f"{W}/v5_jra.jsonl"), load(f"{W}/v6_jra.jsonl")
common = set(a.race_id) & set(b.race_id)
def met(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        u = r.sort_values([col,"umaban"], ascending=[False,True]).umaban.tolist(); fin=dict(zip(r.umaban, r.fin))
        a5={k for k,v in fin.items() if 1<=v<=5}; a3={k for k,v in fin.items() if 1<=v<=3}
        out.append((fin[u[0]]==1, len(set(u[:3])&a3)/3, len(set(u[:5])&a5)/5))
    return np.array(out,float).mean(0).round(4)
for nm, c in [("v5", a), ("v6 names", b)]:
    d = f.merge(c[c.race_id.isin(common)], on=["race_id","umaban"])
    d = d[d.groupby("race_id")["fin"].transform(lambda s: s.between(1,5).sum())>=5]
    print(nm, d.race_id.nunique(), "win-choice", met(d,"clef_win"), "top3", met(d,"clef_top3"), "top5", met(d,"clef_top5"))
d = f.merge(a[a.race_id.isin(common)], on=["race_id","umaban"]); d=d[d.groupby("race_id")["fin"].transform(lambda s: s.between(1,5).sum())>=5]
d["nb"] = -d.brank; print("base same races", met(d,"nb"))
