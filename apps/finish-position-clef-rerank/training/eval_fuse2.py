import duckdb, numpy as np, pandas as pd, lightgbm as lgb, sys, json
cat = sys.argv[1]
W = "/Users/kkk4oru/models/clef-work/data"
df = pd.read_parquet(f"{W}/{cat}_frame2.parquet")
df = df[df.d >= pd.Timestamp("2026-01-01")].copy()
FEATS = json.load(open(f"{W}/{cat}_sfeat2_feats.json"))
m = lgb.Booster(model_file=f"{W}/{cat}_sfeat2.txt")
df["sfeat"] = m.predict(df[FEATS], num_threads=10)
con = duckdb.connect(f"{W}/keiba.duckdb", read_only=True)
bs = con.execute(f"select race_id, umaban, score, brank, bmv from {cat}_basescore").df()
# 10/17 served predictions as base for that day
p17 = con.execute(f"""select kaisai_nen||kaisai_tsukihi||keibajo_code||race_bango race_id, umaban::INT umaban, predicted_score score, predicted_rank::INT brank, model_version bmv, prediction_generated_at
   from preds_1017_1018 where kaisai_tsukihi='1017'""").df() if True else None
print(p17.columns.tolist()[:20]) if len(p17)==0 else None
df = df.merge(bs, on=["race_id","umaban"], how="left")
df = df[df.groupby("race_id")["valid"].transform("sum")>0]
def z(s, g):
    m_ = g.transform("mean"); sd = g.transform("std").replace(0,np.nan); return ((s-m_)/sd).fillna(0)
df["n_in"] = df.groupby("race_id")["umaban"].transform("count")
df["base_r"] = -(df["brank"]-1)/(df["n_in"]-1).clip(lower=1)
df["base_z"] = z(df["base_r"], df.groupby("race_id")["base_r"])
df["sfeat_z"] = z(df["sfeat"], df.groupby("race_id")["sfeat"])
def metrics(d, col):
    out = []
    for rid, r in d.groupby("race_id"):
        if r[col].isna().all() or r["valid"].sum()==0: continue
        r = r.sort_values(col, ascending=False)
        top = r.iloc[0]; p3 = set(r.umaban.iloc[:3]); a3 = set(r[r.fin.between(1,3)].umaban)
        out.append((top.fin==1, top.fin<=3 if top.fin>0 else False, p3==a3, len(p3&a3)/3))
    a = np.array(out, dtype=float); return len(a), a.mean(0).round(4)
has = df[df.brank.notna()]
h1 = has[has.d < pd.Timestamp("2026-07-01")]; h2 = has[has.d >= pd.Timestamp("2026-07-01")]
print("races h1/h2", h1.race_id.nunique(), h2.race_id.nunique())
print("cols: n, [win1, top1-in-3, top3box, top3 overlap]")
for nm, d in [("H1", h1), ("H2", h2)]:
    print(nm, "base", metrics(d, "base_z")); print(nm, "sfeat", metrics(d, "sfeat_z"))
best = None
for a in [0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0]:
    h1 = h1.assign(f=h1.base_z + a*h1.sfeat_z); r = metrics(h1, "f")
    print("H1 alpha", a, r)
    sc = r[1][0] + r[1][3]
    if best is None or sc > best[0]: best = (sc, a)
a = best[1]; h2 = h2.assign(f=h2.base_z + a*h2.sfeat_z)
print("chosen alpha", a, "H2 fused", metrics(h2, "f"))
json.dump({"alpha": a}, open(f"{W}/{cat}_fuse2.json","w"))
