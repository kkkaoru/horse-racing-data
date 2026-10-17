import duckdb, pandas as pd, numpy as np, lightgbm as lgb, json
W="/Users/kkk4oru/models/clef-work/data"
con = duckdb.connect(f"{W}/keiba.duckdb", read_only=True)
coh = con.execute("""
 WITH r AS (SELECT race_id, any_value(grade) grade, any_value(surface) surface, any_value(dist) dist, any_value(race_name) race_name, any_value(d) d,
     max(age) mx, min(age) mn, bool_and(sex='2') fem, count(*) n FROM jra_base GROUP BY race_id)
 SELECT * FROM r WHERE surface='T' AND grade IN ('A','B','C','L') AND mx=3 AND mn=3 AND fem AND dist BETWEEN 1600 AND 2400 AND d >= DATE '2012-01-01'
""").df()
print(len(coh), coh.race_name.str.strip().value_counts().head(15).to_dict())
df = pd.read_parquet(f"{W}/jra_frame.parquet")
df = df[df.race_id.isin(coh.race_id)].copy()
# extra cohort features: best graded finish and G1 runs before this race
b = con.execute("select horse, d, grade, fin, field, valid from jra_base where grade in ('A','B','C') and valid=1").df()
b["fn"] = (b.fin-1)/(b.field-1).clip(lower=1)
def extras(row):
    h = b[(b.horse==row.horse) & (b.d < row.d)]
    g1 = h[h.grade=="A"]
    return pd.Series({"g_best": h.fn.min() if len(h) else np.nan, "g_runs": len(h), "g1_best": g1.fn.min() if len(g1) else np.nan, "g1_runs": len(g1), "g_wins": int((h.fin==1).sum())})
hb = con.execute("select race_id, umaban, horse from jra_base").df()
df = df.merge(hb, on=["race_id","umaban"], how="left")
ex = df[["horse","d"]].apply(extras, axis=1)
df = pd.concat([df.reset_index(drop=True), ex.reset_index(drop=True)], axis=1)
FEATS = json.load(open(f"{W}/jra_sfeat_feats.json"))
gen = lgb.Booster(model_file=f"{W}/jra_sfeat.txt")
df["gen"] = gen.predict(df[FEATS], num_threads=10)
SF = FEATS + ["g_best","g_runs","g1_best","g1_runs","g_wins","gen"]
df["year"] = df.d.dt.year
df.to_parquet(f"{W}/shuka_cohort.parquet")
params = dict(objective="lambdarank", learning_rate=0.03, num_leaves=15, min_data_in_leaf=40, feature_fraction=0.5, bagging_fraction=0.8, bagging_freq=1, lambda_l2=10, verbose=-1, num_threads=10)
def topk(d, col):
    res=[]
    for rid, r in d.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        res.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(res,float).mean(0).round(3), len(res)
rows=[]
for yr in range(2019, 2027):
    tr = df[df.year < yr]; te = df[(df.year == yr) & df.valid.astype(bool)]
    if te.empty: continue
    te = te[te.groupby("race_id")["valid"].transform("sum")>0]
    m = lgb.train(params, lgb.Dataset(tr[SF], tr.label, group=tr.groupby("race_id", sort=False).size().values), 300)
    te = te.assign(spec=m.predict(te[SF]))
    for c in ["gen","spec"]:
        te[c+"_z"] = te.groupby("race_id")[c].transform(lambda s: (s-s.mean())/(s.std() or 1))
    te["blend"] = te.gen_z + te.spec_z
    sh = te[te.race_name.str.contains("秋華賞", na=False)] if "race_name" in te else None
    rows.append((yr, topk(te,"gen"), topk(te,"spec"), topk(te,"blend")))
    print(yr, "gen", topk(te,"gen"), "spec", topk(te,"spec"), "blend", topk(te,"blend"), flush=True)
