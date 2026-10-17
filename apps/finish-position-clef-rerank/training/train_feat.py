import duckdb, numpy as np, pandas as pd, lightgbm as lgb, sys, json, pickle
cat = sys.argv[1]
con = duckdb.connect("/Users/kkk4oru/models/clef-work/data/keiba.duckdb", read_only=True)
df = con.execute(f"""SELECT b.race_id, b.d, b.umaban, b.surface, b.dist, b.going, b.field, b.age, b.sex, b.futan, b.wakuban, b.fin, b.valid, b.win, b.top3, b.grade, b.joken, b.venue,
   f.* EXCLUDE (race_id, umaban, d), m.* EXCLUDE (race_id, umaban)
  FROM {cat}_base b JOIN {cat}_feat f USING (race_id, umaban) JOIN {cat}_form m USING (race_id, umaban)
  WHERE b.d >= DATE '2013-01-01'""").df()
df["surface_c"] = df["surface"].map({"T":0,"D":1,"J":2}).astype(float)
df["going_c"] = pd.to_numeric(df["going"], errors="coerce")
df["sex_c"] = pd.to_numeric(df["sex"], errors="coerce")
df["grade_c"] = df["grade"].map({"A":5,"B":4,"C":3,"D":2,"E":1,"L":2}).fillna(0)
df["joken_c"] = pd.to_numeric(df["joken"], errors="coerce")
df["venue_c"] = pd.to_numeric(df["venue"], errors="coerce")
num = [c for c in df.columns if c.endswith(("_win","_top3"))]
cnt = [c for c in df.columns if c.endswith("_n")]
for c in cnt: df[c] = np.log1p(df[c])
# within-race relative versions of rate features
g = df.groupby("race_id")
for c in num:
    m = g[c].transform("mean"); s = g[c].transform("std").replace(0, np.nan)
    df[c+"_rz"] = ((df[c]-m)/s).fillna(0)
FEATS = num + cnt + [c+"_rz" for c in num] + ["last1","last2","last3","days_since","last_time_sa","field","dist","surface_c","going_c","age","sex_c","futan","wakuban","umaban","grade_c","joken_c","venue_c"]
df["label"] = np.where(df["fin"].between(1,5), 6-df["fin"], 0).clip(0,5)
df = df.sort_values(["d","race_id","umaban"])
df.to_parquet(f"/Users/kkk4oru/models/clef-work/data/{cat}_frame.parquet")
df = df[df.groupby("race_id")["valid"].transform("sum")>0] if False else df
tr = df[(df.groupby("race_id")["valid"].transform("sum")>0) & (df.d < pd.Timestamp("2026-01-01"))]
params = dict(objective="lambdarank", learning_rate=0.05, num_leaves=63, min_data_in_leaf=200, feature_fraction=0.7, bagging_fraction=0.8, bagging_freq=1, lambdarank_truncation_level=10, verbose=-1, num_threads=10)
va = tr[tr.d >= pd.Timestamp("2025-07-01")]; tr2 = tr[tr.d < pd.Timestamp("2025-07-01")]
ds = lgb.Dataset(tr2[FEATS], tr2["label"], group=tr2.groupby("race_id", sort=False).size().values)
dv = lgb.Dataset(va[FEATS], va["label"], group=va.groupby("race_id", sort=False).size().values)
m = lgb.train(params, ds, 2000, valid_sets=[dv], callbacks=[lgb.early_stopping(100), lgb.log_evaluation(200)])
# refit on all pre-2026 with best iter
dall = lgb.Dataset(tr[FEATS], tr["label"], group=tr.groupby("race_id", sort=False).size().values)
m2 = lgb.train(params, dall, m.best_iteration)
m2.save_model(f"/Users/kkk4oru/models/clef-work/data/{cat}_sfeat.txt")
json.dump(FEATS, open(f"/Users/kkk4oru/models/clef-work/data/{cat}_sfeat_feats.json","w"))
imp = sorted(zip(m2.feature_importance("gain"), FEATS), reverse=True)[:30]
print([ (f, int(v)) for v,f in imp])
