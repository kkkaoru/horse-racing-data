import pandas as pd, numpy as np, lightgbm as lgb, json, itertools
W="/Users/kkk4oru/models/clef-work/data"
import duckdb
df = pd.read_parquet(f"{W}/shuka_cohort.parquet")
rn = duckdb.connect(f"{W}/keiba.duckdb", read_only=True).execute("select distinct race_id, race_name from jra_base").df()
df = df.merge(rn, on="race_id", how="left")
FEATS = json.load(open(f"{W}/jra_sfeat_feats.json"))
EX = ["g_best","g_runs","g1_best","g1_runs","g_wins"]
def topk(d, col):
    res=[]
    for rid, r in d.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        res.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(res,float)
def run(rounds, feats, lr, leaves, l2):
    params = dict(objective="lambdarank", learning_rate=lr, num_leaves=leaves, min_data_in_leaf=30, feature_fraction=0.6, bagging_fraction=0.8, bagging_freq=1, lambda_l2=l2, verbose=-1, num_threads=10, seed=1)
    allg, alls = [], []
    shg, shs = [], []
    for yr in range(2017, 2027):
        tr = df[df.year < yr]; te = df[(df.year == yr)]
        te = te[te.groupby("race_id")["valid"].transform("sum")>0]
        if te.empty: continue
        ds = lgb.Dataset(tr[feats], tr.label, group=tr.groupby("race_id", sort=False).size().values, init_score=tr.gen.values)
        m = lgb.train(params, ds, rounds)
        te = te.assign(spec=m.predict(te[feats]) + te.gen.values)
        allg.append(topk(te,"gen")); alls.append(topk(te,"spec"))
        s = te[te.race_name.str.contains("秋華賞", na=False)]
        if len(s): shg.append(topk(s,"gen")); shs.append(topk(s,"spec"))
    g=np.vstack(allg); s=np.vstack(alls)
    return g.mean(0).round(3), s.mean(0).round(3), len(g), np.vstack(shg).mean(0).round(3), np.vstack(shs).mean(0).round(3)
for rounds, fs, lr, lv, l2 in [(50,"ex",0.05,7,10),(100,"ex",0.05,7,10),(200,"ex",0.03,7,20),(100,"all",0.05,7,10),(200,"all",0.03,15,20),(400,"all",0.02,7,50)]:
    feats = EX + ["last1","last2","last3","horse_top3","horse_win","jockey_top3","sire_x_damsire_top3","sire_x_damsire_course_top3","sire_course_top3","damsire_course_top3","siresire_x_damsire_top3","days_since","futan","wakuban","umaban","field"] if fs=="ex" else FEATS+EX
    print(rounds, fs, lr, lv, l2, run(rounds, feats, lr, lv, l2), flush=True)
