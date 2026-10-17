import duckdb, pandas as pd, numpy as np, json, lightgbm as lgb
W="/Users/kkk4oru/models/clef-work/data"
con = duckdb.connect(f"{W}/keiba.duckdb", read_only=True)
p = con.execute("""select kaisai_nen||kaisai_tsukihi||keibajo_code||race_bango race_id, keibajo_code kj, umaban::INT umaban, model_version mv, predicted_rank::INT rk, prediction_generated_at AS gen_at
  from preds_1017_1018 where kaisai_tsukihi='1017'""").df()
res = con.execute("select race_id, umaban, fin from jra_base where race_id like '20261017%' union all select race_id, umaban, fin from nar_base where race_id like '20261017%'").df()
p = p.merge(res, on=["race_id","umaban"])
rows=[]
for (rid, mv), g in p.groupby(["race_id","mv"]):
    g=g.sort_values("rk"); a3=set(g[g.fin.between(1,3)].umaban); p3=set(g.umaban.iloc[:3])
    rows.append((rid, mv, g.gen_at.max(), g.iloc[0].fin==1, g.iloc[0].fin<=3, len(a3&p3)/3))
r = pd.DataFrame(rows, columns=["race_id","mv","gen_at","win1","top1in3","ov3"])
print(r.groupby("mv")[["win1","top1in3","ov3"]].agg(["mean","count"]).round(3).to_string())
# served = latest generated per race
srv = r.sort_values("gen_at").groupby("race_id").tail(1)
srv["cat"] = np.where(srv.race_id.str[8:10].isin(["04","05","08"]), "jra", "nar")
print(srv.groupby("cat")[["win1","top1in3","ov3"]].agg(["mean","count"]).round(3))
print(srv[srv.cat=="jra"].mv.value_counts())
# our fusion on 10/17 JRA, using served rank as base
f = pd.read_parquet(f"{W}/jra_frame.parquet"); f = f[f.race_id.str.startswith("20261017")].copy()
feats = json.load(open(f"{W}/jra_sfeat_feats.json")); f["sfeat"] = lgb.Booster(model_file=f"{W}/jra_sfeat.txt").predict(f[feats])
sv = p.merge(srv[["race_id","mv"]], on=["race_id","mv"])[["race_id","umaban","rk"]]
f = f.merge(sv, on=["race_id","umaban"], how="inner")
z = lambda s: (s-s.mean())/(s.std() or 1)
f["n"] = f.groupby("race_id").umaban.transform("count")
f["bz"] = f.groupby("race_id")["rk"].transform(lambda s: z(-s)); f["fz"] = f.groupby("race_id")["sfeat"].transform(z)
alpha = json.load(open(f"{W}/jra_fuse.json"))["alpha"]
for nm, col in [("served", "bz"), ("sfeat", "fz"), ("fused", None)]:
    if col is None: f["fu"] = f.bz + alpha*f.fz; col = "fu"
    out=[]
    for rid, g in f.groupby("race_id"):
        g=g.sort_values(col, ascending=False); a3=set(g[g.fin.between(1,3)].umaban); p3=set(g.umaban.iloc[:3])
        out.append((g.iloc[0].fin==1, g.iloc[0].fin<=3, len(a3&p3)/3))
    print("10/17 JRA", nm, len(out), np.array(out,float).mean(0).round(3))
