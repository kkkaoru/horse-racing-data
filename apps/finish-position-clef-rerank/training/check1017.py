import lightgbm as lgb
import json, sys, urllib.request, pandas as pd, numpy as np, duckdb
sys.path.insert(0, "/Users/kkk4oru/models/clef-work")
from clef_state import build_request, parse_answers
W="/Users/kkk4oru/models/clef-work/data"
env = dict(l.strip().split("=", 1) for l in open("/Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data/apps/pc-keiba-viewer/.env.local") if "=" in l and not l.startswith("#"))
H = {"CF-Access-Client-Id": env["PC_KEIBA_ACCESS_CLIENT_ID"].strip('"'), "CF-Access-Client-Secret": env["PC_KEIBA_ACCESS_CLIENT_SECRET"].strip('"'), "Content-Type": "application/json", "User-Agent": "pc-keiba-clef-eval/1.0"}
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1)
a = pd.read_parquet(f"{W}/jra_frame.parquet"); a = a[a.race_id.str.startswith("20261017")].copy()
b = pd.read_parquet(f"{W}/jra_frame2.parquet"); b = b[b.race_id.str.startswith("20261017")].copy()
a["sfeat"] = lgb.Booster(model_file=f"{W}/jra_sfeat.txt").predict(a[json.load(open(f"{W}/jra_sfeat_feats.json"))])
b["s2"] = lgb.Booster(model_file=f"{W}/jra_sfeat2.txt").predict(b[json.load(open(f"{W}/jra_sfeat2_feats.json"))])
d = a.merge(b[["race_id","umaban","s2"]], on=["race_id","umaban"])
con = duckdb.connect(f"{W}/keiba.duckdb", read_only=True)
p = con.execute("""select kaisai_nen||kaisai_tsukihi||keibajo_code||race_bango race_id, umaban::INT umaban, predicted_rank::INT brank, model_version mv, prediction_generated_at AS gen_at
  from preds_1017_1018 where kaisai_tsukihi='1017' and keibajo_code in ('04','05','08')""").df()
last = p.groupby(["race_id","mv"]).gen_at.max().reset_index().sort_values("gen_at").groupby("race_id").tail(1)
p = p.merge(last[["race_id","mv"]], on=["race_id","mv"])
d = d.merge(p[["race_id","umaban","brank"]], on=["race_id","umaban"])
d = d[d.groupby("race_id")["valid"].transform("sum")>0].copy()
g = d.groupby("race_id"); d["general"] = g["sfeat"].transform(z) + g["s2"].transform(z)
d["frank"] = g["general"].rank(ascending=False, method="first"); d["career_raw"] = np.expm1(d["career_n"]).round()
rows=[]
for rid, gg in d.groupby("race_id"):
    r0 = gg.iloc[0]
    req = build_request({"surface": r0.surface, "dist": r0.dist, "going": r0.going, "field": len(gg), "grade_c": r0.grade_c, "joken": r0.joken}, gg.to_dict("records"))
    body = json.loads(urllib.request.urlopen(urllib.request.Request("https://pc-keiba-viewer.kkk4oru.com/api/clef-rerank/probe", data=json.dumps(req, default=lambda v: None if v != v else (int(v) if float(v).is_integer() else float(v))).encode(), headers=H, method="POST"), timeout=60).read())
    ans = parse_answers(body["answers"], gg.umaban.tolist())
    rows += [(rid, u, v["clef_top3"]) for u, v in ans.items()]
d = d.merge(pd.DataFrame(rows, columns=["race_id","umaban","ct"]), on=["race_id","umaban"])
g = d.groupby("race_id"); eps=1e-4
d["bz"] = g["brank"].transform(lambda s: z(-(s.astype(float)))); d["fz"] = g["general"].transform(z)
d["tz"] = g["ct"].transform(lambda s: z(np.log(s.clip(eps,1-eps)/(1-s.clip(eps,1-eps)))))
def met(x, col):
    out=[]
    for rid, r in x.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return len(out), np.array(out,float).mean(0).round(3)
print("10/17 JRA served", met(d,"bz"))
print("10/17 JRA base+feature", met(d.assign(s=d.bz+1.9*d.fz),"s"))
print("10/17 JRA r1 (base+feature+clef)", met(d.assign(s=d.bz+1.9*d.fz+0.2*d.tz),"s"))
d.to_parquet(f"{W}/check1017.parquet")
