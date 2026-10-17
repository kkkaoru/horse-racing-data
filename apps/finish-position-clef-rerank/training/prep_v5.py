import lightgbm as lgb
import pandas as pd, numpy as np, json, duckdb, sys
W="/Users/kkk4oru/models/clef-work/data"
z = lambda s: (s-s.mean())/(s.std() if s.std()>0 else 1)
con = duckdb.connect(f"{W}/keiba.duckdb", read_only=True)
for cat in sys.argv[1:]:
    b = pd.read_parquet(f"{W}/{cat}_frame2.parquet"); b = b[b.d >= pd.Timestamp("2026-01-01")].copy()
    b["s2"] = lgb.Booster(model_file=f"{W}/{cat}_sfeat2.txt").predict(b[json.load(open(f"{W}/{cat}_sfeat2_feats.json"))], num_threads=8)
    try:
        a = pd.read_parquet(f"{W}/{cat}_frame.parquet", columns=None); a = a[a.d >= pd.Timestamp("2026-01-01")]
        a = a.assign(s1=lgb.Booster(model_file=f"{W}/{cat}_sfeat.txt").predict(a[json.load(open(f"{W}/{cat}_sfeat_feats.json"))], num_threads=8))[["race_id","umaban","s1"]]
        b = b.merge(a, on=["race_id","umaban"], how="left")
    except FileNotFoundError:
        b["s1"] = b["s2"]
    bs = con.execute(f"select race_id, umaban, brank from {cat}_basescore").df()
    b = b.merge(bs, on=["race_id","umaban"], how="inner")
    g = b.groupby("race_id")
    b["general"] = g["s1"].transform(z) + g["s2"].transform(z)
    b["frank"] = g["general"].rank(ascending=False, method="first")
    b["career_raw"] = np.expm1(b["career_n"]).round()
    b["sfeat"] = b["general"]
    b = b[b.groupby("race_id")["valid"].transform("sum")>0]
    b.to_parquet(f"{W}/{cat}_v5.parquet")
    races = b.race_id.unique().tolist()
    rng = np.random.default_rng(11); rng.shuffle(races)
    json.dump([[cat, r] for r in races], open(f"{W}/queue_v5_{cat}.json","w"))
    print(cat, len(races))
