import pandas as pd, numpy as np, lightgbm as lgb, json, duckdb, sys
W="/Users/kkk4oru/models/clef-work/data"
out = []
for cat in sys.argv[1:]:
    df = pd.read_parquet(f"{W}/{cat}_frame.parquet"); df = df[df.d >= pd.Timestamp("2026-01-01")].copy()
    feats = json.load(open(f"{W}/{cat}_sfeat_feats.json"))
    if feats is None: continue
    m = lgb.Booster(model_file=f"{W}/{cat}_sfeat.txt"); df["sfeat"] = m.predict(df[feats], num_threads=10)
    con = duckdb.connect(f"{W}/keiba.duckdb", read_only=True)
    bs = con.execute(f"select race_id, umaban, brank from {cat}_basescore").df()
    df = df.merge(bs, on=["race_id","umaban"], how="left")
    df["frank"] = df.groupby("race_id")["sfeat"].rank(ascending=False, method="first")
    df["career_raw"] = np.expm1(df["career_n"]).round()
    for c in [c for c in df.columns if c.endswith("_n")]: pass
    df["cat"] = cat
    df.to_parquet(f"{W}/{cat}_2026_scored.parquet")
    races = df[df.brank.notna()].groupby("race_id").agg(d=("d","first")).reset_index()
    rng = np.random.default_rng(7)
    h1 = races[races.d < pd.Timestamp("2026-07-01")].sample(frac=1, random_state=7)
    h2 = races[races.d >= pd.Timestamp("2026-07-01")].sample(frac=1, random_state=8)
    # interleave H2/H1 so partial runs cover both halves
    seq = []
    for a, b in zip(list(h2.race_id), list(h1.race_id)): seq += [a, b]
    out += [(cat, r) for r in seq]
json.dump(out, open(f"{W}/clef_queue_{"_".join(sys.argv[1:])}.json","w")); print(len(out))
