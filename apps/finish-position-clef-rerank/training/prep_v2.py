import pandas as pd, duckdb, sys
W="/Users/kkk4oru/models/clef-work/data"
con = duckdb.connect(f"{W}/keiba.duckdb", read_only=True)
for cat in sys.argv[1:]:
    f = pd.read_parquet(f"{W}/{cat}_2026_scored.parquet")
    src = "jra" if cat == "shuka" else cat
    h = con.execute(f"select race_id, umaban, last5 from {src}_hist where race_id in (select distinct race_id from f)").df()
    f.merge(h, on=["race_id","umaban"], how="left").to_parquet(f"{W}/{cat}_2026_scored_v2.parquet")
    print(cat, len(f))
