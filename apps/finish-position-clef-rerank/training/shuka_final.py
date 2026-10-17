import pandas as pd, numpy as np, json, sys
sys.argv = ["x"]
src = open("/Users/kkk4oru/models/clef-work/shuka3.py").read()
head = src.split("for l2 in")[0]
exec(head)  # builds df (valid cohort races only), Z, fit, met
W="/Users/kkk4oru/models/clef-work/data"
w = fit(df, 3.0)
json.dump({"cols": COLS, "weights": dict(zip(Z, [float(x) for x in w])), "l2": 3.0}, open(f"{W}/shuka_linear.json","w"), ensure_ascii=False)
full = pd.read_parquet(f"{W}/shuka_cohort.parquet")
for c in ["g_best","g1_best","last1"]: full[c] = full[c].fillna(1.0)
gg = full.groupby("race_id")
for c, s in COLS.items():
    full["z_"+c] = gg[c].transform(lambda x: (x-x.mean())/(x.std() if x.std()>0 else 1)).fillna(0)*s
full["spec"] = full[Z].values @ w
full["brank"] = full.groupby("race_id")["gen"].rank(ascending=False, method="first")
full["frank"] = full.groupby("race_id")["spec"].rank(ascending=False, method="first")
full["career_raw"] = np.expm1(full["career_n"]).round()
full["sfeat"] = full["gen"]
full.to_parquet(f"{W}/shuka_scored.parquet")
t = full[full.race_id=="202610180811"].sort_values("spec", ascending=False)
print(t[["umaban","spec","gen","frank"]].to_string())
races = full[full.groupby("race_id")["valid"].transform("sum")>0].race_id.unique().tolist()
json.dump([["shuka", r] for r in sorted(races, reverse=True)] + [["shuka","202610180811"]], open(f"{W}/clef_queue_shuka.json","w"))
print("cohort races", len(races))
