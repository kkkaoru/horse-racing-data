"""秋華賞 cohort: out-of-time general (all JRA) and graded-turf rankers, one fit per test year.

For test year Y every model trains only on races before Y-01-01 (the 2026-10-18 field uses races
before 2026-10-18), so cohort CV scores no longer leak like the 2013-2025 sfeat/sfeat2 models do.
Writes shuka_v5_scores.parquet with gen_h / dom_h per cohort runner.
"""
import json
import sys
import lightgbm as lgb
import pandas as pd

W = "/Users/kkk4oru/models/clef-work/data"
FEATS = json.load(open(f"{W}/jra_sfeat2_feats.json"))
PARAMS = dict(objective="lambdarank", learning_rate=0.05, num_leaves=63, min_data_in_leaf=200, feature_fraction=0.7,
              bagging_fraction=0.8, bagging_freq=1, lambdarank_truncation_level=10, verbose=-1, num_threads=12, seed=7)
DOM_PARAMS = {**PARAMS, "num_leaves": 15, "min_data_in_leaf": 50}
# Optional: shuka_v5_scores.py <rounds> <leaves> <suffix> for a stronger general-model variant.
ROUNDS, DOM_ROUNDS = (int(sys.argv[1]), 150) if len(sys.argv) > 1 else (94, 150)
if len(sys.argv) > 2:
    PARAMS["num_leaves"] = int(sys.argv[2])
SUFFIX = sys.argv[3] if len(sys.argv) > 3 else ""
RACE_DAY = pd.Timestamp("2026-10-18")

frame = pd.read_parquet(f"{W}/jra_frame2.parquet")
cohort = pd.read_parquet(f"{W}/shuka_cohort.parquet", columns=["race_id", "umaban", "year", "d"])
frame = frame[(frame.groupby("race_id")["valid"].transform("sum") > 0) | frame.race_id.isin(cohort.race_id)]
graded = frame.surface.eq("T") & frame.grade.isin(["A", "B", "C", "L"])

def fit(rows: pd.DataFrame, params: dict, rounds: int) -> lgb.Booster:
    rows = rows[rows.groupby("race_id")["valid"].transform("sum") > 0]
    return lgb.train(params, lgb.Dataset(rows[FEATS], rows["label"], group=rows.groupby("race_id", sort=False).size().values), rounds)

out = []
for year in sorted(y for y in cohort.year.unique() if y >= 2014):
    cutoff = pd.Timestamp(f"{year}-01-01")
    before = frame[frame.d < cutoff]
    gen, dom = fit(before, PARAMS, ROUNDS), fit(before[graded.loc[before.index]], DOM_PARAMS, DOM_ROUNDS)
    te = frame[frame.race_id.isin(cohort[cohort.year == year].race_id)]
    out.append(te[["race_id", "umaban"]].assign(gen_h=gen.predict(te[FEATS]), dom_h=dom.predict(te[FEATS])))
    print(year, len(before), len(te), flush=True)
# Race-day field: train on everything before 2026-10-18 for the served score.
before = frame[frame.d < RACE_DAY]
gen, dom = fit(before, PARAMS, ROUNDS), fit(before[graded.loc[before.index]], DOM_PARAMS, DOM_ROUNDS)
te = frame[frame.race_id == "202610180811"]
day = te[["race_id", "umaban"]].assign(gen_h=gen.predict(te[FEATS]), dom_h=dom.predict(te[FEATS]))
scores = pd.concat([pd.concat(out)[lambda x: x.race_id != "202610180811"], day])
scores.to_parquet(f"{W}/shuka_v5_scores{SUFFIX}.parquet")
print("rows", len(scores), "race-day", len(day))
