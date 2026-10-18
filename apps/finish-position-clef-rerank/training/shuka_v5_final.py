"""Score the 2026-10-18 秋華賞 field with v4 = z(gen_h) + 0.5 z(dom_h) + 0.5 z(spec_h) + 0.5 z(pl_h).

Weights are the nested pick of shuka_v5_nested.py for 2024-2026. spec_h / pl_h are refit on every
cohort race with a result before 2026-10-18; gen_h / dom_h are the race-day models of shuka_v5_scores.py.
Writes shuka_v4_scored.parquet (race_id, umaban, v4) for make_bundles.py.
"""
import sys
import numpy as np
import pandas as pd

sys.argv = ["x"]
src = open("/Users/kkk4oru/models/clef-work/shuka3.py").read().split("for l2 in")[0]
exec(src)  # df (valid cohort races), COLS, Z, fit
import torch

def pl_fit(tr: pd.DataFrame, cols: list[str], l2: float) -> np.ndarray:
    """Plackett-Luce top-5 linear fit, same as shuka_v5_eval.py."""
    data = [(torch.tensor(r[cols].values, dtype=torch.float32), list(np.argsort(np.where(r.fin.between(1, 18), r.fin, 99))[:5]))
            for _, r in tr.groupby("race_id")]
    w = torch.zeros(len(cols), requires_grad=True)
    opt = torch.optim.LBFGS([w], max_iter=100)
    def closure():
        opt.zero_grad(); loss = 0
        for x, order in data:
            s = x @ w; mask = torch.ones(len(s), dtype=torch.bool)
            for j in order:
                loss = loss - (s[j] - torch.logsumexp(s[mask], 0)); mask = mask.clone(); mask[j] = False
        loss = loss / len(data) + l2 * (w ** 2).sum(); loss.backward(); return loss
    opt.step(closure)
    return w.detach().numpy()
W = "/Users/kkk4oru/models/clef-work/data"
RACE = "202610180811"
WEIGHTS = {"dom": 0.5, "spec": 0.5, "pl": 0.5}
zs = lambda s: (s - s.mean()) / (s.std() if s.std() > 0 else 1)
scores = pd.read_parquet(f"{W}/shuka_v5_scores.parquet")

full = pd.read_parquet(f"{W}/shuka_cohort.parquet")
for c in ["g_best", "g1_best", "last1"]:
    full[c] = full[c].fillna(1.0)
full = full.merge(scores, on=["race_id", "umaban"], how="inner")
gg = full.groupby("race_id")
for c, sign in COLS.items():
    full["z_" + c] = gg[c].transform(zs).fillna(0) * sign
full["z_gen"] = gg["gen_h"].transform(zs)
full["z_dom"] = gg["dom_h"].transform(zs)
train = full[(full.race_id != RACE) & (full.groupby("race_id")["valid"].transform("sum") > 0)]
train = train[train.groupby("race_id")["fin"].transform(lambda s: s.between(1, 5).sum()) >= 5]
w_spec, w_pl = fit(train, 3.0), pl_fit(train, Z + ["z_dom"], 0.3)
day = full[full.race_id == RACE].copy()
day["spec_h"] = zs(day[Z].values @ w_spec)
day["pl_h"] = zs(day[Z + ["z_dom"]].values @ w_pl)
day["v4"] = day["z_gen"] + WEIGHTS["dom"] * day["z_dom"] + WEIGHTS["spec"] * day["spec_h"] + WEIGHTS["pl"] * day["pl_h"]
assert len(day) > 0 and day["v4"].notna().all()
day[["race_id", "umaban", "v4"]].to_parquet(f"{W}/shuka_v4_scored.parquet")
print("train races", train.race_id.nunique())
print(day.sort_values("v4", ascending=False)[["umaban", "v4", "z_gen", "z_dom", "spec_h", "pl_h"]].round(2).to_string(index=False))
