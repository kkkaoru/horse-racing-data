"""秋華賞 cohort: compare out-of-time candidates on unordered top-1..5 (box_k and overlap ov_k).

Every score for test year Y comes from models trained before Y (shuka_v5_scores.py and the
specialist linear fit on cohort years < Y). Candidates are picked by nested CV on Y-3..Y-1.
"""
import itertools
import os
import json
import numpy as np
import pandas as pd
import torch

src = open("/Users/kkk4oru/models/clef-work/shuka3.py").read().split("for l2 in")[0]
exec(src)  # df (valid cohort races), COLS, Z, fit(tr, l2, top3w)
W = "/Users/kkk4oru/models/clef-work/data"
SUFFIX = os.environ.get("SHUKA_SCORES_SUFFIX", "")  # e.g. _g300 for the stronger general model
K = 5
df = df[df.groupby("race_id")["fin"].transform(lambda s: s.between(1, K).sum()) >= K].copy()
df = df.merge(pd.read_parquet(f"{W}/shuka_v5_scores{SUFFIX}.parquet"), on=["race_id", "umaban"], how="inner")
zs = lambda s: (s - s.mean()) / (s.std() if s.std() > 0 else 1)
g = df.groupby("race_id")
df["z_gen_leak"] = df["z_gen"]
df["z_gen"] = g["gen_h"].transform(zs)  # specialist features now use the out-of-time general score
df["z_dom"] = g["dom_h"].transform(zs)
ZD = Z + ["z_dom"]
FORM2 = ["ts_avg5", "ts_min5", "l3r_avg3", "c4_avg3", "best_cls_t3", "cls_margin", "sire_x_damsire_surface_top3", "sire_x_damsire_venue_top3"]
FORM2_SIGN = {"ts_avg5": 1, "ts_min5": -1, "l3r_avg3": -1, "c4_avg3": -1, "best_cls_t3": 1, "cls_margin": 1, "sire_x_damsire_surface_top3": 1, "sire_x_damsire_venue_top3": 1}
f2 = pd.read_parquet(f"{W}/jra_frame2.parquet", columns=["race_id", "umaban"] + [c for c in FORM2 if c not in df.columns])
df = df.merge(f2, on=["race_id", "umaban"], how="left")
for c in FORM2:
    df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)  # nullable Int32 would make object arrays
    df["z2_" + c] = df.groupby("race_id")[c].transform(zs).fillna(0) * FORM2_SIGN[c]
ZD2 = ZD + ["z2_" + c for c in FORM2]

def metrics(d: pd.DataFrame, col: str) -> np.ndarray:
    rows = []
    for _, r in d.groupby("race_id"):
        order = r.sort_values([col, "umaban"], ascending=[False, True]).umaban.tolist()
        fin = dict(zip(r.umaban, r.fin))
        rows.append([set(order[:k]) == {h for h, f in fin.items() if 1 <= f <= k} for k in range(1, K + 1)]
                    + [len(set(order[:k]) & {h for h, f in fin.items() if 1 <= f <= k}) / k for k in range(1, K + 1)])
    return np.array(rows, float)

def objective(m: np.ndarray) -> float:
    return m[:, K:].mean() + m[:, :K].mean()

def pl_fit(tr: pd.DataFrame, cols: list[str], l2: float) -> np.ndarray:
    data = []
    for _, r in tr.groupby("race_id"):
        data.append((torch.tensor(r[cols].values, dtype=torch.float32),
                     list(np.argsort(np.where(r.fin.between(1, 18), r.fin, 99))[:K])))
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

def year_scores(year: int) -> pd.DataFrame:
    """All candidate columns for cohort year `year`, trained on cohort years before it."""
    tr, te = df[df.year < year], df[df.year == year].copy()
    gz = te.groupby("race_id")
    te["spec_h"] = te[Z].values @ fit(tr, 3.0)
    te["spec_h"] = te.groupby("race_id")["spec_h"].transform(zs)
    te["v3_h"] = te.groupby("race_id")["spec_h"].transform(zs) + te["z_gen"]  # served shuka = z(spec)+z(sfeat2)
    te["pl_h"] = te[ZD].values @ pl_fit(tr, ZD, 0.3)
    te["pl2_h"] = te[ZD2].values @ pl_fit(tr, ZD2, 0.3)
    for a, b in itertools.product((0, 0.25, 0.5, 0.75, 1.0), repeat=2):
        te[f"blend_{a}_{b}"] = te["z_gen"] + a * te["z_dom"] + b * te["spec_h"]
    te["gen_h"] = te["z_gen"]; te["dom_h"] = te["z_dom"]
    return te

CANDIDATES = ["gen_h", "dom_h", "spec_h", "v3_h", "pl_h", "pl2_h"] + [f"blend_{a}_{b}" for a, b in itertools.product((0, 0.25, 0.5, 0.75, 1.0), repeat=2)]
YEARS = sorted(y for y in df.year.unique() if y >= 2014)
scored = {y: year_scores(y) for y in YEARS if (df.year < y).any()}
TEST = [y for y in scored if y >= 2017]
results = {c: np.vstack([metrics(scored[y], c) for y in TEST]) for c in CANDIDATES}
nested, picks = [], []
for y in TEST:
    inner = [iy for iy in (y - 3, y - 2, y - 1) if iy in scored]
    best = max(CANDIDATES, key=lambda c: objective(np.vstack([metrics(scored[iy], c) for iy in inner])))
    picks.append((y, best)); nested.append(metrics(scored[y], best))
results["nested"] = np.vstack(nested)
leak = np.vstack([metrics(scored[y].assign(v3_leak=scored[y]["spec_h"] * 0 + scored[y]["z_gen_leak"]), "v3_leak") for y in TEST])
results["gen_leaky(sfeat)"] = leak
names = [f"box{k}" for k in range(1, K + 1)] + [f"ov{k}" for k in range(1, K + 1)]
table = pd.DataFrame({c: m.mean(0) * 100 for c, m in results.items()}, index=names).T
table["mean_ov"] = table[[f"ov{k}" for k in range(1, K + 1)]].mean(1)
table["sum_box"] = table[[f"box{k}" for k in range(1, K + 1)]].sum(1)
pd.set_option("display.width", 250)
print("test races", len(results["nested"]))
print(table.round(2).sort_values("mean_ov", ascending=False).to_string())
print("picks", picks)
json.dump({"picks": [[int(y), c] for y, c in picks], "table": table.round(3).to_dict()}, open(f"{W}/shuka_v5_eval.json", "w"), ensure_ascii=False)
pd.concat(scored.values()).to_parquet(f"{W}/shuka_v5_cv{SUFFIX}.parquet")
