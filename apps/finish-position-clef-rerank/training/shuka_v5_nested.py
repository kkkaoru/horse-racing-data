"""秋華賞 cohort: nested selection over z(gen_h) + a*z(dom_h) + b*z(spec_h) + c*z(pl_h) on all prior years.

Reads shuka_v5_cv.parquet (out-of-time candidate columns per cohort year from shuka_v5_eval.py).
For each test year Y the weights maximise mean(ov1..5)+mean(box1..5) over every cohort year < Y.
"""
import itertools
import numpy as np
import pandas as pd

W = "/Users/kkk4oru/models/clef-work/data"
K = 5
cv = pd.read_parquet(f"{W}/shuka_v5_cv.parquet")
zs = lambda s: (s - s.mean()) / (s.std() if s.std() > 0 else 1)
cv["pl_z"] = cv.groupby("race_id")["pl_h"].transform(zs)
GRID = list(itertools.product((0, 0.5, 1.0), (0, 0.5, 1.0), (0, 0.5, 1.0)))

def metrics(d: pd.DataFrame, col: str) -> np.ndarray:
    rows = []
    for _, r in d.groupby("race_id"):
        order = r.sort_values([col, "umaban"], ascending=[False, True]).umaban.tolist()
        fin = dict(zip(r.umaban, r.fin))
        top = [{h for h, f in fin.items() if 1 <= f <= k} for k in range(1, K + 1)]
        rows.append([set(order[:k]) == top[k - 1] for k in range(1, K + 1)] + [len(set(order[:k]) & top[k - 1]) / k for k in range(1, K + 1)])
    return np.array(rows, float)

for a, b, c in GRID:
    cv[f"w_{a}_{b}_{c}"] = cv["gen_h"] + a * cv["dom_h"] + b * cv["spec_h"] + c * cv["pl_z"]
cache = {(y, w): metrics(cv[cv.year == y], w) for y in cv.year.unique() for w in [f"w_{a}_{b}_{c}" for a, b, c in GRID] + ["v3_h", "gen_h"]}
objective = lambda m: m[:, K:].mean() + m[:, :K].mean()
nested, base, picks = [], [], []
for y in sorted(y for y in cv.year.unique() if y >= 2017):
    prior = [p for p in cv.year.unique() if p < y]
    best = max((f"w_{a}_{b}_{c}" for a, b, c in GRID), key=lambda w: objective(np.vstack([cache[(p, w)] for p in prior])))
    picks.append((int(y), best)); nested.append(cache[(y, best)]); base.append(cache[(y, "v3_h")])
names = [f"box{k}" for k in range(1, K + 1)] + [f"ov{k}" for k in range(1, K + 1)]
for label, m in (("v3_h", np.vstack(base)), ("nested_all_prior", np.vstack(nested))):
    v = m.mean(0) * 100
    print(f"{label:18s} " + " ".join(f"{n}={x:.2f}" for n, x in zip(names, v)) + f" mean_ov={v[K:].mean():.2f} sum_box={v[:K].sum():.2f}")
print(picks)

# Served order = 0.388*z(base rank) + 0.514*z(feature); gen_h stands in for the served cb-v9 base.
fz = lambda col: cv.groupby("race_id")[col].transform(zs)
nested_col = cv.apply(lambda r: r[dict(picks).get(int(r.year), "v3_h")] if int(r.year) >= 2017 else np.nan, axis=1)
cv["nested_col"] = nested_col
for weights in ((0.388, 0.514), (0.1, 1.0)):
    for label, col in (("v3_h", "v3_h"), ("nested", "nested_col")):
        cv["fused"] = weights[0] * cv["gen_h"] + weights[1] * fz(col)
        m = np.vstack([metrics(cv[cv.year == y], "fused") for y in sorted(cv.year.unique()) if y >= 2017]).mean(0) * 100
        print(f"fused{weights} {label:7s} " + " ".join(f"{n}={x:.2f}" for n, x in zip(names, m)) + f" mean_ov={m[K:].mean():.2f} sum_box={m[:K].sum():.2f}")
