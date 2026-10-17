"""秋華賞 cohort: nested year CV over specialist variants (softmax win/top3, Plackett-Luce top-5, spec/gen blends)."""
import json, sys
import numpy as np, pandas as pd, torch
src = open("/Users/kkk4oru/models/clef-work/shuka3.py").read().split("for l2 in")[0]
exec(src)  # df (valid cohort races), COLS, Z, fit(tr, l2, top3w), met
df = df[df.groupby("race_id")["fin"].transform(lambda s: s.between(1, 5).sum()) >= 5].copy()

def pl_fit(tr, l2):
    data = []
    for _, r in tr.groupby("race_id"):
        order = list(np.argsort(np.where(r.fin.between(1, 18), r.fin, 99))[:5])
        data.append((torch.tensor(r[Z].values, dtype=torch.float32), order))
    w = torch.zeros(len(Z), requires_grad=True); w.data[0] = 1.0
    opt = torch.optim.LBFGS([w], max_iter=100)
    def cl():
        opt.zero_grad(); loss = 0
        for x, order in data:
            s = x @ w; mask = torch.ones(len(s), dtype=torch.bool)
            for j in order:
                loss = loss - (s[j] - torch.logsumexp(s[mask], 0)); mask = mask.clone(); mask[j] = False
        loss = loss / len(data) + l2 * (w[1:] ** 2).sum(); loss.backward(); return loss
    opt.step(cl); return w.detach().numpy()

def m4(d, col):
    out = []
    for _, r in d.groupby("race_id"):
        u = r.sort_values([col, "umaban"], ascending=[False, True]).umaban.tolist(); fin = dict(zip(r.umaban, r.fin))
        a5 = {k for k, v in fin.items() if 1 <= v <= 5}; a3 = {k for k, v in fin.items() if 1 <= v <= 3}
        out.append((set(u[:5]) == a5, set(u[:3]) == a3, fin[u[0]] == 1 and fin[u[1]] == 2, fin[u[0]] == 1, len(set(u[:3]) & a3) / 3))
    return np.array(out, float)

CONFIGS = [("sm", l2, t3) for l2 in (0.3, 1.0, 3.0) for t3 in (0.0, 0.5, 1.0)] + [("pl", l2, None) for l2 in (0.3, 1.0, 3.0)]
BLENDS = (0.0, 0.25, 0.5, 0.75, 1.0)
def score(tr, te, cfg, b):
    kind, l2, t3 = cfg
    w = fit(tr, l2, t3) if kind == "sm" else pl_fit(tr, l2)
    s = te[Z].values @ w; s = (s - s.mean()) / (s.std() or 1)
    return b * s + (1 - b) * te["z_gen"].values
def sel_value(m): return m[:, 1].mean() + m[:, 2].mean() + m[:, 3].mean() + m[:, 4].mean()  # box3+exacta+win1+ov3

rows = {"gen": [], "spec_v3": [], "nested": []}
picks = []
for yr in range(2017, 2027):
    te = df[df.year == yr].copy()
    if te.empty: continue
    inner = []
    for cfg in CONFIGS:
        fits = []
        for iy in range(yr - 3, yr):
            itr, ite = df[df.year < iy], df[df.year == iy].copy()
            if ite.empty: continue
            kind, l2, t3 = cfg
            w = fit(itr, l2, t3) if kind == "sm" else pl_fit(itr, l2)
            s = ite[Z].values @ w; fits.append((ite, (s - s.mean()) / (s.std() or 1)))
        for b in BLENDS:
            ms = np.vstack([m4(ite.assign(v=b * s + (1 - b) * ite["z_gen"].values), "v") for ite, s in fits])
            inner.append((sel_value(ms), cfg, b))
    _, cfg, b = max(inner, key=lambda t: t[0]); picks.append((yr, cfg, b))
    tr = df[df.year < yr]
    te["nested"] = score(tr, te, cfg, b)
    te["spec_v3"] = te[Z].values @ fit(tr, 3.0)
    rows["gen"].append(m4(te, "z_gen")); rows["spec_v3"].append(m4(te, "spec_v3")); rows["nested"].append(m4(te, "nested"))
    print(yr, cfg, b, flush=True)
for k, v in rows.items():
    m = np.vstack(v).mean(0) * 100
    print(f"{k:8s} races={len(np.vstack(v))} box5={m[0]:.2f} box3={m[1]:.2f} exacta={m[2]:.2f} win1={m[3]:.2f} ov3={m[4]:.2f} sum3={m[:3].sum():.2f}")
json.dump([[y, list(c), b] for y, c, b in picks], open("/Users/kkk4oru/models/clef-work/data/shuka_v4_picks.json", "w"))
