import pandas as pd, numpy as np, torch, duckdb, json
W="/Users/kkk4oru/models/clef-work/data"
df = pd.read_parquet(f"{W}/shuka_cohort.parquet")
rn = duckdb.connect(f"{W}/keiba.duckdb", read_only=True).execute("select distinct race_id, race_name from jra_base").df()
df = df.merge(rn, on="race_id", how="left")
df = df[df.groupby("race_id")["valid"].transform("sum")>0].copy()
for c in ["g_best","g1_best","last1"]:
    df[c] = df[c].fillna(1.0)
COLS = {"gen":1, "g_best":-1, "g1_best":-1, "g_runs":1, "g1_runs":1, "sire_x_damsire_course_top3":1, "damsire_course_top3":1, "sire_course_top3":1, "last1":-1, "horse_top3":1, "jockey_top3":1, "jockey_venue_top3":1, "siresire_x_damsire_top3":1}
g = df.groupby("race_id")
Z = []
for c, s in COLS.items():
    zc = g[c].transform(lambda x: (x-x.mean())/(x.std() if x.std()>0 else 1)).fillna(0)*s
    df["z_"+c] = zc; Z.append("z_"+c)
def fit(tr, l2=1.0, top3w=0.5):
    races = [r for _, r in tr.groupby("race_id")]
    X = [torch.tensor(r[Z].values, dtype=torch.float32) for r in races]
    win = [torch.tensor((r.fin.values==1).astype(np.float32)) for r in races]
    t3 = [torch.tensor((r.fin.between(1,3).values).astype(np.float32)) for r in races]
    w = torch.zeros(len(Z), requires_grad=True); w.data[0] = 1.0
    opt = torch.optim.LBFGS([w], max_iter=200)
    def closure():
        opt.zero_grad()
        loss = sum(-(torch.log_softmax(x@w,0)*y).sum() - top3w*(torch.log_softmax(x@w,0)*t).sum()/3 for x,y,t in zip(X,win,t3))/len(X) + l2*((w[1:])**2).sum()
        loss.backward(); return loss
    opt.step(closure); return w.detach().numpy()
def met(d, col):
    out=[]
    for rid, r in d.groupby("race_id"):
        r=r.sort_values(col, ascending=False); a3=set(r[r.fin.between(1,3)].umaban); p3=set(r.umaban.iloc[:3])
        out.append((r.iloc[0].fin==1, r.iloc[0].fin<=3, len(a3&p3)/3))
    return np.array(out,float)
for l2 in [0.3, 1.0, 3.0]:
    G, S, SG, SS = [], [], [], []
    for yr in range(2017, 2027):
        tr = df[df.year < yr]; te = df[df.year == yr].copy()
        if te.empty: continue
        w = fit(tr, l2)
        te["spec"] = te[Z].values @ w
        G.append(met(te,"z_gen")); S.append(met(te,"spec"))
        s = te[te.race_name.str.contains("秋華賞", na=False)]
        if len(s): SG.append(met(s,"z_gen")); SS.append(met(s,"spec"))
    print("l2", l2, "cohort gen", np.vstack(G).mean(0).round(3), "spec", np.vstack(S).mean(0).round(3), len(np.vstack(G)), "| 秋華賞 gen", np.vstack(SG).mean(0).round(3), "spec", np.vstack(SS).mean(0).round(3), len(np.vstack(SG)), flush=True)
w = fit(df, 1.0); print(dict(zip(Z, w.round(3))))
json.dump({"cols": COLS, "weights": dict(zip(Z, [float(x) for x in w]))}, open(f"{W}/shuka_linear.json","w"), ensure_ascii=False)
