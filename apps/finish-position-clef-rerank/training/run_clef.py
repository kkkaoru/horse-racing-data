import sys, json, time, os, torch, pandas as pd
sys.path.insert(0, "/Users/kkk4oru/models/clef-flash"); sys.path.insert(0, "/Users/kkk4oru/models/clef-work")
from joint_schema_model import load_release_model, systemone
from clef_state import build_request, parse_answers
W="/Users/kkk4oru/models/clef-work/data"
queue = json.load(open(sys.argv[1])); outp = sys.argv[2]
done = set()
if os.path.exists(outp):
    for l in open(outp): done.add(json.loads(l)["race_id"])
frames = {c: pd.read_parquet(f"{W}/{c}_2026_scored.parquet") for c in {c for c,_ in queue}}
groups = {c: dict(tuple(f.groupby("race_id"))) for c,f in frames.items()}
model, proc = load_release_model("/Users/kkk4oru/models/clef-flash", device="mps")
with open(outp, "a") as fo:
    for i,(cat, rid) in enumerate(queue):
        if rid in done: continue
        g = groups[cat][rid]; r0 = g.iloc[0]
        race = {"surface": r0.surface, "dist": r0.dist, "going": r0.going, "field": len(g), "grade_c": r0.grade_c, "joken": r0.joken}
        req = build_request(race, g.to_dict("records"))
        t = time.time(); resp = systemone(model, proc, req)
        ans = parse_answers(resp["answers"], g.umaban.tolist())
        fo.write(json.dumps({"cat": cat, "race_id": rid, "tokens": resp["usage"]["input_tokens"], "sec": round(time.time()-t,2), "answers": {str(k): v for k,v in ans.items()}})+"\n"); fo.flush()
        if i % 20 == 0: print(i, rid, resp["usage"]["input_tokens"], round(time.time()-t,2), flush=True)
