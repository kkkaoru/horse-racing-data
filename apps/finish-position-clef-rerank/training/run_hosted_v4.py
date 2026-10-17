import sys, json, os, time, urllib.request, pandas as pd, numpy as np
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/Users/kkk4oru/models/clef-work")
from clef_state import build_request_v4, parse_answers_v4
W = "/Users/kkk4oru/models/clef-work/data"
env = dict(l.strip().split("=", 1) for l in open("/Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data/apps/pc-keiba-viewer/.env.local") if "=" in l and not l.startswith("#"))
H = {"CF-Access-Client-Id": env["PC_KEIBA_ACCESS_CLIENT_ID"].strip('"'), "CF-Access-Client-Secret": env["PC_KEIBA_ACCESS_CLIENT_SECRET"].strip('"'), "Content-Type": "application/json", "User-Agent": "pc-keiba-clef-eval/1.0"}
queue = json.load(open(sys.argv[1])); outp = sys.argv[2]; suffix = sys.argv[3]; K = 5; FW = float(os.environ.get("FW", "2"))
done = {json.loads(l)["race_id"] for l in open(outp)} if os.path.exists(outp) else set()
frames = {c: dict(tuple(pd.read_parquet(f"{W}/{c}_{suffix}.parquet").groupby("race_id"))) for c in {c for c, _ in queue}}
z = lambda s: (s - s.mean()) / (s.std() if s.std() > 0 else 1)
def dumps(o): return json.dumps(o, ensure_ascii=False, default=lambda v: None if v != v else (int(v) if float(v).is_integer() else float(v)))
def candidates(g):
    g = g.assign(fused=z(-g.brank.astype(float)) + FW * z(g.sfeat))
    return g.sort_values("fused", ascending=False).head(K)
def one(item):
    cat, rid = item
    g = frames[cat][rid]; r0 = g.iloc[0]; c = g
    req = build_request_v4({"surface": r0.surface, "dist": r0.dist, "going": r0.going, "field": len(g), "grade_c": r0.grade_c, "joken": r0.joken}, c.to_dict("records"), r0.d.date())
    err = ""
    for attempt in range(4):
        try:
            rq = urllib.request.Request("https://pc-keiba-viewer.kkk4oru.com/api/clef-rerank/probe", data=dumps(req).encode(), headers=H, method="POST")
            body = json.loads(urllib.request.urlopen(rq, timeout=60).read())
            return {"cat": cat, "race_id": rid, "answers": {str(k): v for k, v in parse_answers_v4(body["answers"], [str(u) for u in sorted(c.umaban)]).items()}}
        except Exception as e:
            time.sleep(2 * (attempt + 1)); err = str(e)
    return {"cat": cat, "race_id": rid, "error": err}
todo = [q for q in queue if q[1] not in done]
with open(outp, "a") as fo, ThreadPoolExecutor(int(os.environ.get("CONC", "6"))) as ex:
    for i, r in enumerate(ex.map(one, todo)):
        fo.write(dumps(r) + "\n"); fo.flush()
