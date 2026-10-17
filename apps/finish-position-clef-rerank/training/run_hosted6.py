"""Score queued races with Cloudflare-hosted clef-flash through the Access-protected probe route."""
import sys, json, os, time, urllib.request, pandas as pd
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/Users/kkk4oru/models/clef-work")
from clef_state import build_request, build_request_v2, build_request_v5, build_request_v6, parse_answers, parse_answers_v5
W = "/Users/kkk4oru/models/clef-work/data"
env = dict(l.strip().split("=", 1) for l in open("/Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data/apps/pc-keiba-viewer/.env.local") if "=" in l and not l.startswith("#"))
H = {"CF-Access-Client-Id": env["PC_KEIBA_ACCESS_CLIENT_ID"].strip('"'), "CF-Access-Client-Secret": env["PC_KEIBA_ACCESS_CLIENT_SECRET"].strip('"'), "Content-Type": "application/json", "User-Agent": "pc-keiba-clef-eval/1.0 (curl-compatible)"}
queue = json.load(open(sys.argv[1])); outp = sys.argv[2]; frame_suffix = sys.argv[3] if len(sys.argv) > 3 else "2026_scored"
done = {json.loads(l)["race_id"] for l in open(outp)} if os.path.exists(outp) else set()
frames = {c: dict(tuple(pd.read_parquet(f"{W}/{c}_{frame_suffix}.parquet").groupby("race_id"))) for c in {c for c, _ in queue}}
def dumps(o): return json.dumps(o, ensure_ascii=False, default=lambda v: None if v != v else (int(v) if float(v).is_integer() else float(v)))
def one(item):
    cat, rid = item
    g = frames[cat][rid]; r0 = g.iloc[0]
    race = {"surface": r0.surface, "dist": r0.dist, "going": r0.going, "field": len(g), "grade_c": r0.grade_c, "joken": r0.joken}
    v5 = os.environ.get("STATE") == "v5"
    req = (build_request_v6 if os.environ.get("NAMES") else build_request_v5)(race, g.to_dict("records")) if v5 else build_request(race, g.to_dict("records"))
    if os.environ.get("SHUFFLE"):
        import random
        rows = req["state"]["runners"]; random.Random(rid).shuffle(rows); req["state"]["runners"] = rows
    for attempt in range(4):
        try:
            rq = urllib.request.Request("https://pc-keiba-viewer.kkk4oru.com/api/clef-rerank/probe", data=dumps(req).encode(), headers=H, method="POST")
            body = json.loads(urllib.request.urlopen(rq, timeout=60).read())
            return {"cat": cat, "race_id": rid, "answers": {str(k): v for k, v in (parse_answers_v5(body["answers"], [str(u) for u in g.umaban]) if v5 else parse_answers(body["answers"], g.umaban.tolist())).items()}}
        except Exception as e:
            time.sleep(2 * (attempt + 1)); err = str(e)
    return {"cat": cat, "race_id": rid, "error": err}
todo = [q for q in queue if q[1] not in done]
with open(outp, "a") as fo, ThreadPoolExecutor(int(os.environ.get("CONC", "4"))) as ex:
    for i, r in enumerate(ex.map(one, todo)):
        fo.write(dumps(r) + "\n"); fo.flush()
        if i % 100 == 0: print(i, len(todo), flush=True)
