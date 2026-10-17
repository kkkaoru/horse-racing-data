"""Build the anonymized clef-flash request for one race (no names, dates, odds or popularity)."""
import math

SURF = {"T": "turf", "D": "dirt", "J": "jump"}
GOING = {"1": "firm", "2": "good", "3": "yielding", "4": "soft"}
LEGEND = ("Each runner row: no=horse number, br=base model rank (1=best), fr=feature model rank, "
          "l3=last three finishes as percentile (0=won, 100=last), ds=days since last run, cs=career starts, "
          "ht=horse top3 %, hc=horse top3 % at this surface/distance band, hj=horse with this jockey top3 %, "
          "jt=jockey top3 % (1y), jv=jockey at this venue/surface top3 %, jtv=jockey+trainer at venue top3 %, "
          "tt=trainer top3 % (1y), ov=owner at venue top3 %, jto=jockey+trainer+owner top3 %, fv=gate top3 % for this course, "
          "sd=sire x damsire top3 %, sdc=sire x damsire at this surface/distance top3 %, sdv=sire x damsire at this venue top3 %, "
          "ssd=sire line x damsire top3 %, sc=sire at this surface/distance top3 %, dc=damsire at this surface/distance top3 %, "
          "age, sex (1 colt,2 filly,3 gelding), wt=carried weight kg. Percentages are shrunk toward 24.")

def _pct(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else int(round(float(v) * 100))

def _num(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else (int(v) if float(v).is_integer() else round(float(v), 1))

def runner_row(r):
    l3 = [_pct(r.get(k)) for k in ("last1", "last2", "last3")]
    return {"no": int(r["umaban"]), "br": _num(r.get("brank")), "fr": _num(r.get("frank")), "l3": l3,
            "ds": _num(r.get("days_since")), "cs": _num(r.get("career_raw")), "ht": _pct(r.get("horse_top3")),
            "hc": _pct(r.get("horse_surface_dband_top3")), "hj": _pct(r.get("horse_jockey_top3")),
            "jt": _pct(r.get("jockey_top3")), "jv": _pct(r.get("jockey_venue_top3")), "jtv": _pct(r.get("jockey_trainer_venue_top3")),
            "tt": _pct(r.get("trainer_top3")), "ov": _pct(r.get("owner_venue_top3")), "jto": _pct(r.get("jockey_trainer_owner_top3")),
            "fv": _pct(r.get("frame_top3")), "sd": _pct(r.get("sire_x_damsire_top3")), "sdc": _pct(r.get("sire_x_damsire_course_top3")),
            "sdv": _pct(r.get("sire_x_damsire_venue_top3")), "ssd": _pct(r.get("siresire_x_damsire_top3")),
            "sc": _pct(r.get("sire_course_top3")), "dc": _pct(r.get("damsire_course_top3")),
            "age": _num(r.get("age")), "sex": _num(r.get("sex_c")), "wt": _num(r.get("futan"))}

def build_request(race, runners, model="clef-flash"):
    """race: dict(surface, dist, going, field, grade_c, joken); runners: list of dict rows."""
    rows = sorted((runner_row(r) for r in runners), key=lambda x: x["no"])
    nos = [str(x["no"]) for x in rows]
    state = {"task": "Japanese horse race finishing-order prediction. Use the runner evidence only.",
             "legend": LEGEND,
             "race": {"surface": SURF.get(race["surface"], race["surface"]), "distance_m": _num(race["dist"]),
                      "going": GOING.get(str(race.get("going")), "unknown"), "runners": len(rows),
                      "class_level": _num(race.get("grade_c")), "condition_code": race.get("joken")},
             "runners": rows}
    questions = {"winner": {"type": "choice", "instructions": "Which horse number is most likely to win this race?",
                            "criteria": {n: f"Horse number {n} wins" for n in nos}}}
    for n in nos:
        questions[f"top3_{n}"] = {"type": "noul", "instructions": f"Will horse number {n} finish in the top three?"}
    return {"model": model, "state": state, "questions": questions}

def parse_answers(answers, nos):
    win = answers["winner"]["probabilities"]
    return {int(n): {"clef_win": float(win.get(str(n), 0.0)), "clef_top3": float(answers[f"top3_{n}"]["noul"])} for n in nos}

LEGEND_V2 = ("Each runner: no=horse number; runs=up to five previous runs, newest last, as "
             "'days before today|surface+distance m|going|field size|finish|margin to winner in 0.1s (negative=won by)|4th-corner position|last-600m speed rank|class'; "
             "rates are top-3 percentages shrunk toward 24: ht=horse, hc=horse at this surface/distance band, hj=horse with this jockey, "
             "jt=jockey (1y), jv=jockey at this venue/surface, jtv=jockey+trainer at venue, tt=trainer (1y), ov=owner at venue, "
             "jto=jockey+trainer+owner, fv=gate at this course, sd=sire x damsire, sdc=sire x damsire at this surface/distance, "
             "sdv=sire x damsire at this venue, ssd=sire line x damsire, sc=sire at this surface/distance, dc=damsire at this surface/distance; "
             "age; sex (1 colt, 2 filly, 3 gelding); wt=carried weight kg.")
_GO = {"1": "firm", "2": "good", "3": "yield", "4": "soft"}

def _run_text(run, today):
    days = (today - run["d"]).days
    t = run.get("t")
    return "|".join([str(days), f"{run['s']}{run['m']}", _GO.get(str(run.get("g")), "?"), str(run.get("n")), str(run.get("f")),
                     "?" if t is None else str(int(t)), "?" if run.get("c") is None else str(int(run["c"])), "?" if run.get("l") is None else str(int(run["l"])), str(run.get("k") or "?").strip()])

def runner_row_v2(r, today):
    base = runner_row(r)
    for k in ("br", "fr", "l3", "ds", "cs"):
        base.pop(k)
    runs = r.get("last5")
    base["runs"] = [] if runs is None else [_run_text(x, today) for x in list(runs) if x is not None]
    return base

def build_request_v2(race, runners, today, model="clef-flash"):
    rows = sorted((runner_row_v2(r, today) for r in runners), key=lambda x: x["no"])
    nos = [str(x["no"]) for x in rows]
    state = {"task": "Japanese horse race finishing-order prediction. Judge each runner from its recent form and the connection/pedigree rates.",
             "legend": LEGEND_V2,
             "race": {"surface": SURF.get(race["surface"], race["surface"]), "distance_m": _num(race["dist"]),
                      "going": GOING.get(str(race.get("going")), "unknown"), "runners": len(rows),
                      "class_level": _num(race.get("grade_c")), "condition_code": race.get("joken")},
             "runners": rows}
    questions = {"winner": {"type": "choice", "instructions": "Which horse number is most likely to win this race?",
                            "criteria": {n: f"Horse number {n} wins" for n in nos}}}
    for n in nos:
        questions[f"top3_{n}"] = {"type": "noul", "instructions": f"Will horse number {n} finish in the top three?"}
    return {"model": model, "state": state, "questions": questions}

LEGEND_V3 = LEGEND_V2.replace("Each runner: no=horse number;", "Each candidate: no=horse number;")

def build_request_v3(race, candidates, today, model="clef-flash"):
    """Top candidates only; asks winner, top-3 and every pairwise 'finishes ahead' question."""
    rows = sorted((runner_row_v2(r, today) for r in candidates), key=lambda x: x["no"])
    nos = [str(x["no"]) for x in rows]
    state = {"task": "Japanese horse race. These are the leading candidates of a larger field. Compare them using recent form and connection/pedigree rates.",
             "legend": LEGEND_V3,
             "race": {"surface": SURF.get(race["surface"], race["surface"]), "distance_m": _num(race["dist"]),
                      "going": GOING.get(str(race.get("going")), "unknown"), "field_size": _num(race["field"]),
                      "class_level": _num(race.get("grade_c")), "condition_code": race.get("joken")},
             "candidates": rows}
    q = {"winner": {"type": "choice", "instructions": "Which of these candidates is most likely to win the race?",
                    "criteria": {n: f"Horse number {n} wins" for n in nos}}}
    for n in nos:
        q[f"top3_{n}"] = {"type": "noul", "instructions": f"Will horse number {n} finish in the top three of the whole field?"}
    for i, a in enumerate(nos):
        for b in nos[i + 1:]:
            q[f"ahead_{a}_{b}"] = {"type": "noul", "instructions": f"Will horse number {a} finish ahead of horse number {b}?"}
    return {"model": model, "state": state, "questions": q}

def parse_answers_v3(answers, nos):
    win = answers["winner"]["probabilities"]
    out = {int(n): {"clef_win": float(win.get(str(n), 0.0)), "clef_top3": float(answers[f"top3_{n}"]["noul"]), "clef_pair": 0.0} for n in nos}
    for i, a in enumerate(nos):
        for b in nos[i + 1:]:
            p = float(answers[f"ahead_{a}_{b}"]["noul"])
            out[int(a)]["clef_pair"] += p; out[int(b)]["clef_pair"] += 1 - p
    return out

V4_QUESTIONS = {
    "form": "Based on its recent runs, is horse number {n} in good current form?",
    "fit": "Do horse number {n}'s previous runs suggest that today's surface and distance suit it?",
    "cls": "Has horse number {n} already run well at today's class level or higher?",
}

def build_request_v4(race, runners, today, model="clef-flash"):
    req = build_request_v2(race, runners, today, model)
    nos = [str(x["no"]) for x in req["state"]["runners"]]
    req["state"]["task"] = "Japanese horse race. Judge each runner's evidence from its recent runs and connection/pedigree rates."
    req["questions"] = {f"{k}_{n}": {"type": "noul", "instructions": t.format(n=n)} for n in nos for k, t in V4_QUESTIONS.items()}
    return req

def parse_answers_v4(answers, nos):
    return {int(n): {f"clef_{k}": float(answers[f"{k}_{n}"]["noul"]) for k in V4_QUESTIONS} for n in nos}
