"""Point-in-time runner features: heatmap-equivalent partnerships + pedigree combinations."""
import duckdb, time, sys
con = duckdb.connect("/Users/kkk4oru/models/clef-work/data/keiba.duckdb")
con.execute("SET threads=10")
t0 = time.time()
for cat in ["banei"]:
    y0 = "2014"
    surf = "'B'"
    con.execute(f"""
    CREATE OR REPLACE TABLE {cat}_base AS
    WITH r AS (SELECT * FROM nar_ra WHERE keibajo_code='83' QUALIFY row_number() OVER (PARTITION BY kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango ORDER BY data_kubun DESC)=1),
    s AS (SELECT * FROM nar_se WHERE keibajo_code='83' QUALIFY row_number() OVER (PARTITION BY kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban ORDER BY kakutei_chakujun DESC)=1)
    SELECT '{cat}' cat,
      s.kaisai_nen||s.kaisai_tsukihi||s.keibajo_code||s.race_bango race_id,
      strptime(s.kaisai_nen||s.kaisai_tsukihi,'%Y%m%d')::DATE d,
      s.keibajo_code venue, {surf} surface, TRY_CAST(r.kyori AS INT) dist,
      CASE WHEN TRY_CAST(r.kyori AS INT)<=1400 THEN 'S' WHEN TRY_CAST(r.kyori AS INT)<=1800 THEN 'M' WHEN TRY_CAST(r.kyori AS INT)<=2200 THEN 'I' ELSE 'L' END dband,
      coalesce(nullif(r.babajotai_code_shiba,'0'), nullif(r.babajotai_code_dirt,'0'), '0') going,
      r.grade_code grade, r.kyoso_joken_code joken, r.kyoso_shubetsu_code shubetsu, r.kyosomei_hondai race_name,
      TRY_CAST(s.umaban AS INT) umaban, TRY_CAST(s.wakuban AS INT) wakuban, s.ketto_toroku_bango horse,
      s.kishu_code jockey, s.chokyoshi_code trainer, s.banushi_code own,
      TRY_CAST(s.barei AS INT) age, s.seibetsu_code sex, TRY_CAST(s.futan_juryo AS INT)/10.0 futan,
      TRY_CAST(s.bataiju AS INT) weight, CASE WHEN s.zogen_fugo='-' THEN -1 ELSE 1 END*TRY_CAST(s.zogen_sa AS INT) wchg,
      u.s01 sire, u.s05 damsire, u.s03 siresire, u.s13 damdamsire, u.n01 sire_name, u.n05 damsire_name,
      TRY_CAST(s.kakutei_chakujun AS INT) fin, s.ijo_kubun_code ijo, TRY_CAST(s.time_sa AS INT) time_sa,
      TRY_CAST(s.kohan_3f AS INT) l3f, TRY_CAST(s.corner_4 AS INT) c4
    FROM s JOIN r USING (kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)
    LEFT JOIN (SELECT * FROM nar_um QUALIFY row_number() OVER (PARTITION BY ketto_toroku_bango)=1) u ON u.ketto_toroku_bango=s.ketto_toroku_bango
    WHERE s.kaisai_nen >= '{y0}' AND {surf} <> 'O'
    """)
    con.execute(f"""CREATE OR REPLACE TABLE {cat}_base AS SELECT *,
        count(*) OVER (PARTITION BY race_id) AS field,
        CASE WHEN fin BETWEEN 1 AND 18 THEN 1 ELSE 0 END AS valid,
        CASE WHEN fin=1 THEN 1 ELSE 0 END AS win, CASE WHEN fin BETWEEN 1 AND 3 THEN 1 ELSE 0 END AS top3
      FROM {cat}_base""")
    print(cat, "base", con.execute(f"select count(*) from {cat}_base").fetchone(), time.time()-t0, flush=True)

    # key name -> (columns, window years or None)
    specs = {
      "horse": (["horse"], None), "horse_jockey": (["horse","jockey"], None),
      "jockey": (["jockey"], 1), "jockey_venue": (["jockey","venue","surface"], 3),
      "jockey_trainer_venue": (["jockey","trainer","venue","surface"], 10), "trainer": (["trainer"], 1),
      "owner_venue": (["own","venue"], None), "jockey_trainer_owner": (["jockey","trainer","own"], None),
      "owner": (["own"], None), "frame": (["venue","surface","dist","wakuban"], 10), "jockey_frame": (["jockey","wakuban"], 3),
      "horse_surface_dband": (["horse","surface","dband"], None),
      "sire": (["sire"], None), "damsire": (["damsire"], None),
      "sire_course": (["sire","surface","dband"], None), "damsire_course": (["damsire","surface","dband"], None),
      "sire_venue": (["sire","venue","surface"], None), "sire_going": (["sire","surface","going"], None),
      "sire_x_damsire": (["sire","damsire"], None), "sire_x_damsire_course": (["sire","damsire","surface","dband"], None),
      "sire_x_damsire_surface": (["sire","damsire","surface"], None), "sire_x_damsire_venue": (["sire","damsire","venue","surface"], None),
      "siresire_x_damsire": (["siresire","damsire"], None), "siresire_x_damsire_course": (["siresire","damsire","surface","dband"], None),
      "sire_x_damdamsire": (["sire","damdamsire"], None), "sire_x_damsire_going": (["sire","damsire","surface","going"], None),
    }
    out = f"{cat}_feat"
    con.execute(f"CREATE OR REPLACE TABLE {out} AS SELECT race_id, umaban, d FROM {cat}_base")
    for name,(cols,win) in specs.items():
        k = ", ".join(cols)
        nn = " AND ".join(f"{c} IS NOT NULL AND trim(CAST({c} AS VARCHAR)) NOT IN ('','0000000000','00000','000000')" for c in cols)
        frame = f"RANGE BETWEEN INTERVAL {win} YEAR PRECEDING AND INTERVAL 1 DAY PRECEDING" if win else "RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 1 DAY PRECEDING"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE agg AS
          WITH daily AS (SELECT {k}, d, sum(valid) n, sum(win) w, sum(top3) t FROM {cat}_base WHERE {nn} GROUP BY ALL)
          SELECT {k}, d, coalesce(sum(n) OVER wnd,0) pn, coalesce(sum(w) OVER wnd,0) pw, coalesce(sum(t) OVER wnd,0) pt
          FROM daily WINDOW wnd AS (PARTITION BY {k} ORDER BY d {frame})""")
        con.execute(f"""CREATE OR REPLACE TABLE {out} AS SELECT f.*, coalesce(a.pn,0) {name}_n,
             (coalesce(a.pw,0)+0.08*10)/(coalesce(a.pn,0)+10) {name}_win, (coalesce(a.pt,0)+0.24*10)/(coalesce(a.pn,0)+10) {name}_top3
           FROM {out} f JOIN {cat}_base b USING (race_id, umaban) LEFT JOIN agg a ON {' AND '.join(f'a.{c} = b.{c}' for c in cols)} AND a.d = b.d""")
        print(cat, name, round(time.time()-t0,1), flush=True)
    # horse form: last runs
    con.execute(f"""CREATE OR REPLACE TABLE {cat}_form AS
      WITH h AS (SELECT race_id, umaban, horse, d, fin, field, valid, time_sa, l3f,
          CASE WHEN valid=1 THEN (fin-1.0)/greatest(field-1,1) END fnorm FROM {cat}_base)
      SELECT race_id, umaban,
        lag(fnorm,1) OVER w last1, lag(fnorm,2) OVER w last2, lag(fnorm,3) OVER w last3,
        date_diff('day', lag(d,1) OVER w, d) days_since,
        lag(time_sa,1) OVER w last_time_sa,
        count(*) OVER (PARTITION BY horse ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) career_n
      FROM h WINDOW w AS (PARTITION BY horse ORDER BY d)""")
    print(cat, "form", round(time.time()-t0,1), flush=True)
