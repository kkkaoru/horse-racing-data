import duckdb, time
con = duckdb.connect("/Users/kkk4oru/models/clef-work/data/keiba.duckdb")
t=time.time()
for cat in ["banei"]:
    con.execute(f"""CREATE OR REPLACE TABLE {cat}_form2 AS
    WITH b AS (SELECT race_id, umaban, horse, d, dist, surface, jockey, futan, field, fin, time_sa, c4, valid,
        CASE WHEN valid=1 THEN rank() OVER (PARTITION BY race_id ORDER BY l3f) END AS l3r,
        CASE WHEN grade IN ('A') THEN 9 WHEN grade='B' THEN 8 WHEN grade='C' THEN 7 WHEN grade IN ('L','E') THEN 6
             WHEN joken='999' THEN 5 WHEN joken='016' THEN 4 WHEN joken='010' THEN 3 WHEN joken='005' THEN 2 WHEN joken IN ('701','702','703') THEN 1 ELSE NULL END AS cls
      FROM {cat}_base WHERE horse IS NOT NULL),
    w AS (SELECT *,
        lag(time_sa,2) OVER h AS ts2, lag(time_sa,3) OVER h AS ts3,
        avg(CASE WHEN valid=1 THEN l3r*1.0/field END) OVER (PARTITION BY horse ORDER BY d ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS l3r_avg3,
        avg(CASE WHEN valid=1 THEN c4*1.0/field END) OVER (PARTITION BY horse ORDER BY d ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS c4_avg3,
        avg(CASE WHEN valid=1 THEN time_sa END) OVER (PARTITION BY horse ORDER BY d ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS ts_avg5,
        min(CASE WHEN valid=1 THEN time_sa END) OVER (PARTITION BY horse ORDER BY d ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS ts_min5,
        dist - lag(dist) OVER h AS dist_chg,
        CASE WHEN surface = lag(surface) OVER h THEN 0 ELSE 1 END AS surf_chg,
        CASE WHEN jockey = lag(jockey) OVER h THEN 0 ELSE 1 END AS jockey_chg,
        futan - lag(futan) OVER h AS futan_chg,
        cls - lag(cls) OVER h AS cls_chg,
        max(cls) FILTER (WHERE fin BETWEEN 1 AND 3) OVER (PARTITION BY horse ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS best_cls_t3,
        cls AS cls_now
      FROM b WINDOW h AS (PARTITION BY horse ORDER BY d))
    SELECT race_id, umaban, ts2, ts3, l3r_avg3, c4_avg3, ts_avg5, ts_min5, dist_chg, surf_chg, jockey_chg, futan_chg, cls_chg, best_cls_t3, cls_now,
       best_cls_t3 - cls_now AS cls_margin FROM w""")
    print(cat, time.time()-t, flush=True)
