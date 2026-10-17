import duckdb
con = duckdb.connect("/Users/kkk4oru/models/clef-work/data/keiba.duckdb")
for cat in ["jra","nar"]:
    con.execute(f"""CREATE OR REPLACE TABLE {cat}_hist AS
    WITH b AS (SELECT race_id, umaban, horse, d, venue, surface, dist, going, field, fin, time_sa, c4, l3f, grade, joken,
        rank() OVER (PARTITION BY race_id ORDER BY l3f) AS l3r FROM {cat}_base WHERE horse IS NOT NULL),
    runs AS (SELECT race_id, umaban, horse, d,
        CASE WHEN fin BETWEEN 1 AND 18 THEN struct_pack(d := d, v := venue, s := surface, m := dist, g := going, n := field, f := fin, t := time_sa, c := c4, l := l3r, k := coalesce(nullif(grade,' '), joken)) END AS run FROM b)
    SELECT race_id, umaban,
      list(run) FILTER (WHERE run IS NOT NULL) OVER (PARTITION BY horse ORDER BY d ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last5
    FROM runs""")
    print(cat, con.execute(f"select count(*) from {cat}_hist").fetchone())
print(con.execute("select * from jra_hist where race_id='202610180811' limit 2").fetchall())
