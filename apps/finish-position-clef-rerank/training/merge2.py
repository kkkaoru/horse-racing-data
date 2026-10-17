import duckdb
R="/Users/kkk4oru/models/clef-work/data/raw"
con = duckdb.connect("/Users/kkk4oru/models/clef-work/data/keiba.duckdb")
for cat,p in [("jra","jvd"),("nar","nvd")]:
    con.execute(f"CREATE OR REPLACE TEMP TABLE g AS SELECT * FROM read_json_auto('{R}/{p}_se_late.json')")
    cols = [r[0] for r in con.execute(f"DESCRIBE {cat}_se").fetchall()]
    print(cat, con.execute("select count(*), count(*) filter (where kakutei_chakujun not in ('00','')) from g").fetchone())
    con.execute(f"DELETE FROM {cat}_se WHERE kaisai_nen='2026' AND kaisai_tsukihi BETWEEN '0920' AND '1004'")
    con.execute(f"INSERT INTO {cat}_se SELECT {', '.join('g.'+c for c in cols)} FROM g")
print(con.execute("select kaisai_tsukihi, count(*), count(*) filter (where kakutei_chakujun not in ('00','')) from jra_se where kaisai_nen='2026' and kaisai_tsukihi>='0920' group by 1 order by 1").fetchall())
