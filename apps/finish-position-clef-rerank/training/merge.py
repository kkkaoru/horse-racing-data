import duckdb, json
R="/Users/kkk4oru/models/clef-work/data/raw"
con = duckdb.connect("/Users/kkk4oru/models/clef-work/data/keiba.duckdb")
for cat,p in [("jra","jvd"),("nar","nvd")]:
    for t in ["se","ra","um"]:
        con.execute(f"CREATE OR REPLACE TEMP TABLE g AS SELECT * FROM read_json_auto('{R}/{p}_{t}_gap.json')")
        cols = [r[0] for r in con.execute(f"DESCRIBE {cat}_{t}").fetchall()]
        sel = ", ".join(f"g.{c}" for c in cols)
        if t == "um":
            con.execute(f"DELETE FROM {cat}_um WHERE ketto_toroku_bango IN (SELECT ketto_toroku_bango FROM g)")
        else:
            con.execute(f"DELETE FROM {cat}_{t} WHERE kaisai_nen='2026' AND kaisai_tsukihi >= '1005'")
        con.execute(f"INSERT INTO {cat}_{t} SELECT {sel} FROM g")
# patch JRA 10/17 results from realtime D1 snapshots
con.execute(f"CREATE OR REPLACE TEMP TABLE res AS SELECT split_part(race_key,':',1) cat, split_part(race_key,':',4) kj, split_part(race_key,':',5) rb, lpad(horse_number,2,'0') umaban, finish_position fp FROM read_json_auto('{R}/results1017.json')")
for cat in ["jra","nar"]:
    n = con.execute(f"""UPDATE {cat}_se s SET kakutei_chakujun = lpad(regexp_replace(r.fp,'[^0-9]','','g'),2,'0') FROM res r
      WHERE r.cat='{cat}' AND s.kaisai_nen='2026' AND s.kaisai_tsukihi='1017' AND s.keibajo_code=r.kj AND s.race_bango=r.rb AND s.umaban=r.umaban
      AND (s.kakutei_chakujun IS NULL OR s.kakutei_chakujun IN ('00','')) AND regexp_matches(r.fp,'^[0-9]+$')""").fetchone()
    print(cat, "patched", n)
print(con.execute("select kaisai_tsukihi, count(*), count(*) filter (where kakutei_chakujun not in ('00','')) from jra_se where kaisai_nen='2026' and kaisai_tsukihi>='1004' group by 1 order by 1").fetchall())
con.execute(f"CREATE OR REPLACE TABLE preds_1017_1018 AS SELECT * FROM read_json_auto('{R}/preds_1017_1018.json')")
con.execute(f"CREATE OR REPLACE TABLE results_1017_realtime AS SELECT * FROM read_json_auto('{R}/results1017.json')")
print(con.execute("select kaisai_tsukihi, model_version, count(*) from preds_1017_1018 group by 1,2 order by 1,3 desc").fetchall())
