import duckdb, time
con = duckdb.connect("/Users/kkk4oru/models/clef-work/data/keiba.duckdb")
con.execute("INSTALL postgres; LOAD postgres;")
pw = [l.split("=",1)[1].strip() for l in open("/Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data/apps/local-postgresql/.env") if l.startswith("POSTGRES_PASSWORD=")][0]
con.execute(f"ATTACH 'host=127.0.0.1 port=15432 dbname=horse_racing user=horse_racing password={pw}' AS pg (TYPE postgres, READ_ONLY)")
t=time.time()
for cat, se, ra, um, y0 in [("jra","jvd_se","jvd_ra","jvd_um","2008"),("nar","nvd_se","nvd_ra","nvd_um","2012")]:
    con.execute(f"""CREATE OR REPLACE TABLE {cat}_se AS SELECT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, wakuban, umaban, ketto_toroku_bango, barei, seibetsu_code,
      chokyoshi_code, banushi_code, kishu_code, futan_juryo, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, kakutei_chakujun, time_sa, corner_4, kohan_3f, soha_time, kyakushitsu_hantei
      FROM pg.{se} WHERE kaisai_nen >= '{y0}' AND data_kubun IN ('7','A','B','2','1','3','4','5','6')""")
    con.execute(f"""CREATE OR REPLACE TABLE {cat}_ra AS SELECT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyosomei_hondai, grade_code, kyoso_shubetsu_code, kyoso_joken_code, kyoso_joken_meisho, kyori, track_code,
      shusso_tosu, babajotai_code_shiba, babajotai_code_dirt, data_kubun, juryo_shubetsu_code FROM pg.{ra} WHERE kaisai_nen >= '{y0}'""")
    con.execute(f"""CREATE OR REPLACE TABLE {cat}_um AS SELECT ketto_toroku_bango, seinengappi, seibetsu_code, ketto_joho_01a s01, ketto_joho_02a s02, ketto_joho_03a s03, ketto_joho_05a s05, ketto_joho_07a s07, ketto_joho_09a s09, ketto_joho_11a s11, ketto_joho_13a s13,
      ketto_joho_01b n01, ketto_joho_03b n03, ketto_joho_05b n05, ketto_joho_13b n13 FROM pg.{um}""")
    print(cat, con.execute(f"select count(*) from {cat}_se").fetchone(), con.execute(f"select count(*) from {cat}_ra").fetchone(), time.time()-t, flush=True)
