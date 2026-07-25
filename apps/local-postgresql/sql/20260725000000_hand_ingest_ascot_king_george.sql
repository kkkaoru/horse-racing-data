-- Hand-ingest: 2026-07-25 Ascot (A6) King George VI & Queen Elizabeth Stakes (G1)
-- Source: /tmp/jra_ascot_utf8.html (JRA overseas racecard)
-- Scope: LOCAL Postgres only. No master INSERT/UPDATE/DELETE. No Neon/D1 writes.
-- Race key: kaisai_nen=2026, kaisai_tsukihi=0725, keibajo_code=A6, race_bango=05.
-- race_bango=05 follows the JRA page's race selector; JV's eventual delayed row may use a different key.
-- D1 pre-check race_key jra:2026:0620:A6:03: empty (no odds_fetch_state/fetch_logs).
-- Placeholders: result/odds fields 00/0000, hasso_jikoku=0000, unresolved codes 00000/000000/ketto 0000000000.
-- Overseas JV precedent stores the page's gate number separately from wakuban; every data_kubun=B jvd_se row uses wakuban=0.
-- Resolved codes come from existing JV masters; ambiguous or explicitly unresolved entities retain zero codes.

BEGIN;

CREATE TEMP TABLE hand_ingest_insert_counts (
  target text PRIMARY KEY,
  inserted_rows integer NOT NULL
) ON COMMIT DROP;

WITH inserted_ra AS (
INSERT INTO jvd_ra (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, yobi_code, tokubetsu_kyoso_bango, kyosomei_hondai, kyosomei_fukudai, kyosomei_kakkonai, kyosomei_hondai_eur, kyosomei_fukudai_eur, kyosomei_kakkonai_eur, kyosomei_ryakusho_10, kyosomei_ryakusho_6, kyosomei_ryakusho_3, kyosomei_kubun, jusho_kaiji, grade_code, grade_code_henkomae, kyoso_shubetsu_code, kyoso_kigo_code, juryo_shubetsu_code, kyoso_joken_code_2sai, kyoso_joken_code_3sai, kyoso_joken_code_4sai, kyoso_joken_code_5sai_ijo, kyoso_joken_code, kyoso_joken_meisho, kyori, kyori_henkomae, track_code, track_code_henkomae, course_kubun, course_kubun_henkomae, honshokin, honshokin_henkomae, fukashokin, fukashokin_henkomae, hasso_jikoku, hasso_jikoku_henkomae, toroku_tosu, shusso_tosu, nyusen_tosu, tenko_code, babajotai_code_shiba, babajotai_code_dirt, lap_time, shogai_mile_time, zenhan_3f, zenhan_4f, kohan_3f, kohan_4f, corner_tsuka_juni_1, corner_tsuka_juni_2, corner_tsuka_juni_3, corner_tsuka_juni_4, record_koshin_kubun)
VALUES ('RA', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '0000', 'キングジョージ６世＆クイーンエリザベスステークス　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', 'KING GEORGE VI AND QUEEN ELIZABETH STAKES                                                                               ', '                                                                                                                        ', '                                                                                                                        ', 'キングジョージ６世＆', 'キングジョー', 'キング', '0', '000', 'A', ' ', '00', '000', '0', '000', '000', '000', '000', '999', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '2390', '0000', '17', '00', '  ', '  ', '00000000000000000000000000000000000000000000000000000000', '0000000000000000000000000000000000000000', '0000000000000000000000000000000000000000', '000000000000000000000000', '0000', '0000', '00', '09', '00', '0', '0', '0', '000000000000000000000000000000000000000000000000000000000000000000000000000', '0000', '000', '000', '000', '000', '00                                                                      ', '00                                                                      ', '00                                                                      ', '00                                                                      ', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts (target, inserted_rows)
SELECT 'jvd_ra', count(*) FROM inserted_ra;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '01', '2021190001', 'カランダガン　　　　　　　　　　　　', '00', '3', '1', '03', '05', '4', '05701', 'グラファ', '166803', 'アガ・カーン・スタッズ　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '610', '000', '0', '0', '05504', '00000', 'バルザロ', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '02', '2020190005', 'ゴリアット　　　　　　　　　　　　　', '00', '3', '1', '03', '06', '4', '05701', 'グラファ', '147803', 'RESOLUTE BLOODSTOCK,ET AL.', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '610', '000', '0', '0', '05271', '00000', 'スミヨン', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '03', '0000000000', 'ランボーン　　　　　　　　　　　　　', '00', '1', '1', '03', '04', '0', '05518', 'オブライ', '000000', 'MME J.MAGNIER,ET AL.', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '610', '000', '0', '0', '05495', '00000', 'ビュイッ', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '04', '2022105519', 'マスカレードボール　　　　　　　　　', '00', '1', '1', '04', '04', '1', '01038', '手塚貴久', '415800', '社台レースホース　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '610', '000', '0', '0', '05339', '00000', 'ルメール', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '05', '2020103060', 'ヴェルテンベルク　　　　　　　　　　', '00', '1', '1', '03', '06', '2', '01073', '宮本博　', '758005', '吉田　照哉　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '610', '000', '0', '0', '05575', '00000', 'マーフィ', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '06', '0000000000', 'カルパナ　　　　　　　　　　　　　　', '00', '2', '1', '03', '05', '0', '05519', 'ボールデ', '000000', 'JUDDMONTE', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '600', '000', '0', '0', '00000', '00000', 'キーン　', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '07', '0000000000', 'ミニーホーク　　　　　　　　　　　　', '00', '2', '1', '03', '04', '0', '05518', 'オブライ', '000000', 'DERRICK SMITH,ET AL.', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '600', '000', '0', '0', '00000', '00000', 'ローダン', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '08', '0000000000', 'アクション　　　　　　　　　　　　　', '00', '1', '1', '01', '03', '0', '05518', 'オブライ', '000000', 'MICHAEL TABOR,ET AL.', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '560', '000', '0', '0', '00000', '00000', 'レヴィー', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

WITH inserted_se AS (
INSERT INTO jvd_se (record_id, data_kubun, data_sakusei_nengappi, kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, wakuban, umaban, ketto_toroku_bango, bamei, umakigo_code, seibetsu_code, hinshu_code, moshoku_code, barei, tozai_shozoku_code, chokyoshi_code, chokyoshimei_ryakusho, banushi_code, banushimei, fukushoku_hyoji, yobi_1, futan_juryo, futan_juryo_henkomae, blinker_shiyo_kubun, yobi_2, kishu_code, kishu_code_henkomae, kishumei_ryakusho, kishumei_ryakusho_henkomae, kishu_minarai_code, kishu_minarai_code_henkomae, bataiju, zogen_fugo, zogen_sa, ijo_kubun_code, nyusen_juni, kakutei_chakujun, dochaku_kubun, dochaku_tosu, soha_time, chakusa_code_1, chakusa_code_2, chakusa_code_3, corner_1, corner_2, corner_3, corner_4, tansho_odds, tansho_ninkijun, kakutoku_honshokin, kakutoku_fukashokin, yobi_3, yobi_4, kohan_4f, kohan_3f, aiteuma_joho_1, aiteuma_joho_2, aiteuma_joho_3, time_sa, record_koshin_kubun, mining_kubun, yoso_soha_time, yoso_gosa_plus, yoso_gosa_minus, yoso_juni, kyakushitsu_hantei)
VALUES ('SE', 'B', '20260725', '2026', '0725', 'A6', '00', '00', '05', '0', '09', '0000000000', 'ベンヴェヌートチェッリーニ　　　　　', '00', '1', '1', '01', '03', '0', '05518', 'オブライ', '000000', 'BRANT,ET AL.', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　', '560', '000', '0', '0', '05366', '00000', 'ムーア　', '　　　　', '0', '0', '   ', ' ', '   ', '0', '00', '00', '0', '0', '0000', '   ', '   ', '   ', '00', '00', '00', '00', '0000', '00', '00000000', '00000000', '000', '000', '000', '000', '0000000000                                    ', '0000000000                                    ', '0000000000                                    ', '0000', '0', '0', '00000', '0000', '0000', '00', '0')
ON CONFLICT DO NOTHING
RETURNING 1
)
INSERT INTO hand_ingest_insert_counts AS counts (target, inserted_rows)
SELECT 'jvd_se', count(*) FROM inserted_se
ON CONFLICT (target) DO UPDATE
SET inserted_rows = counts.inserted_rows + EXCLUDED.inserted_rows;

-- Same-day correction: the initial conservative zero-code mapping predated direct verification
-- of these existing, unambiguous JV master rows. The canonical INSERT values above are corrected
-- too, because ketto_toroku_bango participates in jvd_se's primary key and must stay idempotent.
-- Keep the exact racecard owner display text while linking its verified master code.
UPDATE jvd_se
SET ketto_toroku_bango = '2020190005',
    banushi_code = '147803'
WHERE kaisai_nen = '2026'
  AND kaisai_tsukihi = '0725'
  AND keibajo_code = 'A6'
  AND race_bango = '05'
  AND umaban = '02'
  AND bamei LIKE 'ゴリアット%';

UPDATE jvd_se
SET chokyoshi_code = '05519'
WHERE kaisai_nen = '2026'
  AND kaisai_tsukihi = '0725'
  AND keibajo_code = 'A6'
  AND race_bango = '05'
  AND umaban = '06'
  AND bamei LIKE 'カルパナ%';

-- First run must insert 1 RA + 9 SE rows. A repeat run is accepted only when it inserts
-- 0 + 0 and the existing race exactly matches the authoritative mapping below.
DO $$
DECLARE
  ra_inserted integer;
  se_inserted integer;
  ra_count integer;
  se_count integer;
  mismatches integer;
BEGIN
  SELECT inserted_rows INTO STRICT ra_inserted
  FROM hand_ingest_insert_counts
  WHERE target = 'jvd_ra';

  SELECT inserted_rows INTO STRICT se_inserted
  FROM hand_ingest_insert_counts
  WHERE target = 'jvd_se';

  IF NOT ((ra_inserted = 1 AND se_inserted = 9) OR (ra_inserted = 0 AND se_inserted = 0)) THEN
    RAISE EXCEPTION 'unexpected partial hand-ingest: inserted jvd_ra=%, jvd_se=%', ra_inserted, se_inserted;
  END IF;

  SELECT count(*) INTO ra_count
  FROM jvd_ra
  WHERE kaisai_nen = '2026'
    AND kaisai_tsukihi = '0725'
    AND keibajo_code = 'A6'
    AND race_bango = '05'
    AND record_id = 'RA'
    AND data_kubun = 'B'
    AND data_sakusei_nengappi = '20260725'
    AND btrim(kyosomei_hondai, ' 　') = 'キングジョージ６世＆クイーンエリザベスステークス'
    AND btrim(kyosomei_hondai_eur) = 'KING GEORGE VI AND QUEEN ELIZABETH STAKES'
    AND grade_code = 'A'
    AND kyori = '2390'
    AND track_code = '17'
    AND hasso_jikoku = '0000'
    AND toroku_tosu = '00'
    AND shusso_tosu = '09'
    AND nyusen_tosu = '00';

  IF ra_count <> 1 THEN
    RAISE EXCEPTION 'jvd_ra state check failed: matching rows=% (want 1)', ra_count;
  END IF;

  SELECT count(*) INTO se_count
  FROM jvd_se
  WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0725' AND keibajo_code = 'A6' AND race_bango = '05';

  IF se_count <> 9 THEN
    RAISE EXCEPTION 'jvd_se state check failed: rows=% (want 9)', se_count;
  END IF;

  WITH expected (
    umaban, wakuban, ketto, sex_code, coat_code, age_code, trainer_code, trainer_name,
    owner_code, owner_name, weight_code, jockey_code, jockey_name, odds_code, popularity_code,
    finish_code, arrival_code
  ) AS (
    VALUES
      ('01','0','2021190001','3','03','05','05701','グラファ','166803','アガ・カーン・スタッズ','610','05504','バルザロ','0000','00','00','00'),
      ('02','0','2020190005','3','03','06','05701','グラファ','147803','RESOLUTE BLOODSTOCK,ET AL.','610','05271','スミヨン','0000','00','00','00'),
      ('03','0','0000000000','1','03','04','05518','オブライ','000000','MME J.MAGNIER,ET AL.','610','05495','ビュイッ','0000','00','00','00'),
      ('04','0','2022105519','1','04','04','01038','手塚貴久','415800','社台レースホース','610','05339','ルメール','0000','00','00','00'),
      ('05','0','2020103060','1','03','06','01073','宮本博','758005','吉田　照哉','610','05575','マーフィ','0000','00','00','00'),
      ('06','0','0000000000','2','03','05','05519','ボールデ','000000','JUDDMONTE','600','00000','キーン','0000','00','00','00'),
      ('07','0','0000000000','2','03','04','05518','オブライ','000000','DERRICK SMITH,ET AL.','600','00000','ローダン','0000','00','00','00'),
      ('08','0','0000000000','1','01','03','05518','オブライ','000000','MICHAEL TABOR,ET AL.','560','00000','レヴィー','0000','00','00','00'),
      ('09','0','0000000000','1','01','03','05518','オブライ','000000','BRANT,ET AL.','560','05366','ムーア','0000','00','00','00')
  ), actual AS (
    SELECT
      umaban,
      wakuban,
      ketto_toroku_bango,
      seibetsu_code,
      moshoku_code,
      barei,
      chokyoshi_code,
      btrim(chokyoshimei_ryakusho, ' 　'),
      banushi_code,
      btrim(banushimei, ' 　'),
      futan_juryo,
      kishu_code,
      btrim(kishumei_ryakusho, ' 　'),
      tansho_odds,
      tansho_ninkijun,
      kakutei_chakujun,
      nyusen_juni
    FROM jvd_se
    WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0725' AND keibajo_code = 'A6' AND race_bango = '05'
  ), differences AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT SELECT * FROM expected)
  )
  SELECT count(*) INTO mismatches FROM differences;

  IF mismatches <> 0 THEN
    RAISE EXCEPTION 'jvd_se authoritative mapping check failed: % set differences', mismatches;
  END IF;

  RAISE NOTICE 'hand-ingest verified: inserted jvd_ra=%, jvd_se=%; stored jvd_ra=1, jvd_se=9', ra_inserted, se_inserted;
END $$;

COMMIT;
