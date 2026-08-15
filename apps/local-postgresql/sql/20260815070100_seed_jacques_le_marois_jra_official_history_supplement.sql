-- Official JRA card supplement for all ten 2026 Prix Jacques le Marois runners.
-- Existing JRA-VAN base rows are matched exactly by source-native identity and
-- race date; no base value is overwritten.
with official_rows (
  umaban, race_date, country_or_jra_venue_label, grade, field_size,
  gate_number, popularity, weight_carried_kg, race_time_text,
  race_time_seconds, race_time_parse_status, going, corner_positions_text, comparison_horse_name,
  comparison_margin_text
) as (
  values
    (1, '2026-07-11', 'アスコット', 'G2', 8, 3, 2, 58.5, '1:40.1', 100.1, 'parsed', '良', '1,4,3', 'ホロウェイボーイ', '0.1'),
    (1, '2026-06-16', 'アスコット', 'G1', 9, 5, 5, 58.0, '1:38.0', 98.0, 'parsed', '良', '4,4,3', 'テンボブトニー', '0.5'),
    (1, '2026-05-16', 'ニューベリー', 'G1', 10, 1, 3, 58.0, '1:36.7', 96.7, 'parsed', '良', '4,4,4', 'ノータブルスピー', '0.5'),
    (1, '2026-04-24', 'サンダウン', 'G2', 6, 1, 2, 60.0, '1:42.8', 102.8, 'parsed', '良', '2,2,2', 'オペラバッロ', '0.6'),
    (2, '2026-03-29', 'サンクルー', 'G3', 10, 8, 6, 57.0, '1:42.6', 102.6, 'parsed', '重', '10,9,10', 'ネイティブウォリ', '0.1'),
    (2, '2025-10-04', 'パリロンシャン', 'G2', 10, 8, 3, 56.5, '1:41.2', 101.2, 'parsed', '重', '10,10,10', 'リダリ', '0.8'),
    (2, '2025-08-30', 'ドーヴィル', 'G3', 11, 4, 3, 55.0, '1:34.3', 94.3, 'parsed', '重', '4,4,4', 'キケロズギフト', '0.1'),
    (2, '2025-06-28', 'シャンティイ', null, 9, 4, 4, 56.5, '1:35.5', 95.5, 'parsed', '稍重', '6,7,6', 'ティエゴザファー', '0.1'),
    (3, '2026-06-07', '東京', 'G1', 17, 4, 8, 58.0, '1:32.1', 92.1, 'parsed', '良', '2,2', 'ワールズエンド', '0.0'),
    (3, '2026-04-26', '京都', 'G2', 18, 16, 4, 57.0, '1:32.1', 92.1, 'parsed', '良', '9,8', 'アドマイヤズーム', '0.4'),
    (3, '2026-02-22', '東京', 'G1', 16, 5, 6, 58.0, '1:36.4', 96.4, 'parsed', '良', '1,1', 'コスタノヴァ', '1.0'),
    (3, '2025-12-07', '中京', 'G1', 16, 11, 5, 58.0, '1:51.5', 111.5, 'parsed', '良', '2,1,2,2', 'ダブルハートボン', '1.3'),
    (4, '2026-04-26', 'シャティン', 'G1', 14, 7, 10, 57.0, '1:33.7', 93.7, 'parsed', '良', '6,6,7', 'マイウィッシュ', '1.4'),
    (4, '2026-02-07', 'アブダビ', null, 15, 10, 1, 57.0, '1:33.9', 93.9, 'parsed', '良', '5,3,6', 'ダークトルーパー', '0.2'),
    (4, '2025-11-01', 'ランドウィック', null, 15, 13, 6, 57.5, '1:17.4', 77.4, 'parsed', '重', '11,11', 'ジミースター', '0.9'),
    (4, '2025-08-10', '中京', 'G3', 17, 5, 4, 57.0, '1:07.5', 67.5, 'parsed', '良', '16,13', 'インビンシブルパ', '0.1'),
    (5, '2026-07-11', 'アスコット', 'G2', 8, 1, 1, 58.5, '1:40.3', 100.3, 'parsed', '良', '5,7,8', 'ゼウスオリンピオ', '0.2'),
    (5, '2026-06-16', 'アスコット', 'G1', 9, 9, 2, 58.0, '1:37.6', 97.6, 'parsed', '良', '6,7,5', 'テンボブトニー', '0.1'),
    (5, '2026-05-16', 'ニューベリー', 'G1', 10, 9, 5, 58.0, '1:36.5', 96.5, 'parsed', '良', '10,10,9', 'ノータブルスピー', '0.3'),
    (5, '2025-10-05', 'パリロンシャン', 'G1', 16, 16, 3, 58.0, '1:21.3', 81.3, 'parsed', '重', '16,15,12', 'マラノアチャーリ', '0.5'),
    (6, '2026-07-11', 'アスコット', 'G2', 8, 7, 4, 60.0, '1:41.1', 101.1, 'parsed', '良', '7,4,4', 'ゼウスオリンピオ', '1.0'),
    (6, '2026-05-01', 'サンクルー', 'G2', 7, 6, 1, 57.0, '1:36.8', 96.8, 'parsed', '良', '6,5,4', 'シーガルズイレブ', '0.1'),
    (6, '2026-03-08', 'シャンティイ', null, 8, 6, 1, 56.5, '1:37.7', 97.7, 'parsed', '良', '4,4,3', 'クラヴス', '0.3'),
    (6, '2026-02-15', 'カーニュシュルメール', null, 10, 7, 1, 58.5, '1:32.6', 92.6, 'parsed', '良', '4,4,2', 'コリンザブレイヴ', '0.4'),
    (7, '2026-06-04', 'バーデンバーデン', 'G3', 6, 2, 3, 56.0, '1:39.5', 99.5, 'parsed', '稍重', '1,1,1', 'アスター', '0.6'),
    (7, '2026-04-19', 'トゥールーズ', null, 8, 3, 1, 56.0, '1:34.6', 94.6, 'parsed', '稍重', '1,1,1', 'カンブロンヌ', '0.3'),
    (7, '2026-03-21', 'ラテストドビュック', null, 13, 3, 4, 56.0, '1:24.6', 84.6, 'parsed', '重', '3,5,4', 'ダリウスセン', '0.4'),
    (7, '2026-02-15', 'カーニュシュルメール', null, 10, 5, 5, 56.5, '1:33.2', 93.2, 'parsed', '良', '1,1,1', 'ノーランチ', '0.6'),
    (8, '2026-07-12', 'ドーヴィル', 'G1', 8, 5, 1, 58.5, '1:22.5', 82.5, 'parsed', '良', '5,6,5', 'ザシークレットア', '0.4'),
    (8, '2026-05-10', 'パリロンシャン', 'G1', 13, 1, 2, 58.0, '1:38.6', 98.6, 'parsed', '重', '3,4,4', 'コモレビ', '0.2'),
    (8, '2025-10-05', 'パリロンシャン', 'G1', 9, 4, 1, 57.0, '1:22.1', 82.1, 'parsed', '重', '6,7,6', 'プエルトリコ', '0.6'),
    (8, '2025-08-17', 'ドーヴィル', 'G3', 7, 7, 2, 57.5, '1:21.5', 81.5, 'parsed', '稍重', '7,7,7', 'アンダブ', '0.4'),
    (9, '2026-07-12', 'ドーヴィル', 'G1', 8, 1, 4, 58.5, '1:22.1', 82.1, 'parsed', '良', '1,1,1', 'トゥルーラブ', '0.1'),
    (9, '2026-06-20', 'アスコット', 'G3', 16, 2, 7, 60.0, '1:25.1', 85.1, 'parsed', '良', '6,7,6', 'テイクチャージス', '0.1'),
    (9, '2026-05-23', 'カラ', 'G1', 9, 1, 4, 58.0, '1:36.4', 96.4, 'parsed', '良', '4,4,4', 'グシュタード', '0.8'),
    (9, '2026-05-02', 'ニューマーケット', 'G1', 14, 5, 9, 58.0, '1:37.5', 97.5, 'parsed', '良', '7,4,6', 'ボウエコー', '2.0'),
    (10, '2026-08-02', 'ドーヴィル', 'G1', 8, 4, 2, 56.5, '1:35.8', 95.8, 'parsed', '良', '2,2,2', 'ブルーボルト', '0.1'),
    (10, '2026-07-10', 'ニューマーケット', 'G1', 7, 3, 1, 57.0, '1:37.6', 97.6, 'parsed', '良', '5,5,4', 'ブルーボルト', '0.3'),
    (10, '2026-06-19', 'アスコット', 'G1', 9, 5, 1, 58.0, '1:39.5', 99.5, 'parsed', '良', '8,6,1', 'トゥリーン', '0.3'),
    (10, '2026-05-24', 'カラ', 'G1', 11, 7, 2, 58.0, '1:36.9', 96.9, 'parsed', '良', '9,9,9', 'トゥルーラブ', '0.4')
), matched_history as (
  select
    h.history_id,
    o.country_or_jra_venue_label,
    o.grade,
    o.field_size,
    o.gate_number,
    o.popularity,
    o.weight_carried_kg,
    o.race_time_text,
    o.race_time_seconds,
    o.race_time_parse_status,
    o.going,
    o.corner_positions_text,
    o.comparison_horse_name,
    o.comparison_margin_text
  from official_rows o
  join oversea_runner_source_id m
    on m.race_source = 'jra'
    and m.kaisai_nen = '2026'
    and m.kaisai_tsukihi = '0816'
    and m.keibajo_code = 'A8'
    and m.race_bango = '04'
    and m.umaban::integer = o.umaban
    and m.source = 'jra-van'
  join oversea_horse_race_history h
    on h.source = m.source
    and h.source_horse_id = m.source_horse_id
    and h.race_date = o.race_date::date
)
insert into oversea_horse_race_history_supplement (
  history_id,
  supplement_source,
  source_url,
  captured_at,
  country_or_jra_venue_label,
  grade,
  field_size,
  gate_number,
  popularity,
  weight_carried_kg,
  race_time_text,
  race_time_seconds,
  race_time_parse_status,
  going,
  corner_positions_text,
  comparison_horse_name,
  comparison_margin_text
)
select
  history_id,
  'jra_official',
  'https://www.jra.go.jp/JRADB/accessSD.html?CNAME=pk01dde0112720260101041/73',
  '2026-08-15T14:25:00+09:00'::timestamptz,
  country_or_jra_venue_label,
  grade,
  field_size,
  gate_number,
  popularity,
  weight_carried_kg,
  race_time_text,
  race_time_seconds,
  race_time_parse_status,
  going,
  corner_positions_text,
  comparison_horse_name,
  comparison_margin_text
from matched_history
on conflict (history_id, supplement_source) do nothing;
