-- Cached netkeiba pedigree AJAX responses for the ten final runners.

insert into oversea_horse_pedigree (
  source,
  source_horse_id,
  sire_source_id,
  sire_name,
  sire_sire_source_id,
  sire_sire_name,
  dam_source_id,
  dam_name,
  dam_sire_source_id,
  dam_sire_name,
  source_url
)
values
  ('netkeiba', '000a02d639', '000a013801', 'Night of Thunder', '000a0115e2', 'Dubawi', '000a02d63c', 'Rhea', '000a012056', 'Siyouni', 'https://db.netkeiba.com/horse/ped/000a02d639/'),
  ('netkeiba', '000a029c22', '000a012caf', 'Adlerflug', '000a001cd4', 'In the Wings', '000a029c21', 'Game Theory', '000a0116bf', 'Aussie Rules', 'https://db.netkeiba.com/horse/ped/000a029c22/'),
  ('netkeiba', '2021105724', '2010105827', 'キズナ', '2002100816', 'ディープインパクト', '000a014412', 'フィンレイズラッキーチャーム', '000a01316b', 'Twirling Candy', 'https://db.netkeiba.com/horse/ped/2021105724/'),
  ('netkeiba', '2021105744', '2011100655', 'モーリス', '2004103328', 'スクリーンヒーロー', '2003102993', 'ブルーメンブラット', '1996107396', 'アドマイヤベガ', 'https://db.netkeiba.com/horse/ped/2021105744/'),
  ('netkeiba', '000a02d00c', '000a013801', 'Night of Thunder', '000a0115e2', 'Dubawi', '000a02d00b', 'Buying Trouble', '2001103018', 'ハットトリック', 'https://db.netkeiba.com/horse/ped/000a02d00c/'),
  ('netkeiba', '000a027210', '000a0115e2', 'Dubawi', '000a0022d1', 'Dubai Millennium', '000a02720d', 'Lunch Lady', '000a010c92', 'Shamardal', 'https://db.netkeiba.com/horse/ped/000a027210/'),
  ('netkeiba', '000a02d629', '000a0122b4', 'Dark Angel', '000a0122b0', 'Acclamation', '000a01c300', 'Lastroseofsummer', '000a010530', 'Haafhd', 'https://db.netkeiba.com/horse/ped/000a02d629/'),
  ('netkeiba', '000a02ca97', '000a012696', 'Sea The Moon', '000a011c78', 'Sea The Stars', '000a029ec7', 'Rayisa', '000a011ab8', 'Holy Roman Emperor', 'https://db.netkeiba.com/horse/ped/000a02ca97/'),
  ('netkeiba', '000a02d63e', '000a01a7ad', 'St Mark''s Basilica', '000a012056', 'Siyouni', '000a02d63d', 'Too Soon To Panic', '000a0128fa', 'Gleneagles', 'https://db.netkeiba.com/horse/ped/000a02d63e/'),
  ('netkeiba', '000a02ca51', '000a013dbf', 'Starspangledbanner', '000a010d4c', 'Choisir', '000a02ca50', 'Way To My Heart', '000a00232b', 'Galileo', 'https://db.netkeiba.com/horse/ped/000a02ca51/')
on conflict (source, source_horse_id)
do update set
  sire_source_id = excluded.sire_source_id,
  sire_name = excluded.sire_name,
  sire_sire_source_id = excluded.sire_sire_source_id,
  sire_sire_name = excluded.sire_sire_name,
  dam_source_id = excluded.dam_source_id,
  dam_name = excluded.dam_name,
  dam_sire_source_id = excluded.dam_sire_source_id,
  dam_sire_name = excluded.dam_sire_name,
  source_url = excluded.source_url,
  updated_at = now();
