-- Repair fields that the preliminary netkeiba card could not resolve.
-- Only verified existing JV jockey masters are used as fixed-width codes.
-- Owner names are display data from the canonical identity table; owner codes
-- remain placeholders. Foreign affiliation is repaired only for known foreign
-- trainers in this race, while the two Japanese trainers retain affiliation 1.

begin;

update jvd_se se
set
  kishu_code = verified.kishu_code,
  kishumei_ryakusho = verified.kishumei_ryakusho
from (
  select
    mapping.kaisai_nen,
    mapping.kaisai_tsukihi,
    mapping.keibajo_code,
    mapping.race_bango,
    mapping.umaban,
    ks.kishu_code,
    ks.kishumei_ryakusho
  from oversea_runner_source_id mapping
  join jvd_ks ks on ks.kishu_code = mapping.source_jockey_id
  where
    mapping.source = 'netkeiba'
    and mapping.kaisai_nen = '2026'
    and mapping.kaisai_tsukihi = '0816'
    and mapping.keibajo_code = 'A8'
    and mapping.race_bango = '04'
) verified
where
  se.kaisai_nen = verified.kaisai_nen
  and se.kaisai_tsukihi = verified.kaisai_tsukihi
  and se.keibajo_code = verified.keibajo_code
  and se.race_bango = verified.race_bango
  and se.umaban = verified.umaban
  and btrim(se.kishu_code) = '00000';

update jvd_se se
set
  chokyoshi_code = verified.chokyoshi_code,
  chokyoshimei_ryakusho = verified.chokyoshimei_ryakusho
from (
  select
    mapping.kaisai_nen,
    mapping.kaisai_tsukihi,
    mapping.keibajo_code,
    mapping.race_bango,
    mapping.umaban,
    ch.chokyoshi_code,
    ch.chokyoshimei_ryakusho
  from oversea_runner_source_id mapping
  join jvd_ch ch on ch.chokyoshi_code = mapping.source_trainer_id
  where
    mapping.source = 'netkeiba'
    and mapping.kaisai_nen = '2026'
    and mapping.kaisai_tsukihi = '0816'
    and mapping.keibajo_code = 'A8'
    and mapping.race_bango = '04'
) verified
where
  se.kaisai_nen = verified.kaisai_nen
  and se.kaisai_tsukihi = verified.kaisai_tsukihi
  and se.keibajo_code = verified.keibajo_code
  and se.race_bango = verified.race_bango
  and se.umaban = verified.umaban
  and btrim(se.chokyoshi_code) = '00000';

update jvd_se se
set banushimei = left(identity.owner_name_full, 64)
from oversea_runner_identity identity
where
  se.kaisai_nen = identity.kaisai_nen
  and se.kaisai_tsukihi = identity.kaisai_tsukihi
  and se.keibajo_code = identity.keibajo_code
  and se.race_bango = identity.race_bango
  and se.umaban = identity.umaban
  and identity.race_source = 'jra'
  and identity.kaisai_nen = '2026'
  and identity.kaisai_tsukihi = '0816'
  and identity.keibajo_code = 'A8'
  and identity.race_bango = '04'
  and btrim(coalesce(se.banushimei, '')) = ''
  and btrim(coalesce(identity.owner_name_full, '')) <> '';

update jvd_se
set tozai_shozoku_code = '4'
where
  kaisai_nen = '2026'
  and kaisai_tsukihi = '0816'
  and keibajo_code = 'A8'
  and race_bango = '04'
  and umaban in ('01', '02', '05', '06', '07', '08', '09', '10')
  and btrim(tozai_shozoku_code) in ('', '0');

commit;
