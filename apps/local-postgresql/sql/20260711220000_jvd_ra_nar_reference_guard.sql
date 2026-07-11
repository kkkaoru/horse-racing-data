-- Guard local PG jvd_ra against ongoing NAR-reference-record contamination inflow.
--
-- Root cause (investigated 2026-07-11): jvd_ra is populated by PC-KEIBA Database
-- (Com.Pckeiba.Database.exe), a closed-source Windows app on a Parallels VM that
-- connects directly to this Mac's exposed PostgreSQL port and performs its own
-- internal writes -- there is no in-repo importer to patch. No agent code (build-
-- corner-feature-table.ts, running-style-feature-sql.ts, finish_position_features_duckdb.py)
-- writes to jvd_ra; they only read it. This trigger is the only reachable fix point.
--
-- Besides the authoritative JRA "RA" (race) record lifecycle (data_kubun 7/9,
-- keibajo_code 01-10), JV-Link also ships a secondary reference "RA" subtype that
-- PC-KEIBA's importer drops into jvd_ra without checking data_kubun:
--   - data_kubun = 'A': NAR reference records (abbreviated race names, some blank
--     fields, batch-created on scattered dates) for NAR venues -- keibajo_code is
--     purely numeric and outside 01-10 (30-61 observed). A large subset (62,116 of
--     95,799 rows on 2026-07-11) exactly duplicates the authoritative nvd_ra race
--     key; the remainder shares the identical contamination fingerprint but has no
--     nvd_ra match (nvd_ra coverage gaps for those race-dates), so it is NOT safe
--     to key the guard off "already exists in nvd_ra" -- data_kubun is the correct,
--     complete discriminator.
--   - data_kubun = 'B': legitimate overseas-race reference records (Milano Gran
--     Premio, Grand Prix de Saint-Cloud, etc.), keibajo_code alphanumeric (A4, B6,
--     C0, ...). These must NOT be blocked.
--
-- Verified as a 100%-clean, table-wide partition over the full 238,862-row jvd_ra
-- on 2026-07-11 (zero exceptions in either direction):
--   data_kubun IN ('7','9')  <=> keibajo_code BETWEEN '01' AND '10'        (JRA, 139,535 rows)
--   data_kubun = 'A'         <=> keibajo_code ~ '^[0-9]{2}$' AND NOT BETWEEN '01' AND '10'  (NAR reference, 95,799 rows)
--   data_kubun = 'B'         <=> keibajo_code NOT ~ '^[0-9]{2}$'          (overseas reference, 3,491 rows)
--
-- A CHECK constraint is deliberately avoided (it would abort the writer's whole
-- batch on the first contaminated row). This BEFORE INSERT trigger instead returns
-- NULL for contaminated rows so PC-KEIBA's batch insert continues uninterrupted,
-- silently skipping only the NAR-reference rows. Existing rows are untouched --
-- cleanup of the already-landed 62,116/95,799 rows is a separate, explicitly
-- authorized delete operation owned by a sibling task.

begin;

create or replace function public.jvd_ra_skip_nar_reference_rows()
returns trigger
language plpgsql
as $$
begin
  if new.data_kubun = 'A'
     and new.keibajo_code ~ '^[0-9]{2}$'
     and new.keibajo_code not between '01' and '10' then
    return null;
  end if;
  return new;
end;
$$;

drop trigger if exists jvd_ra_skip_nar_reference_rows_trg on public.jvd_ra;

create trigger jvd_ra_skip_nar_reference_rows_trg
  before insert on public.jvd_ra
  for each row
  execute function public.jvd_ra_skip_nar_reference_rows();

commit;
