-- Create race_entry_finish_vectors: pgvector store for the finish-position
-- vector ensemble member (P1 of the approved finish-position-accuracy plan).
--
-- Each row is one finished race entry embedded into a vector(256) space. At
-- inference time a candidate entry is matched against historical rows of the
-- same class_code whose race_date is STRICTLY earlier (no leakage), then the
-- neighbours' relevance is aggregated into the ensemble. relevance encodes the
-- finishing bucket (3=win, 2=2nd, 1=3rd, 0=other) so a kNN vote doubles as a
-- soft ranking signal.
--
-- Indexing is deliberately a single btree prefilter index, NOT hnsw/ivfflat:
-- the prefilter (embed_version, class_code, race_date desc) narrows each query
-- to a small same-class slice, over which brute-force exact vector distance was
-- measured faster (and exact) than an approximate ANN index. The INCLUDE
-- columns let the prefilter range scan stay index-only for the kyori /
-- keibajo_code filters that ride alongside class_code.
--
-- The pgvector extension is enabled once at DB init via
-- initdb/001-enable-vector.sql (mirroring race_entry_corner_features, whose
-- migration likewise does not re-create the extension), so this migration only
-- adds the table + index.
--
-- Idempotent — re-runs are safe (every object uses ``if not exists``). Additive
-- only: per feedback_no_data_delete this migration never DELETEs, TRUNCATEs or
-- DROPs.

begin;

create table if not exists race_entry_finish_vectors (
  embed_version smallint not null,
  source text not null default 'jra',
  race_id text not null,                  -- 'jra:YYYY:MMDD:KK:RR'
  ketto_toroku_bango text not null,
  umaban integer,
  race_date text not null,                -- yyyymmdd, used by the STRICT < predicate
  class_code text,                        -- kyoso_joken_code from jvd_ra join
  keibajo_code text,
  track_code text,
  kyori integer,
  finish_position smallint,               -- only finished entries are indexed
  relevance smallint not null default 0,  -- 3=win, 2=2nd, 1=3rd, 0=other
  embedding vector(256) not null,
  created_at timestamptz not null default now(),
  primary key (embed_version, race_id, ketto_toroku_bango)
);

create index if not exists refv_prefilter_idx
  on race_entry_finish_vectors (embed_version, class_code, race_date desc)
  include (kyori, keibajo_code);

commit;
