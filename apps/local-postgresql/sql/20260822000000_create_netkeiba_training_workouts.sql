-- Additive local raw storage for authenticated netkeiba training workouts.
-- The JVD-compatible columns intentionally mirror jvd_hc/jvd_wc naming while
-- source race identity and provenance remain explicit. Re-runs are safe.

begin;

create table if not exists netkeiba_training_workouts (
  kaisai_nen varchar(4) not null,
  kaisai_tsukihi varchar(4) not null,
  keibajo_code varchar(2) not null,
  race_bango varchar(2) not null,
  source_race_id varchar(12) not null check (source_race_id ~ '^[0-9]{12}$'),
  umaban varchar(2) not null,
  ketto_toroku_bango varchar(10) not null,
  workout_key varchar(64) not null check (workout_key ~ '^[0-9a-f]{64}$'),
  record_id varchar(2) not null default 'NK',
  data_kubun varchar(1) not null default '1',
  data_sakusei_nengappi varchar(8) not null,
  tracen_kubun varchar(1),
  chokyo_nengappi varchar(8) not null,
  chokyo_jikoku varchar(4) not null default '',
  course text,
  babamawari text,
  training_type text not null,
  workout_index integer not null check (workout_index >= 1),
  bamei text,
  rider_name text,
  time_gokei_10f varchar(4),
  lap_time_10f varchar(3),
  time_gokei_9f varchar(4),
  lap_time_9f varchar(3),
  time_gokei_8f varchar(4),
  lap_time_8f varchar(3),
  time_gokei_7f varchar(4),
  lap_time_7f varchar(3),
  time_gokei_6f varchar(4),
  lap_time_6f varchar(3),
  time_gokei_5f varchar(4),
  lap_time_5f varchar(3),
  time_gokei_4f varchar(4),
  lap_time_4f varchar(3),
  time_gokei_3f varchar(4),
  lap_time_3f varchar(3),
  time_gokei_2f varchar(4),
  lap_time_2f varchar(3),
  lap_time_1f varchar(3),
  evaluation_grade text,
  evaluation_text text,
  comment_text text,
  source_url text,
  fetched_at timestamptz not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    ketto_toroku_bango,
    workout_key
  )
);

create index if not exists netkeiba_training_workouts_race_umaban_idx
  on netkeiba_training_workouts (
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    umaban
  );

create index if not exists netkeiba_training_workouts_ketto_date_idx
  on netkeiba_training_workouts (ketto_toroku_bango, chokyo_nengappi desc);

commit;
