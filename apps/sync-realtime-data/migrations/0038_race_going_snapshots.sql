-- Pre-race going (馬場状態) observed on the race-day 出馬表 page, one row per
-- race surface. Side record for finish-position serving: catalog RA rows are
-- never mutated; serving coalesces this only when RA carries no going.
CREATE TABLE IF NOT EXISTS race_going_snapshots (
  race_key TEXT NOT NULL,
  surface TEXT NOT NULL CHECK (surface IN ('turf', 'dirt')),
  going TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  PRIMARY KEY (race_key, surface)
);
