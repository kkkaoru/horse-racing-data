ALTER TABLE premium_stable_comments ADD COLUMN evaluation_grade INTEGER;

CREATE INDEX IF NOT EXISTS idx_premium_stable_comments_grade
  ON premium_stable_comments (race_key, evaluation_grade, horse_number);
