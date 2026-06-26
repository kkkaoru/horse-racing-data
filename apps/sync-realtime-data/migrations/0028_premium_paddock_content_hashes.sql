-- Migration: 0028_premium_paddock_content_hashes
--
-- `fetchAndStorePremiumRaceData` rewrites the per-race stable_comments and
-- training_reviews rows on every poll. During the 20-minute hot window the
-- planner re-queues the same race every minute, which churns ~14 horses x 2
-- tables x ~20 cycles of DELETE+INSERT even when the scraped HTML hasn't
-- changed since the last fetch. We cache a SHA-1 content hash of the
-- {stableComments, trainingReviews} payload here so the writer can skip the
-- DELETE+INSERT when the hash matches the previous run.

CREATE TABLE IF NOT EXISTS premium_paddock_content_hashes (
  race_key TEXT PRIMARY KEY,
  content_hash TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
