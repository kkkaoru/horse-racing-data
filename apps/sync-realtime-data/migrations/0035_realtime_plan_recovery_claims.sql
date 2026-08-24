CREATE TABLE IF NOT EXISTS realtime_plan_recovery_claims (
  claim_key TEXT PRIMARY KEY,
  owner_token TEXT NOT NULL,
  claimed_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_realtime_plan_recovery_claims_expires_at
  ON realtime_plan_recovery_claims(expires_at);
