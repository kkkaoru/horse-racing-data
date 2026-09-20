// run with: bun run test -- src/realtime-plan-recovery-claim.test.ts

interface ClaimRealtimePlanRecoveryInput {
  claimKey: string;
  claimedAt: string;
  db: D1Database;
  expiresAt: string;
  ownerToken: string;
}

interface ReleaseRealtimePlanRecoveryInput {
  claimKey: string;
  db: D1Database;
  ownerToken: string;
}

export const claimRealtimePlanRecovery = async (
  input: ClaimRealtimePlanRecoveryInput,
): Promise<boolean> => {
  const result = await input.db
    .prepare(
      `
        insert into realtime_plan_recovery_claims (
          claim_key,
          owner_token,
          claimed_at,
          expires_at
        ) values (?, ?, ?, ?)
        on conflict(claim_key) do update set
          owner_token = excluded.owner_token,
          claimed_at = excluded.claimed_at,
          expires_at = excluded.expires_at
        where realtime_plan_recovery_claims.expires_at <= excluded.claimed_at
        returning 1 as changed
      `,
    )
    .bind(input.claimKey, input.ownerToken, input.claimedAt, input.expiresAt)
    .all<{ changed: number }>();
  // BEFORE capture triggers can write on a rejected claim; count only returned source rows.
  return result.results.length > 0;
};

export const releaseRealtimePlanRecovery = async (
  input: ReleaseRealtimePlanRecoveryInput,
): Promise<boolean> => {
  const result = await input.db
    .prepare(
      `
        delete from realtime_plan_recovery_claims
        where claim_key = ?
          and owner_token = ?
        returning 1 as changed
      `,
    )
    .bind(input.claimKey, input.ownerToken)
    .all<{ changed: number }>();
  return result.results.length > 0;
};
