// run with: bun run test -- src/realtime-plan-recovery-claim.test.ts
import { expect, it, vi } from "vitest";

const buildDb = (changes: number) => {
  const run = vi.fn(async () => ({ meta: { changes } }));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  return { bind, db: { prepare } as unknown as D1Database, prepare, run };
};

it("claimRealtimePlanRecovery atomically acquires an absent or expired claim", async () => {
  const { claimRealtimePlanRecovery } = await import("./realtime-plan-recovery-claim");
  const { bind, db, prepare, run } = buildDb(1);

  const claimed = await claimRealtimePlanRecovery({
    claimedAt: "2026-08-24T09:30:00+09:00",
    claimKey: "plan-realtime-fetches-recovery:20260824",
    db,
    expiresAt: "2026-08-24T09:35:00+09:00",
    ownerToken: "owner-a",
  });

  expect(claimed).toBe(true);
  expect(prepare).toHaveBeenCalledWith(expect.stringMatching(/on conflict\(claim_key\)/u));
  expect(prepare).toHaveBeenCalledWith(
    expect.stringMatching(/expires_at <= excluded\.claimed_at/u),
  );
  expect(bind).toHaveBeenCalledWith(
    "plan-realtime-fetches-recovery:20260824",
    "owner-a",
    "2026-08-24T09:30:00+09:00",
    "2026-08-24T09:35:00+09:00",
  );
  expect(run).toHaveBeenCalledTimes(1);
});

it("claimRealtimePlanRecovery rejects a concurrently owned active claim", async () => {
  const { claimRealtimePlanRecovery } = await import("./realtime-plan-recovery-claim");
  const { db } = buildDb(0);

  const claimed = await claimRealtimePlanRecovery({
    claimedAt: "2026-08-24T09:30:00+09:00",
    claimKey: "plan-realtime-fetches-recovery:20260824",
    db,
    expiresAt: "2026-08-24T09:35:00+09:00",
    ownerToken: "owner-b",
  });

  expect(claimed).toBe(false);
});

it("releaseRealtimePlanRecovery rolls back only the matching owner claim", async () => {
  const { releaseRealtimePlanRecovery } = await import("./realtime-plan-recovery-claim");
  const { bind, db, prepare } = buildDb(1);

  const released = await releaseRealtimePlanRecovery({
    claimKey: "plan-realtime-fetches-recovery:20260824",
    db,
    ownerToken: "owner-a",
  });

  expect(released).toBe(true);
  expect(prepare).toHaveBeenCalledWith(expect.stringMatching(/delete from/u));
  expect(prepare).toHaveBeenCalledWith(expect.stringMatching(/owner_token = \?/u));
  expect(bind).toHaveBeenCalledWith("plan-realtime-fetches-recovery:20260824", "owner-a");
});

it("releaseRealtimePlanRecovery reports false for a stale owner token", async () => {
  const { releaseRealtimePlanRecovery } = await import("./realtime-plan-recovery-claim");
  const { db } = buildDb(0);

  const released = await releaseRealtimePlanRecovery({
    claimKey: "plan-realtime-fetches-recovery:20260824",
    db,
    ownerToken: "stale-owner",
  });

  expect(released).toBe(false);
});
