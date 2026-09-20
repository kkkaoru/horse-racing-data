// run with: bun run test -- src/realtime-plan-recovery-claim.test.ts
import { expect, it, vi } from "vitest";

const buildDb = (returnedRows: number) => {
  const unexpected = (): never => {
    throw new Error("Unexpected D1 test operation");
  };
  const bind = vi.fn<D1PreparedStatement["bind"]>();
  const statement: D1PreparedStatement = {
    all: unexpected,
    bind,
    first: unexpected,
    raw: unexpected,
    run: unexpected,
  };
  const all = vi.spyOn(statement, "all").mockResolvedValue({
    success: true,
    results: returnedRows > 0 ? [{ changed: 1 }] : [],
    // A trigger may add writes even when the original mutation returns no rows.
    meta: {
      changes: 3,
      changed_db: true,
      duration: 0,
      last_row_id: 1,
      rows_read: 1,
      rows_written: 3,
      size_after: 1,
    },
  });
  bind.mockReturnValue(statement);
  const prepare = vi.fn<D1Database["prepare"]>().mockReturnValue(statement);
  const db: D1Database = {
    prepare,
    batch: unexpected,
    exec: unexpected,
    dump: unexpected,
    withSession: unexpected,
  };
  return { all, bind, db, prepare };
};

it("claimRealtimePlanRecovery atomically acquires an absent or expired claim", async () => {
  const { claimRealtimePlanRecovery } = await import("./realtime-plan-recovery-claim");
  const { all, bind, db, prepare } = buildDb(1);

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
  expect(all).toHaveBeenCalledTimes(1);
  expect(prepare).toHaveBeenCalledWith(expect.stringMatching(/returning 1 as changed/u));
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
  expect(prepare).toHaveBeenCalledWith(expect.stringMatching(/returning 1 as changed/u));
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
