// Runs with bun via Vitest; actual consumer functions execute with real workerd D1 capture.
import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { expect, test } from "vitest";
import { buildD1CapturePlan } from "./d1-change-capture";
import { discoverD1CaptureSchema } from "./d1-capture-schema";

const schemas = [
  {
    table: "realtime_plan_recovery_claims",
    sql: "CREATE TABLE realtime_plan_recovery_claims (claim_key TEXT PRIMARY KEY, owner_token TEXT, claimed_at TEXT, expires_at TEXT)",
  },
  {
    table: "realtime_race_sources",
    sql: "CREATE TABLE realtime_race_sources (race_key TEXT PRIMARY KEY, last_weight_fetch_at TEXT, last_weight_fetch_attempt_at TEXT, last_weight_fetch_soft_miss_at TEXT, updated_at TEXT, result_fetch_lock_until TEXT, result_complete_at TEXT)",
  },
  {
    table: "jra_track_condition_fetch_state",
    sql: "CREATE TABLE jra_track_condition_fetch_state (kaisai_nen TEXT, kaisai_tsukihi TEXT, keibajo_code TEXT, updated_at TEXT, fetch_lock_until TEXT, PRIMARY KEY(kaisai_nen, kaisai_tsukihi, keibajo_code))",
  },
  {
    table: "premium_paddock_notification_state",
    sql: "CREATE TABLE premium_paddock_notification_state (race_key TEXT PRIMARY KEY, status TEXT, payload_signature TEXT, last_payload_fetched_at TEXT, last_send_attempt_at TEXT, last_notified_at TEXT, skip_reason TEXT, message TEXT, updated_at TEXT)",
  },
  {
    table: "odds_fetch_state",
    sql: "CREATE TABLE odds_fetch_state (race_key TEXT PRIMARY KEY, odds_fetch_lock_until TEXT, updated_at TEXT)",
  },
];

test("actual realtime and hot claims retain contention, retry and owner fencing with capture", async () => {
  const recovery = new URL(
    "../../sync-realtime-data/src/realtime-plan-recovery-claim.ts",
    import.meta.url,
  ).pathname;
  const storage = new URL("../../sync-realtime-data/src/storage.ts", import.meta.url).pathname;
  const hot = new URL("../../sync-realtime-data-hot/src/storage.ts", import.meta.url).pathname;
  const bundle = await build({
    stdin: {
      loader: "js",
      resolveDir: process.cwd(),
      contents: `
import { claimRealtimePlanRecovery, releaseRealtimePlanRecovery } from ${JSON.stringify(recovery)};
import { claimWeightFetch, claimReservedWeightFetch, claimResultFetch, claimTrackConditionFetch, claimPremiumPaddockNotificationSend } from ${JSON.stringify(storage)};
import { claimOddsFetch } from ${JSON.stringify(hot)};
export default { async fetch(request, env) {
  const db = env.DB;
  const now = '2026-09-16T12:00:00+09:00';
  const future = '2026-09-16T12:05:00+09:00';
  const past = '2026-09-16T11:58:30+09:00';
  const claim = { db, claimKey: 'probe', claimedAt: now, expiresAt: future };
  const recovery = [
    await claimRealtimePlanRecovery({ ...claim, ownerToken: 'a' }),
    await claimRealtimePlanRecovery({ ...claim, ownerToken: 'b' }),
    await releaseRealtimePlanRecovery({ db, claimKey: 'probe', ownerToken: 'b' }),
    await releaseRealtimePlanRecovery({ db, claimKey: 'probe', ownerToken: 'a' }),
    await claimRealtimePlanRecovery({ ...claim, ownerToken: 'b' }),
  ];
  const weights = [
    await claimWeightFetch(db, 'race', now, past),
    await claimWeightFetch(db, 'race', now, past),
    await claimReservedWeightFetch(db, 'race', now, '2026-09-16T12:00:01+09:00'),
    await claimReservedWeightFetch(db, 'race', now, '2026-09-16T12:00:01+09:00'),
  ];
  const results = [await claimResultFetch(db, 'race', future, now), await claimResultFetch(db, 'race', future, now)];
  const trackInput = { date: '20260916', keibajoCode: '05', now, lockUntil: future };
  const track = [await claimTrackConditionFetch(db, trackInput), await claimTrackConditionFetch(db, trackInput)];
  const paddockInput = { raceKey: 'race', payloadSignature: 'sig', payloadFetchedAt: now, sendAttemptAt: now, lockBefore: past };
  const retryInput = { ...paddockInput, sendAttemptAt: '2026-09-16T12:06:00+09:00', lockBefore: future };
  const paddock = [
    await claimPremiumPaddockNotificationSend(db, paddockInput),
    await claimPremiumPaddockNotificationSend(db, paddockInput),
    await claimPremiumPaddockNotificationSend(db, retryInput),
    await claimPremiumPaddockNotificationSend(db, retryInput),
  ];
  const odds = [await claimOddsFetch(db, 'race', future, now), await claimOddsFetch(db, 'race', future, now)];
  return Response.json({ recovery, weights, results, track, paddock, odds });
} };`,
    },
    bundle: true,
    format: "esm",
    platform: "neutral",
    write: false,
    external: ["cloudflare:workers", "node:crypto"],
    logLevel: "silent",
  });
  const output = bundle.outputFiles[0];
  if (output === undefined) throw new Error("Missing claim fixture bundle");
  const runtime = new Miniflare({
    modules: true,
    script: output.text,
    compatibilityDate: "2026-06-18",
    compatibilityFlags: ["nodejs_compat"],
    d1Databases: { DB: "claim-capture-test" },
  });
  try {
    const db = await runtime.getD1Database("DB");
    await db.batch(schemas.map(({ sql }) => db.prepare(sql)));
    await db.prepare("INSERT INTO realtime_race_sources(race_key) VALUES ('race')").run();
    await db.prepare("INSERT INTO odds_fetch_state(race_key) VALUES ('race')").run();
    const plans = await Promise.all(
      schemas.map(async ({ table }) => {
        const schema = await discoverD1CaptureSchema(
          table,
          async ({ sql, params }) =>
            (
              await db
                .prepare(sql)
                .bind(...params)
                .all<Record<string, unknown>>()
            ).results,
        );
        return buildD1CapturePlan({ ...schema, captureId: "claims-test" });
      }),
    );
    await db.batch(
      plans
        .flatMap((plan, position) => [
          ...(position === 0 ? [plan.createOutbox] : []),
          ...plan.triggers.map(({ sql }) => sql),
        ])
        .map((sql) => db.prepare(sql)),
    );
    const response = await runtime.dispatchFetch("https://claims.test");
    expect(response.status).toBe(200);
    expect(await response.json()).toStrictEqual({
      recovery: [true, false, false, true, true],
      weights: [true, false, true, false],
      results: [true, false],
      track: [true, false],
      paddock: [true, false, true, false],
      odds: [true, false],
    });
  } finally {
    await runtime.dispose();
  }
}, 20000);
