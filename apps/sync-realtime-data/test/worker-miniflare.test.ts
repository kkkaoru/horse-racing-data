import { build } from "esbuild";
import { mkdir, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Miniflare } from "miniflare";

const TEST_NOW = "2026-05-12T03:00:00.000Z";
const TEST_DATE = "20260512";
const TEST_QUEUE = "sync-realtime-data-jobs";
const TEST_PREMIUM_QUEUE = "sync-realtime-data-premium-race-jobs";

let db: D1Database;
let mf: Miniflare;
let tempDir: string;
let worker: {
  queue: (
    queueName: string,
    messages: Array<{ attempts: number; body: unknown; id: string; timestamp: Date }>,
  ) => Promise<unknown>;
  scheduled: (options: { cron: string; scheduledTime?: Date }) => Promise<unknown>;
};

const root = fileURLToPath(new URL("..", import.meta.url));

const applySqlFile = async (path: string): Promise<void> => {
  const statements = (await readFile(path, "utf8"))
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
  for (const statement of statements) {
    await db.exec(`${statement.replace(/\s+/g, " ")};`);
  }
};

const seedRace = async (
  raceKey: string,
  raceStartAtJst: string,
  options: {
    lastOddsFetchAt?: string | null;
    lastOddsQueuedAt?: string | null;
    oddsFetchLockUntil?: string | null;
    resultCompleteAt?: string | null;
    lastResultFetchAt?: string | null;
    source?: "jra" | "nar";
  } = {},
): Promise<void> => {
  const source = options.source ?? "nar";
  await db
    .prepare(
      `
        insert into realtime_race_sources (
          race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          baba_code, kaisai_kai, kaisai_nichime, race_start_at_jst, race_name, deba_url, odds_links_json,
          discovered_at, updated_at, last_odds_fetch_at, last_odds_queued_at,
          odds_fetch_lock_until, last_weight_fetch_at, last_result_fetch_at, result_complete_at
        )
        values (?, ?, '2026', '0512', ?, '01', ?, '01', '05', ?, 'test race',
          'https://example.test/deba', '{}', ?, ?, ?, ?, ?, null, ?, ?)
      `,
    )
    .bind(
      raceKey,
      source,
      source === "jra" ? "08" : "55",
      source === "jra" ? "08" : "22",
      raceStartAtJst,
      TEST_NOW,
      TEST_NOW,
      options.lastOddsFetchAt ?? null,
      options.lastOddsQueuedAt ?? null,
      options.oddsFetchLockUntil ?? null,
      options.lastResultFetchAt ?? null,
      options.resultCompleteAt ?? null,
    )
    .run();
};

beforeAll(async () => {
  tempDir = join(root, ".wrangler/miniflare-tests");
  await mkdir(tempDir, { recursive: true });
  const bundlePath = join(tempDir, "worker.mjs");
  await build({
    bundle: true,
    entryPoints: [join(root, "src/index.ts")],
    format: "esm",
    outfile: bundlePath,
    platform: "node",
    plugins: [
      {
        name: "stub-postgres",
        setup(build) {
          build.onResolve({ filter: /^\.\/postgres$/ }, () => ({
            namespace: "stub-postgres",
            path: "postgres",
          }));
          build.onResolve({ filter: /^@cloudflare\/playwright$/ }, () => ({
            namespace: "stub-playwright",
            path: "playwright",
          }));
          build.onLoad({ filter: /.*/, namespace: "stub-postgres" }, () => ({
            contents:
              "export const fetchJraRacesByDate = async () => []; export const fetchNarRacesByDate = async () => [];",
            loader: "js",
          }));
          build.onLoad({ filter: /.*/, namespace: "stub-playwright" }, () => ({
            contents:
              "export const launch = async () => { throw new Error('playwright unavailable in test'); };",
            loader: "js",
          }));
        },
      },
    ],
    target: "es2022",
  });

  mf = new Miniflare({
    bindings: {
      DATABASE_TARGET: "cloudflare",
      PREMIUM_RACE_ORIGIN: "https://example.test",
      REALTIME_TEST_NOW: TEST_NOW,
    },
    compatibilityDate: "2026-05-10",
    compatibilityFlags: ["nodejs_compat", "global_fetch_strictly_public"],
    d1Databases: {
      REALTIME_DB: "realtime-test-db",
    },
    modules: true,
    queueProducers: {
      PREMIUM_RACE_JOBS: TEST_PREMIUM_QUEUE,
      REALTIME_JOBS: TEST_QUEUE,
    },
    scriptPath: bundlePath,
  });
  db = await mf.getD1Database("REALTIME_DB");
  await applySqlFile(join(root, "migrations/0001_init.sql"));
  await applySqlFile(join(root, "migrations/0002_odds_fetch_state.sql"));
  await applySqlFile(join(root, "migrations/0003_race_results.sql"));
  await applySqlFile(join(root, "migrations/0004_result_completion.sql"));
  await applySqlFile(join(root, "migrations/0005_race_entry_snapshots.sql"));
  await applySqlFile(join(root, "migrations/0006_realtime_race_sources.sql"));
  await applySqlFile(join(root, "migrations/0007_jra_track_conditions.sql"));
  await applySqlFile(join(root, "migrations/0008_jra_race_keys.sql"));
  await applySqlFile(join(root, "migrations/0009_premium_race_data.sql"));
  await applySqlFile(join(root, "migrations/0010_premium_race_data_fetch_state.sql"));
  await applySqlFile(join(root, "migrations/0011_premium_stable_comment_grade.sql"));
  await applySqlFile(join(root, "migrations/0012_premium_training_rider.sql"));
  await applySqlFile(join(root, "migrations/0013_premium_paddock_notification_state.sql"));
  await applySqlFile(join(root, "migrations/0014_premium_paddock_notification_audit.sql"));
  await applySqlFile(join(root, "migrations/0015_premium_paddock_notification_events.sql"));
  await applySqlFile(join(root, "migrations/0016_jra_realtime_source_race_dates.sql"));
  worker = (await mf.getWorker()) as unknown as typeof worker;
});

beforeEach(async () => {
  await db.exec(`
    delete from fetch_logs;
    delete from premium_paddock_notification_events;
    delete from premium_paddock_notification_state;
    delete from premium_paddock_fetch_state;
    delete from premium_race_data_fetch_state;
    delete from premium_paddock_bulletins;
    delete from premium_stable_comments;
    delete from premium_training_reviews;
    delete from premium_race_links;
    delete from odds_snapshots;
    delete from race_entry_snapshots;
    delete from race_result_snapshots;
    delete from horse_weight_snapshots;
    delete from jra_track_condition_snapshots;
    delete from jra_track_condition_fetch_state;
    delete from realtime_race_sources;
  `);
});

afterAll(async () => {
  await mf.dispose();
  await rm(tempDir, { force: true, recursive: true });
});

describe("worker scheduling with Miniflare", () => {
  it("processes scheduled planning inline", async () => {
    await expect(worker.scheduled({ cron: "* 1-12 * * *" })).resolves.toMatchObject({
      outcome: "ok",
    });

    const logCount = await db.prepare("select count(*) as count from fetch_logs").first<{
      count: number;
    }>();
    expect(logCount?.count).toBe(1);
  }, 20_000);

  it("marks due races queued through the queue planner", async () => {
    await seedRace("nar:2026:0512:55:01", "2026-05-12T13:10:00+09:00");

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-1",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const race = await db
      .prepare(
        `
          select last_odds_queued_at, odds_fetch_lock_until
          from realtime_race_sources
          where race_key = ?
        `,
      )
      .bind("nar:2026:0512:55:01")
      .first<{ last_odds_queued_at: string | null; odds_fetch_lock_until: string | null }>();
    expect(race?.last_odds_queued_at).toBe("2026-05-12T12:00:00+09:00");
    expect(race?.odds_fetch_lock_until).toBeNull();
  });

  it("queues finished races for result fetches and skips completed results", async () => {
    await seedRace("nar:2026:0512:55:03", "2026-05-12T11:55:00+09:00");
    await seedRace("jra:2026:0512:08:03", "2026-05-12T11:55:00+09:00", {
      source: "jra",
    });
    await seedRace("nar:2026:0512:55:04", "2026-05-12T11:50:00+09:00", {
      resultCompleteAt: "2026-05-12T11:58:00+09:00",
    });

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-results-1",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const queued = await db
      .prepare(
        `
          select race_key, last_result_queued_at
          from realtime_race_sources
          where race_key in (?, ?, ?)
          order by race_key
        `,
      )
      .bind("jra:2026:0512:08:03", "nar:2026:0512:55:03", "nar:2026:0512:55:04")
      .all<{ last_result_queued_at: string | null; race_key: string }>();
    expect(queued.results).toEqual([
      {
        last_result_queued_at: "2026-05-12T12:00:00+09:00",
        race_key: "jra:2026:0512:08:03",
      },
      {
        last_result_queued_at: "2026-05-12T12:00:00+09:00",
        race_key: "nar:2026:0512:55:03",
      },
      {
        last_result_queued_at: null,
        race_key: "nar:2026:0512:55:04",
      },
    ]);
  });

  it("queues incomplete result fetches every five minutes", async () => {
    await seedRace("nar:2026:0512:55:05", "2026-05-12T11:50:00+09:00", {
      lastResultFetchAt: "2026-05-12T11:56:00+09:00",
    });
    await seedRace("nar:2026:0512:55:06", "2026-05-12T11:50:00+09:00", {
      lastResultFetchAt: "2026-05-12T11:55:00+09:00",
    });

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-results-2",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const queued = await db
      .prepare(
        `
          select race_key, last_result_queued_at
          from realtime_race_sources
          where race_key in (?, ?)
          order by race_key
        `,
      )
      .bind("nar:2026:0512:55:05", "nar:2026:0512:55:06")
      .all<{ last_result_queued_at: string | null; race_key: string }>();
    expect(queued.results).toEqual([
      {
        last_result_queued_at: null,
        race_key: "nar:2026:0512:55:05",
      },
      {
        last_result_queued_at: "2026-05-12T12:00:00+09:00",
        race_key: "nar:2026:0512:55:06",
      },
    ]);
  });

  it("clears queued and lock state when a stale odds job is no longer due", async () => {
    await seedRace("nar:2026:0512:55:02", "2026-05-12T12:05:00+09:00", {
      lastOddsFetchAt: "2026-05-12T11:59:30+09:00",
      lastOddsQueuedAt: "2026-05-12T11:59:00+09:00",
      oddsFetchLockUntil: null,
    });

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { raceKey: "nar:2026:0512:55:02", type: "fetch-odds" },
        id: "odds-1",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const race = await db
      .prepare(
        `
          select last_odds_fetch_at, last_odds_queued_at, odds_fetch_lock_until
          from realtime_race_sources
          where race_key = ?
        `,
      )
      .bind("nar:2026:0512:55:02")
      .first<{
        last_odds_fetch_at: string | null;
        last_odds_queued_at: string | null;
        odds_fetch_lock_until: string | null;
      }>();
    expect(race?.last_odds_fetch_at).toBe("2026-05-12T11:59:30+09:00");
    expect(race?.last_odds_queued_at).toBeNull();
    expect(race?.odds_fetch_lock_until).toBeNull();
  });

  it("queues one JRA track condition job per venue on a thirty-minute slot", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T10:00:00+09:00", { source: "jra" });
    await seedRace("jra:2026:0512:08:12", "2026-05-12T16:30:00+09:00", { source: "jra" });

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-track-condition-1",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const state = await db
      .prepare(
        `
          select kaisai_nen, kaisai_tsukihi, keibajo_code, last_queued_at
          from jra_track_condition_fetch_state
        `,
      )
      .first<{
        kaisai_nen: string;
        kaisai_tsukihi: string;
        keibajo_code: string;
        last_queued_at: string | null;
      }>();
    expect(state).toEqual({
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      last_queued_at: "2026-05-12T12:00:00+09:00",
    });
  });

  it("queues premium paddock fetches in the race-window and rechecks saved bulletins", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T12:20:00+09:00", { source: "jra" });
    await db
      .prepare(
        `
          insert into premium_race_links (
            race_key, source_race_id, entry_url, discovered_at, updated_at
          )
          values (?, '202605120801', 'https://example.test/race', ?, ?)
        `,
      )
      .bind("jra:2026:0512:08:01", TEST_NOW, TEST_NOW)
      .run();
    await db
      .prepare(
        `
          insert into premium_paddock_fetch_state (
            race_key, status, last_fetch_at, updated_at
          )
          values (?, 'ok', '2026-05-12T11:56:30+09:00', '2026-05-12T11:56:30+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();
    await db
      .prepare(
        `
          insert into premium_race_data_fetch_state (
            race_key, status, last_fetch_at, updated_at
          )
          values (?, 'ok', ?, ?)
        `,
      )
      .bind("jra:2026:0512:08:01", TEST_NOW, TEST_NOW)
      .run();

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-premium-paddock-1",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const state = await db
      .prepare(
        `
          select status, last_queued_at
          from premium_paddock_fetch_state
          where race_key = ?
        `,
      )
      .bind("jra:2026:0512:08:01")
      .first<{ last_queued_at: string | null; status: string }>();
    expect(state).toEqual({
      last_queued_at: "2026-05-12T12:00:00+09:00",
      status: "queued",
    });
  });

  it("does not requeue premium paddock fetches inside the recheck interval", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T12:20:00+09:00", { source: "jra" });
    await db
      .prepare(
        `
          insert into premium_race_links (
            race_key, source_race_id, entry_url, discovered_at, updated_at
          )
          values (?, '202605120801', 'https://example.test/race', ?, ?)
        `,
      )
      .bind("jra:2026:0512:08:01", TEST_NOW, TEST_NOW)
      .run();
    await db
      .prepare(
        `
          insert into premium_paddock_fetch_state (
            race_key, status, last_fetch_at, updated_at
          )
          values (?, 'ok', '2026-05-12T11:58:00+09:00', '2026-05-12T11:58:00+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();
    await db
      .prepare(
        `
          insert into premium_race_data_fetch_state (
            race_key, status, last_fetch_at, updated_at
          )
          values (?, 'ok', ?, ?)
        `,
      )
      .bind("jra:2026:0512:08:01", TEST_NOW, TEST_NOW)
      .run();

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-premium-paddock-2",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const state = await db
      .prepare(
        `
          select status, last_queued_at
          from premium_paddock_fetch_state
          where race_key = ?
        `,
      )
      .bind("jra:2026:0512:08:01")
      .first<{ last_queued_at: string | null; status: string }>();
    expect(state).toEqual({
      last_queued_at: null,
      status: "ok",
    });
  });

  it("does not requeue missed premium paddock notifications after the race window", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T11:50:00+09:00", { source: "jra" });
    await db
      .prepare(
        `
          insert into premium_race_links (
            race_key, source_race_id, entry_url, discovered_at, updated_at
          )
          values (?, '202605120801', 'https://example.test/race', ?, ?)
        `,
      )
      .bind("jra:2026:0512:08:01", TEST_NOW, TEST_NOW)
      .run();
    await db
      .prepare(
        `
          insert into premium_paddock_fetch_state (
            race_key, status, message, retry_after, updated_at
          )
          values (?, 'failed', 'premium race fetch failed: proxy: 500', ?, ?)
        `,
      )
      .bind("jra:2026:0512:08:01", "2026-05-12T11:59:00+09:00", TEST_NOW)
      .run();

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-premium-paddock-recovery",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const state = await db
      .prepare(
        `
          select status, last_queued_at
          from premium_paddock_fetch_state
          where race_key = ?
        `,
      )
      .bind("jra:2026:0512:08:01")
      .first<{ last_queued_at: string | null; status: string }>();
    expect(state).toEqual({
      last_queued_at: null,
      status: "failed",
    });
  });
});
