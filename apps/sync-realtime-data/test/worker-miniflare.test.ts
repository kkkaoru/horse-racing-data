import { build } from "esbuild";
import { mkdir, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Miniflare } from "miniflare";
import {
  claimPremiumPaddockNotificationSend,
  listPremiumRaceDataFetchCandidatesByDate,
} from "../src/storage";

const TEST_NOW = "2026-05-12T03:00:00.000Z";
const TEST_DATE = "20260512";
const TEST_QUEUE = "sync-realtime-data-jobs";
const TEST_PREMIUM_QUEUE = "sync-realtime-data-premium-race-jobs";

let db: D1Database;
let mf: Miniflare;
let tempDir: string;
let worker: {
  fetch: (request: string) => Promise<Response>;
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
  const raceKeySegments = raceKey.split(":");
  const keibajoCode = raceKeySegments[3] ?? (source === "jra" ? "08" : "55");
  const raceBango = raceKeySegments[4] ?? "01";
  await db
    .prepare(
      `
        insert into realtime_race_sources (
          race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          baba_code, kaisai_kai, kaisai_nichime, race_start_at_jst, race_name, deba_url, odds_links_json,
          discovered_at, updated_at, last_odds_fetch_at, last_odds_queued_at,
          odds_fetch_lock_until, last_weight_fetch_at, last_result_fetch_at, result_complete_at
        )
        values (?, ?, '2026', '0512', ?, ?, ?, '01', '05', ?, 'test race',
          'https://example.test/deba', '{}', ?, ?, ?, ?, ?, null, ?, ?)
      `,
    )
    .bind(
      raceKey,
      source,
      keibajoCode,
      raceBango,
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
          build.onResolve({ filter: /^\.\/finish-position-lite-pool$/ }, () => ({
            namespace: "stub-finish-position-lite-pool",
            path: "finish-position-lite-pool",
          }));
          build.onResolve({ filter: /^\.\/running-style-feature-parquet$/ }, () => ({
            namespace: "stub-running-style-feature-parquet",
            path: "running-style-feature-parquet",
          }));
          build.onResolve({ filter: /^@cloudflare\/playwright$/ }, () => ({
            namespace: "stub-playwright",
            path: "playwright",
          }));
          build.onResolve({ filter: /^\.\/keiba-go$/ }, () => ({
            namespace: "stub-keiba-go",
            path: "keiba-go",
          }));
          build.onLoad({ filter: /.*/, namespace: "stub-postgres" }, () => ({
            contents:
              "export const fetchJraRacesByDate = async () => []; export const fetchNarRacesByDate = async () => [];",
            loader: "js",
          }));
          build.onLoad({ filter: /.*/, namespace: "stub-finish-position-lite-pool" }, () => ({
            contents:
              "export const getFinishPositionPool = () => { throw new Error('finish-position pool unavailable in test'); };",
            loader: "js",
          }));
          build.onLoad({ filter: /.*/, namespace: "stub-running-style-feature-parquet" }, () => ({
            contents: `
              export const buildRunningStyleFeatureParquetKey = () => "";
              export const loadRunningStyleFeatureParquet = async () => [];
              export const putRunningStyleFeatureParquet = async () => 0;
              export const runningStyleParquetVerificationKey = () => "";
              export const validateFeatureCoverage = () => ({ missingCells: 0, missingFeatureNames: [] });
            `,
            loader: "js",
          }));
          build.onLoad({ filter: /.*/, namespace: "stub-playwright" }, () => ({
            contents:
              "export const launch = async () => { throw new Error('playwright unavailable in test'); };",
            loader: "js",
          }));
          build.onLoad({ filter: /.*/, namespace: "stub-keiba-go" }, () => ({
            contents: `
              export const BABA_CODE_TO_LOCAL_KEIBAJO = {};
              export const buildRaceListUrl = () => "";
              export const buildRaceResultUrl = () => "";
              export const buildRaceKey = () => "";
              export const extractOddsLinks = () => [];
              export const fetchOdds = async () => null;
              export const fetchRaceLinksFromRaceList = async () => [];
              export const fetchRacePage = async () => null;
              export const fetchTodayRaceListUrls = async () => [];
              export const parseRaceMetadata = () => null;
              export const parseRaceEntries = () => [];
              export const parseHorseWeights = () => [];
              export const parseRaceEntryHorseNumbers = () => [];
              export const parseRaceResultExcludedHorseNumbers = () => [];
              export const parseRaceResults = () => [];
              export const parseRaceResultHorseWeights = () => [];
            `,
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
  await applySqlFile(join(root, "migrations/0020_premium_data_top_horses.sql"));
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
    delete from premium_data_top_horses;
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

    const planLog = await db
      .prepare("select count(*) as count from fetch_logs where job_type = 'plan-realtime-fetches'")
      .first<{
        count: number;
      }>();
    expect(planLog?.count).toBe(1);
  }, 20_000);

  it("seeds the realtime planner watchdog from API traffic when stale", async () => {
    const response = await worker.fetch("https://example.test/health");

    expect(response.status).toBe(200);
    await response.text();
    await new Promise((resolve) => setTimeout(resolve, 100));
    const planLog = await db
      .prepare(
        `
          select job_type, status, message
          from fetch_logs
          where job_type in ('plan-realtime-fetches', 'plan-realtime-fetches-self')
          order by rowid desc
          limit 2
        `,
      )
      .all<{ job_type: string; message: string | null; status: string }>();
    expect(planLog.results).toEqual([
      {
        job_type: "plan-realtime-fetches-self",
        message: "0 jobs queued",
        status: "ok",
      },
      {
        job_type: "plan-realtime-fetches",
        message: "0 jobs queued",
        status: "ok",
      },
    ]);
  });

  it("runs scheduled JRA premium link discovery for the next race day", async () => {
    await expect(
      worker.scheduled({
        cron: "0 4 * * 5",
        scheduledTime: new Date("2026-05-15T04:00:00.000Z"),
      }),
    ).resolves.toMatchObject({
      outcome: "ok",
    });

    const log = await db
      .prepare(
        `
          select job_type, status, message
          from fetch_logs
          order by rowid desc
          limit 1
        `,
      )
      .first<{ job_type: string; message: string | null; status: string }>();
    expect(log).toMatchObject({
      job_type: "discover-premium-race-links",
      status: "ok",
    });
    expect(log?.message).toContain('"configured":false');
  }, 20_000);

  it("runs scheduled JRA premium training fetch planning for the next race day", async () => {
    await expect(
      worker.scheduled({
        cron: "0 5 * * 6",
        scheduledTime: new Date("2026-05-16T05:00:00.000Z"),
      }),
    ).resolves.toMatchObject({
      outcome: "ok",
    });

    const log = await db
      .prepare(
        `
          select job_type, status, message
          from fetch_logs
          order by rowid desc
          limit 1
        `,
      )
      .first<{ job_type: string; message: string | null; status: string }>();
    expect(log).toMatchObject({
      job_type: "plan-premium-race-data-fetches",
      status: "ok",
    });
    expect(log?.message).toContain('"queued":0');
  }, 20_000);

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

  it("queues incomplete result fetches that are past the throttle interval", async () => {
    // RESULT_FETCH_INTERVAL_MINUTES is 2 — a race polled 1 minute ago is still
    // throttled, but one polled 5 minutes ago is due again. Now = 12:00 JST.
    await seedRace("nar:2026:0512:55:05", "2026-05-12T11:50:00+09:00", {
      lastResultFetchAt: "2026-05-12T11:59:00+09:00",
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

  it("requeues stale premium race data when last fetch is more than 30 minutes before race start", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T15:40:00+09:00", { source: "jra" });
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
          insert into premium_race_data_fetch_state (
            race_key, status, last_fetch_at, updated_at
          )
          values (?, 'ok', '2026-05-11T08:56:21+09:00', '2026-05-11T08:56:21+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();
    await db
      .prepare(
        `
          insert into premium_data_top_horses (
            race_key, source_race_id, fetched_at, rank, horse_number, horse_name, reasons_json, created_at
          )
          values (?, '202605120801', '2026-05-11T08:56:21+09:00', 1, '1', 'sample', '[]', '2026-05-11T08:56:21+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-premium-race-data-freshness",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const state = await db
      .prepare(
        `
          select status, last_queued_at
          from premium_race_data_fetch_state
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

  it("does not requeue premium race data when last fetch is close to race start", async () => {
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
          insert into premium_race_data_fetch_state (
            race_key, status, last_fetch_at, updated_at
          )
          values (?, 'ok', '2026-05-12T11:55:00+09:00', '2026-05-12T11:55:00+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();
    await db
      .prepare(
        `
          insert into premium_data_top_horses (
            race_key, source_race_id, fetched_at, rank, horse_number, horse_name, reasons_json, created_at
          )
          values (?, '202605120801', '2026-05-12T11:55:00+09:00', 1, '1', 'sample', '[]', '2026-05-12T11:55:00+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-premium-race-data-fresh",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const state = await db
      .prepare(
        `
          select status, last_queued_at
          from premium_race_data_fetch_state
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

  it("does not requeue premium race data after the race start has clearly passed", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T11:00:00+09:00", { source: "jra" });
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
          insert into premium_race_data_fetch_state (
            race_key, status, last_fetch_at, updated_at
          )
          values (?, 'ok', '2026-05-11T08:56:21+09:00', '2026-05-11T08:56:21+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();
    await db
      .prepare(
        `
          insert into premium_data_top_horses (
            race_key, source_race_id, fetched_at, rank, horse_number, horse_name, reasons_json, created_at
          )
          values (?, '202605120801', '2026-05-11T08:56:21+09:00', 1, '1', 'sample', '[]', '2026-05-11T08:56:21+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();

    await worker.queue(TEST_QUEUE, [
      {
        attempts: 1,
        body: { date: TEST_DATE, type: "plan-realtime-fetches" },
        id: "plan-premium-race-data-after-start",
        timestamp: new Date(TEST_NOW),
      },
    ]);

    const state = await db
      .prepare(
        `
          select status, last_queued_at
          from premium_race_data_fetch_state
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

  it("requeues premium race data stuck in 'queued' status for more than 15 minutes", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T13:00:00+09:00", { source: "jra" });
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
          insert into premium_race_data_fetch_state (
            race_key, status, last_queued_at, updated_at
          )
          values (?, 'queued', '2026-05-12T11:40:00+09:00', '2026-05-12T11:40:00+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();
    const candidates = await listPremiumRaceDataFetchCandidatesByDate(
      db,
      TEST_DATE,
      "2026-05-12T12:00:00+09:00",
    );
    expect(candidates.map((row) => row.raceKey)).toStrictEqual(["jra:2026:0512:08:01"]);
  });

  it("does not requeue premium race data still 'queued' within the 15-minute grace window", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T13:00:00+09:00", { source: "jra" });
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
          insert into premium_race_data_fetch_state (
            race_key, status, last_queued_at, updated_at
          )
          values (?, 'queued', '2026-05-12T11:55:00+09:00', '2026-05-12T11:55:00+09:00')
        `,
      )
      .bind("jra:2026:0512:08:01")
      .run();
    const candidates = await listPremiumRaceDataFetchCandidatesByDate(
      db,
      TEST_DATE,
      "2026-05-12T12:00:00+09:00",
    );
    expect(candidates).toStrictEqual([]);
  });

  it("orders premium race data candidates by proximity of race start to the supplied 'now'", async () => {
    await seedRace("jra:2026:0512:08:11", "2026-05-12T13:30:00+09:00", { source: "jra" });
    await seedRace("jra:2026:0512:08:12", "2026-05-12T12:30:00+09:00", { source: "jra" });
    await seedRace("jra:2026:0512:08:13", "2026-05-12T16:00:00+09:00", { source: "jra" });
    await seedRace("jra:2026:0512:08:14", "2026-05-12T11:00:00+09:00", { source: "jra" });
    for (const raceKey of [
      "jra:2026:0512:08:11",
      "jra:2026:0512:08:12",
      "jra:2026:0512:08:13",
      "jra:2026:0512:08:14",
    ]) {
      await db
        .prepare(
          `
            insert into premium_race_links (
              race_key, source_race_id, entry_url, discovered_at, updated_at
            )
            values (?, ?, 'https://example.test/race', ?, ?)
          `,
        )
        .bind(raceKey, raceKey.replace(/:/g, ""), TEST_NOW, TEST_NOW)
        .run();
    }
    const candidates = await listPremiumRaceDataFetchCandidatesByDate(
      db,
      TEST_DATE,
      "2026-05-12T12:00:00+09:00",
    );
    expect(candidates.map((row) => row.raceKey)).toStrictEqual([
      "jra:2026:0512:08:12",
      "jra:2026:0512:08:14",
      "jra:2026:0512:08:11",
      "jra:2026:0512:08:13",
    ]);
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

  it("claims only one premium paddock notification send per race at a time", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T12:20:00+09:00", { source: "jra" });

    await expect(
      claimPremiumPaddockNotificationSend(db, {
        lockBefore: "2026-05-12T11:58:30+09:00",
        payloadFetchedAt: "2026-05-12T12:00:00+09:00",
        payloadSignature: "signature-a",
        raceKey: "jra:2026:0512:08:01",
        sendAttemptAt: "2026-05-12T12:00:00+09:00",
      }),
    ).resolves.toBe(true);
    await expect(
      claimPremiumPaddockNotificationSend(db, {
        lockBefore: "2026-05-12T11:58:40+09:00",
        payloadFetchedAt: "2026-05-12T12:00:10+09:00",
        payloadSignature: "signature-b",
        raceKey: "jra:2026:0512:08:01",
        sendAttemptAt: "2026-05-12T12:00:10+09:00",
      }),
    ).resolves.toBe(false);

    const state = await db
      .prepare(
        `
          select status, payload_signature, last_send_attempt_at, last_notified_at
          from premium_paddock_notification_state
          where race_key = ?
        `,
      )
      .bind("jra:2026:0512:08:01")
      .first<{
        last_notified_at: string | null;
        last_send_attempt_at: string | null;
        payload_signature: string | null;
        status: string;
      }>();
    expect(state).toEqual({
      last_notified_at: null,
      last_send_attempt_at: "2026-05-12T12:00:00+09:00",
      payload_signature: "signature-a",
      status: "sending",
    });
  });

  it("does not claim premium paddock notification sends after a race was notified", async () => {
    await seedRace("jra:2026:0512:08:01", "2026-05-12T12:20:00+09:00", { source: "jra" });
    await db
      .prepare(
        `
          insert into premium_paddock_notification_state (
            race_key, status, payload_signature, last_payload_fetched_at,
            last_send_attempt_at, last_notified_at, updated_at
          )
          values (?, 'ok', 'signature-a', ?, ?, ?, ?)
        `,
      )
      .bind(
        "jra:2026:0512:08:01",
        "2026-05-12T12:00:00+09:00",
        "2026-05-12T12:00:00+09:00",
        "2026-05-12T12:00:00+09:00",
        "2026-05-12T12:00:00+09:00",
      )
      .run();

    await expect(
      claimPremiumPaddockNotificationSend(db, {
        lockBefore: "2026-05-12T12:01:30+09:00",
        payloadFetchedAt: "2026-05-12T12:03:00+09:00",
        payloadSignature: "signature-b",
        raceKey: "jra:2026:0512:08:01",
        sendAttemptAt: "2026-05-12T12:03:00+09:00",
      }),
    ).resolves.toBe(false);

    const state = await db
      .prepare(
        `
          select status, payload_signature, last_notified_at
          from premium_paddock_notification_state
          where race_key = ?
        `,
      )
      .bind("jra:2026:0512:08:01")
      .first<{
        last_notified_at: string | null;
        payload_signature: string | null;
        status: string;
      }>();
    expect(state).toEqual({
      last_notified_at: "2026-05-12T12:00:00+09:00",
      payload_signature: "signature-a",
      status: "ok",
    });
  });
});
