import { build } from "esbuild";
import { mkdir, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Miniflare } from "miniflare";

const TEST_NOW = "2026-05-12T03:00:00.000Z";
const TEST_DATE = "20260512";
const TEST_QUEUE = "sync-realtime-data-jobs";

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
  } = {},
): Promise<void> => {
  await db
    .prepare(
      `
        insert into nar_race_sources (
          race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          baba_code, race_start_at_jst, race_name, deba_url, odds_links_json,
          discovered_at, updated_at, last_odds_fetch_at, last_odds_queued_at,
          odds_fetch_lock_until, last_weight_fetch_at
        )
        values (?, 'nar', '2026', '0512', '55', '01', '22', ?, 'test race',
          'https://example.test/deba', '{}', ?, ?, ?, ?, ?, null)
      `,
    )
    .bind(
      raceKey,
      raceStartAtJst,
      TEST_NOW,
      TEST_NOW,
      options.lastOddsFetchAt ?? null,
      options.lastOddsQueuedAt ?? null,
      options.oddsFetchLockUntil ?? null,
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
          build.onLoad({ filter: /.*/, namespace: "stub-postgres" }, () => ({
            contents:
              "export const fetchNarRacesByDate = async () => { throw new Error('postgres is not used in Miniflare scheduling tests'); };",
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
      REALTIME_TEST_NOW: TEST_NOW,
    },
    compatibilityDate: "2026-05-10",
    compatibilityFlags: ["nodejs_compat", "global_fetch_strictly_public"],
    d1Databases: {
      REALTIME_DB: "realtime-test-db",
    },
    modules: true,
    queueProducers: {
      REALTIME_JOBS: TEST_QUEUE,
    },
    scriptPath: bundlePath,
  });
  db = await mf.getD1Database("REALTIME_DB");
  await applySqlFile(join(root, "migrations/0001_init.sql"));
  await applySqlFile(join(root, "migrations/0002_odds_fetch_state.sql"));
  worker = (await mf.getWorker()) as typeof worker;
});

beforeEach(async () => {
  await db.exec(`
    delete from fetch_logs;
    delete from odds_snapshots;
    delete from horse_weight_snapshots;
    delete from nar_race_sources;
  `);
});

afterAll(async () => {
  await mf.dispose();
  await rm(tempDir, { force: true, recursive: true });
});

describe("worker scheduling with Miniflare", () => {
  it("dispatches scheduled events without processing realtime work inline", async () => {
    await expect(worker.scheduled({ cron: "* 1-12 * * *" })).resolves.toMatchObject({
      outcome: "ok",
    });

    const logCount = await db.prepare("select count(*) as count from fetch_logs").first<{
      count: number;
    }>();
    expect(logCount?.count).toBe(0);
  });

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
          from nar_race_sources
          where race_key = ?
        `,
      )
      .bind("nar:2026:0512:55:01")
      .first<{ last_odds_queued_at: string | null; odds_fetch_lock_until: string | null }>();
    expect(race?.last_odds_queued_at).toBe("2026-05-12T12:00:00+09:00");
    expect(race?.odds_fetch_lock_until).toBeNull();
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
          from nar_race_sources
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
});
