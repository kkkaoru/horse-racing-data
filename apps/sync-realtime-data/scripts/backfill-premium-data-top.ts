// Run with:
//   CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE=postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing \
//     bun run apps/sync-realtime-data/scripts/backfill-premium-data-top.ts --date 20260523

import { getPlatformProxy } from "wrangler";

import { handleJob } from "../src/worker";

const parseArgs = (): { date: string; delayMs: number } => {
  const dateIndex = process.argv.indexOf("--date");
  const delayIndex = process.argv.indexOf("--delay-ms");
  const date = dateIndex >= 0 ? process.argv[dateIndex + 1] : "20260523";
  const delayMs = delayIndex >= 0 ? Number(process.argv[delayIndex + 1]) : 3000;
  if (!date || !/^\d{8}$/u.test(date)) {
    throw new Error(
      "Usage: bun run scripts/backfill-premium-data-top.ts --date YYYYMMDD [--delay-ms 3000]",
    );
  }
  if (!Number.isFinite(delayMs) || delayMs < 0) {
    throw new Error("--delay-ms must be a non-negative number");
  }
  return { date, delayMs };
};

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

const main = async (): Promise<void> => {
  const { date, delayMs } = parseArgs();
  const { dispose, env } = await getPlatformProxy<{ REALTIME_DB: D1Database }>({
    configPath: new URL("../wrangler.jsonc", import.meta.url).pathname,
    remoteBindings: true,
  });
  try {
    const totalRow = await env.REALTIME_DB.prepare(
      "select count(*) as count from realtime_race_sources where source = 'jra'",
    ).first<{ count: number }>();
    console.log(`[backfill-premium-data-top] remote jra race sources=${totalRow?.count ?? 0}`);

    console.log(`[backfill-premium-data-top] discover links for ${date}`);
    await handleJob(env, { date, type: "discover-premium-races" });

    const rows = await env.REALTIME_DB.prepare(
      `
        select race_key
        from realtime_race_sources
        where source = 'jra'
          and kaisai_nen = ?
          and kaisai_tsukihi = ?
          and keibajo_code in ('04', '05', '06', '07', '08')
        order by keibajo_code, race_bango
      `,
    )
      .bind(date.slice(0, 4), date.slice(4, 8))
      .all<{ race_key: string }>();

    const raceKeys = rows.results.map((row) => row.race_key);
    console.log(`[backfill-premium-data-top] fetch ${raceKeys.length} races (delay=${delayMs}ms)`);
    let ok = 0;
    let failed = 0;
    for (const [index, raceKey] of raceKeys.entries()) {
      if (index > 0 && delayMs > 0) {
        await sleep(delayMs);
      }
      try {
        await handleJob(env, { raceKey, type: "fetch-premium-race-data" });
        ok += 1;
        process.stdout.write(".");
      } catch (error) {
        failed += 1;
        console.error(`\n[backfill-premium-data-top] failed ${raceKey}:`, error);
      }
    }
    console.log(`\n[backfill-premium-data-top] done ok=${ok} failed=${failed}`);
  } finally {
    await dispose();
  }
};

await main();
