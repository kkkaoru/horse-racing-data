// Run with:
// ODDS_R2_VERIFY_RACE_KEYS=nar:2026:0708:30:11,nar:2026:0708:45:12 \
// WRANGLER_R2_SQL_AUTH_TOKEN=... \
// bun run apps/sync-realtime-data-hot/scripts/run-verify-odds-r2-cutover.ts

import {
  buildDefaultConfig,
  verifyOddsR2Cutover,
  type CommandRunner,
} from "./verify-odds-r2-cutover";

const runCommand: CommandRunner = async (args, env) => {
  const proc = Bun.spawn(args, {
    env: { ...process.env, ...env },
    stderr: "pipe",
    stdout: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { code, stderr, stdout };
};

const result = await verifyOddsR2Cutover(
  buildDefaultConfig(runCommand, globalThis.fetch, process.env),
);
for (const check of result.checks) {
  console.log(`${check.ok ? "ok" : "ng"} ${check.name}: ${check.detail}`);
}
if (!result.ok) {
  process.exitCode = 1;
}
