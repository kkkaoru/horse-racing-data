// Run with:
// ODDS_R2_CATALOG_TOKEN=... \
// bun run --filter sync-realtime-data-hot provision:odds-r2-catalog

import { buildDefaultProvisionConfig, provisionOddsR2Catalog } from "./provision-odds-r2-catalog";
import type { CommandRunner } from "./verify-odds-r2-cutover";

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

const result = await provisionOddsR2Catalog(buildDefaultProvisionConfig(runCommand, process.env));
for (const check of result.checks) {
  console.log(`${check.ok ? "ok" : "ng"} ${check.name}: ${check.detail}`);
}
if (!result.ok) {
  process.exitCode = 1;
}
