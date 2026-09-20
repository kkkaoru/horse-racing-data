// This entrypoint runs with Bun. Partial collection is never a successful publication.
import { createHistoryCliRuntime, type HistoryCliReport } from "./history-cli";
import { runHistoryCommand } from "./history-command";
import type { HistoryPublicationReport } from "./history-publication-operator";

try {
  const result: HistoryCliReport | HistoryPublicationReport = await runHistoryCommand({
    argv: Bun.argv.slice(2),
    env: process.env,
    runtime: createHistoryCliRuntime(),
  });
  console.log(JSON.stringify(result));
  if (result.status === "paused") process.exitCode = 2;
  if (result.status === "blocked") process.exitCode = 1;
} catch {
  console.error(
    "History operator failed. Inspect the private plan and archive; never treat missing or partial data as complete.",
  );
  process.exitCode = 1;
}
