// This entrypoint runs with Bun. Partial collection is never a successful publication.
import { createHistoryCliRuntime, type HistoryCliReport } from "./history-cli";
import { runHistoryCommand } from "./history-command";
import type { HistoryPublicationReport } from "./history-publication-operator";
import type { SourceIdMirrorReport } from "./source-id-mirror";
import type { EventEntitiesReport } from "./event-entities";
import type { PedigreeStoreReport } from "./pedigree-store";
import type { PostPositionReport } from "./post-position-store";
import type { RaceTimeStoreReport } from "./race-time-store";
import type { RacingPostTimesReport } from "./racing-post-times";
import type { JvRaceTimesReport } from "./jv-race-times";
import type { JraCardRaceTimesReport } from "./jra-card-race-times";

try {
  const result:
    | HistoryCliReport
    | HistoryPublicationReport
    | SourceIdMirrorReport
    | EventEntitiesReport
    | PedigreeStoreReport
    | PostPositionReport
    | RaceTimeStoreReport
    | RacingPostTimesReport
    | JvRaceTimesReport
    | JraCardRaceTimesReport = await runHistoryCommand({
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
