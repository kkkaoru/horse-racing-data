// This logic-free entrypoint runs with Bun.
import { createProductionCliRuntime, runProductionCli } from "./production-cli";

try {
  console.log(
    JSON.stringify(
      await runProductionCli({
        argv: Bun.argv.slice(2),
        env: process.env,
        runtime: createProductionCliRuntime(),
      }),
    ),
  );
} catch {
  console.error(
    "Production operator failed. Check arguments, manifest integrity, and tool authorization; do not blindly retry apply.",
  );
  process.exitCode = 1;
}
