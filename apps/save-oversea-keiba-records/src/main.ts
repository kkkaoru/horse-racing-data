// This logic-free entrypoint runs with Bun.
import { createRealCliDependencies, runCli } from "./cli";

const exitCode: number = await runCli({
  argv: Bun.argv.slice(2),
  env: process.env,
  dependencies: createRealCliDependencies(),
});
process.exitCode = exitCode;
