#!/usr/bin/env bun
// Run with bun run import:jra -- --input <Wine export> --from-date YYYY-MM-DD --to-date YYYY-MM-DD [--apply].
import { parseRaceImportOptions, runRaceImport } from "../src/import-race-records";

console.log(
  JSON.stringify(
    await runRaceImport(
      parseRaceImportOptions(process.argv.slice(2), process.env.DATABASE_URL_LOCAL),
    ),
  ),
);
