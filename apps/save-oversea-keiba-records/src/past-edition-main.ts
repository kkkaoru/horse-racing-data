// This entrypoint runs with Bun. Local PostgreSQL only; production goes through the race request.
import { runPastEdition } from "./run-past-edition";
import { createPostgresClient, resolvePostgresConfig } from "./storage/pg-client";

const client = createPostgresClient({ config: resolvePostgresConfig(process.env) });
try {
  await client.connect();
  await runPastEdition({
    argv: Bun.argv.slice(2),
    env: process.env,
    ports: {
      readFile: (path: string): Promise<string> => Bun.file(path).text(),
      executor: client,
      withTransaction: (callback) => client.withTransaction(callback),
      log: (message: string): void => {
        console.log(message);
      },
    },
  });
} catch (error: unknown) {
  console.error(error instanceof Error ? error.message : "Past edition import failed.");
  process.exitCode = 1;
} finally {
  await client.end();
}
