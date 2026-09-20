// Runs with bun. One-shot migration CLI; all batching and mapping are tested in src/.
import { readFile, rename, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { neon } from "@neondatabase/serverless";
import {
  parseVectorBackfillCheckpoint,
  submitVectorBackfillBatch,
  type VectorBackfillCheckpoint,
} from "../src/vector-backfill";
import { CORNER_MIGRATION_SQL } from "../src/vector-migration";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const required = (value: string | undefined): string => {
  if (!value) throw new Error("Missing vector migration argument or credential");
  return value;
};

const run = async (): Promise<void> => {
  const args = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      namespace: { type: "string" },
      checkpoint: { type: "string" },
      batches: { type: "string" },
      "batch-size": { type: "string" },
      "token-file": { type: "string" },
      endpoint: { type: "string" },
    },
  }).values;
  const namespace: string = required(args.namespace);
  const checkpointPath: string = required(args.checkpoint);
  const batchLimit: number = Number(required(args.batches));
  const batchSize: number = Number(required(args["batch-size"]));
  const endpoint: URL = new URL(required(args.endpoint));
  if (
    endpoint.protocol !== "https:" ||
    endpoint.hostname !== "pc-keiba-r2-catalog.kaoru.workers.dev"
  )
    throw new Error("Invalid migration destination");
  if (
    !/^[a-zA-Z0-9_-]{1,64}$/.test(namespace) ||
    !Number.isSafeInteger(batchLimit) ||
    batchLimit < 1
  )
    throw new Error("Invalid migration arguments");
  const token: string = (await readFile(required(args["token-file"]), "utf8")).trim();
  if (!token) throw new Error("Missing migration token");
  const sql = neon(required(process.env.NEON_PRIMARY_URL));
  const initial: VectorBackfillCheckpoint = {
    namespace,
    cursor: { source: "", year: "", monthDay: "", venue: "", race: "", horse: "" },
    submittedRows: 0,
    lastMutationId: null,
    phase: "submitting",
  };
  const progress: { state: VectorBackfillCheckpoint; batches: number } = {
    state: initial,
    batches: 0,
  };
  try {
    const value: unknown = JSON.parse(await readFile(checkpointPath, "utf8"));
    progress.state = parseVectorBackfillCheckpoint(value, namespace);
  } catch (error: unknown) {
    if (!isRecord(error) || error.code !== "ENOENT") throw error;
  }
  while (progress.batches < batchLimit && progress.state.phase === "submitting") {
    progress.state = await submitVectorBackfillBatch({
      batchSize,
      state: progress.state,
      dependencies: {
        load: async (parameters) => {
          const rows: unknown = await sql.query(CORNER_MIGRATION_SQL, [...parameters]);
          if (!Array.isArray(rows) || !rows.every(isRecord))
            throw new Error("Invalid migration source rows");
          return rows;
        },
        upsert: async (vectors) => {
          const response: Response = await fetch(endpoint, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
            body: JSON.stringify({ namespace, vectors }),
            signal: AbortSignal.timeout(60000),
          });
          if (response.status !== 202)
            throw new Error(`Vector migration target returned HTTP ${response.status}`);
          const receipt: unknown = await response.json();
          if (!isRecord(receipt) || typeof receipt.mutationId !== "string")
            throw new Error("Invalid migration receipt");
          return { mutationId: receipt.mutationId };
        },
        checkpoint: async (state) => {
          await writeFile(`${checkpointPath}.next`, JSON.stringify(state), { mode: 0o600 });
          await rename(`${checkpointPath}.next`, checkpointPath);
        },
      },
    });
    progress.batches += 1;
    console.log(
      JSON.stringify({
        submittedRows: progress.state.submittedRows,
        phase: progress.state.phase,
        batches: progress.batches,
        lastMutationId: progress.state.lastMutationId,
      }),
    );
  }
};

await run().catch(() => {
  console.error(
    "Vector backfill stopped; retain checkpoint and inspect source/target health before resuming.",
  );
  process.exitCode = 1;
});
