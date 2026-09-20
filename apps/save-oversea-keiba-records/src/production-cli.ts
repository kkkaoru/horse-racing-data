// This module runs with Bun. It submits scoped requests to the existing Worker, not replicas.
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile, access } from "node:fs/promises";
import { join } from "node:path";
import { createPostgresClient, resolvePostgresConfig } from "./storage/pg-client";
import {
  buildProductionRequest,
  submitProductionRequest,
  type ProductionRequestInput,
  type ProductionRequest,
} from "./production-request";

export interface ProductionCliRuntime {
  readonly read: (path: string) => Promise<string>;
  readonly write: (path: string, content: string) => Promise<void>;
  readonly exists: (path: string) => Promise<boolean>;
  readonly command: (args: readonly string[]) => Promise<string>;
  readonly loadRace: (keys: readonly string[]) => Promise<ProductionRequestInput>;
}
export interface ProductionCliInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly runtime: ProductionCliRuntime;
}
interface Operator {
  readonly directory: string;
  readonly runtime: ProductionCliRuntime;
  readonly env: Readonly<Record<string, string | undefined>>;
}
interface ApiRequest {
  readonly operator: Operator;
  readonly path: string;
  readonly body: unknown;
}
interface FileRequest {
  readonly directory: string;
  readonly input: ProductionRequestInput;
  readonly runtime: ProductionCliRuntime;
}
const INPUT_FILE: string = "production-input.json";
const PLAN_FILE: string = "production-plan.json";
const ACCEPTED_FILE: string = "production-accepted.json";
const STATUS_SQL: string =
  "select r.run_id,r.status,r.advance_cursor,t.table_name,t.catalog_status,t.neon_status,t.source_records,t.error_stage from sync_runs r join sync_run_tables t using(run_id) where r.run_id=? order by t.table_name";
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const isRow = (value: unknown): value is Record<string, string | null> =>
  isRecord(value) &&
  Object.values(value).every((cell) => cell === null || typeof cell === "string");
const isInput = (value: unknown): value is ProductionRequestInput =>
  isRecord(value) &&
  typeof value.runId === "string" &&
  typeof value.createdAt === "string" &&
  isRow(value.race) &&
  Array.isArray(value.runners) &&
  value.runners.every(isRow);
const digest = (text: string): string => createHash("sha256").update(text).digest("hex");
const required = (operator: Operator, key: string): string => {
  const value: string | undefined = operator.env[key];
  if (!value || !/^[a-zA-Z0-9_-]+$/.test(value))
    throw new Error(`Missing or invalid operator setting: ${key}`);
  return value;
};
const readInput = async (operator: Operator): Promise<ProductionRequestInput> => {
  const input: unknown = JSON.parse(
    await operator.runtime.read(join(operator.directory, INPUT_FILE)),
  );
  if (!isInput(input)) throw new Error("Invalid production input manifest");
  return input;
};
export const parseOperatorResponse = (text: string): unknown => {
  const envelope: unknown = JSON.parse(text);
  if (
    !isRecord(envelope) ||
    envelope.ok !== true ||
    !isRecord(envelope.data) ||
    envelope.data.isError === true ||
    !Array.isArray(envelope.data.content)
  )
    throw new Error("Managed API execution failed or requires approval");
  const entry: unknown = envelope.data.content.find(
    (item: unknown) => isRecord(item) && item.type === "text",
  );
  if (!isRecord(entry) || typeof entry.text !== "string")
    throw new Error("Managed API response is missing");
  const result: unknown = JSON.parse(entry.text);
  if (!isRecord(result) || result.success !== true)
    throw new Error("Production API operation failed");
  return result.result;
};
const api = async ({ operator, path, body }: ApiRequest): Promise<unknown> => {
  const profile: string = required(operator, "PC_KEIBA_CLOUDFLARE_PROFILE");
  const account: string = required(operator, "CLOUDFLARE_ACCOUNT_ID");
  const code: string = `async()=>{if(accountId!==${JSON.stringify(account)})throw Error("Cloudflare account mismatch");const r=await cloudflare.request({method:"POST",path:"/accounts/"+accountId+${JSON.stringify(path)},body:${JSON.stringify(body)}});if(!r.success||(Array.isArray(r.result)&&r.result.some(x=>x.success===false)))throw Error("Production API operation failed");return {success:true,result:r.result};}`;
  return parseOperatorResponse(
    await operator.runtime.command([
      "executor",
      "call",
      "cloudflare-api",
      "user",
      profile,
      "execute",
      JSON.stringify({ code }),
    ]),
  );
};
const savePrepared = async ({
  directory,
  input,
  runtime,
}: FileRequest): Promise<ProductionRequest> => {
  const plan: ProductionRequest = buildProductionRequest(input);
  await runtime.write(join(directory, INPUT_FILE), JSON.stringify(input, null, 2));
  await runtime.write(join(directory, PLAN_FILE), JSON.stringify(plan, null, 2));
  return plan;
};
const apply = async (operator: Operator): Promise<unknown> => {
  if (await operator.runtime.exists(join(operator.directory, ACCEPTED_FILE)))
    throw new Error("Request already accepted; use status");
  const input: ProductionRequestInput = await readInput(operator);
  const plan: ProductionRequest = buildProductionRequest(input);
  const frozen: unknown = JSON.parse(
    await operator.runtime.read(join(operator.directory, PLAN_FILE)),
  );
  if (JSON.stringify(plan) !== JSON.stringify(frozen)) throw new Error("Durable request changed");
  const bucket: string = required(operator, "PC_KEIBA_SOURCE_STAGING_BUCKET");
  const database: string = required(operator, "PC_KEIBA_SYNC_DATABASE_ID");
  const queue: string = required(operator, "PC_KEIBA_CATALOG_QUEUE_ID");
  await api({
    operator,
    path: `/d1/database/${database}/query`,
    body: { sql: STATUS_SQL, params: [plan.runId] },
  });
  await submitProductionRequest(input, {
    uploadAndVerify: async (stage) => {
      const file: string = join(operator.directory, `${stage.tableName}.stage.json`);
      const readback: string = join(operator.directory, `${stage.tableName}.readback.json`);
      await operator.runtime.write(file, stage.content);
      await operator.runtime.command([
        "bunx",
        "wrangler",
        "r2",
        "object",
        "put",
        `${bucket}/${stage.key}`,
        "--remote",
        "--file",
        file,
        "--content-type",
        "application/json",
      ]);
      await operator.runtime.command([
        "bunx",
        "wrangler",
        "r2",
        "object",
        "get",
        `${bucket}/${stage.key}`,
        "--remote",
        "--file",
        readback,
      ]);
      if (digest(await operator.runtime.read(readback)) !== stage.sha256)
        throw new Error("Remote staging digest mismatch");
    },
    register: async (statements) => {
      for (const body of statements)
        await api({ operator, path: `/d1/database/${database}/query`, body });
    },
    enqueue: async (jobs) => {
      await api({
        operator,
        path: `/queues/${queue}/messages/batch`,
        body: { messages: jobs.map((job) => ({ body: job, content_type: "json" })) },
      });
      await operator.runtime.write(
        join(operator.directory, ACCEPTED_FILE),
        JSON.stringify({ runId: plan.runId, accepted: true }),
      );
    },
  });
  return { accepted: true, runId: plan.runId, published: false };
};
export const runProductionCli = async ({
  argv,
  env,
  runtime,
}: ProductionCliInput): Promise<unknown> => {
  const [action, directory]: readonly (string | undefined)[] = argv;
  if (!directory)
    throw new Error(
      "Use prepare DIRECTORY YEAR MMDD VENUE RACE, apply DIRECTORY --confirm-production, or status DIRECTORY",
    );
  const operator: Operator = { directory, runtime, env };
  if (action === "prepare" && argv.length === 6) {
    if (await runtime.exists(join(directory, INPUT_FILE)))
      throw new Error("Durable request already exists");
    const input: ProductionRequestInput = await runtime.loadRace(argv.slice(2));
    const plan: ProductionRequest = await savePrepared({ directory, input, runtime });
    return { prepared: true, runId: plan.runId, productionWrites: 0 };
  }
  if (action === "apply" && argv.length === 3 && argv[2] === "--confirm-production")
    return apply(operator);
  if (action === "status" && argv.length === 2) {
    const plan: ProductionRequest = buildProductionRequest(await readInput(operator));
    return api({
      operator,
      path: `/d1/database/${required(operator, "PC_KEIBA_SYNC_DATABASE_ID")}/query`,
      body: { sql: STATUS_SQL, params: [plan.runId] },
    });
  }
  throw new Error("Invalid production command or missing --confirm-production");
};

const loadLocalRace = async (keys: readonly string[]): Promise<ProductionRequestInput> => {
  const client = createPostgresClient({ config: resolvePostgresConfig(process.env) });
  try {
    await client.connect();
    const predicate: string =
      "where kaisai_nen=$1 and kaisai_tsukihi=$2 and keibajo_code=$3 and race_bango=$4";
    const races = await client.execute({ text: `select * from jvd_ra ${predicate}`, values: keys });
    const runners = await client.execute({
      text: `select * from jvd_se ${predicate} order by umaban`,
      values: keys,
    });
    const race = races.rows[0];
    if (!race || races.rows.length !== 1) throw new Error("Expected exactly one local race");
    return {
      runId: randomUUID(),
      createdAt: new Date().toISOString(),
      race,
      runners: runners.rows,
    };
  } finally {
    await client.end();
  }
};
const command = async (args: readonly string[]): Promise<string> => {
  const process = Bun.spawn([...args], { stdout: "pipe", stderr: "pipe" });
  const [stdout, , code]: readonly [string, string, number] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited,
  ]);
  if (code !== 0)
    throw new Error("Operator command failed; inspect tool authorization and configuration");
  return stdout;
};
export const createProductionCliRuntime = (): ProductionCliRuntime => ({
  read: async (path) => readFile(path, "utf8"),
  write: async (path, content) => {
    await mkdir(join(path, ".."), { recursive: true, mode: 0o700 });
    await writeFile(path, content, { mode: 0o600 });
  },
  exists: async (path) =>
    access(path).then(
      () => true,
      () => false,
    ),
  command,
  loadRace: loadLocalRace,
});
