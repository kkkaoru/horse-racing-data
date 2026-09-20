// This file runs with Bun. Production writes remain owned by daily-keiba-sync.
import { createHash } from "node:crypto";
import layouts from "../../daily-keiba-sync/src/generated/record-layouts.json";

export interface ProductionRequestInput {
  readonly runId: string;
  readonly createdAt: string;
  readonly race: Readonly<Record<string, string | null>>;
  readonly runners: readonly Readonly<Record<string, string | null>>[];
}

export interface ProductionStatement {
  readonly sql: string;
  readonly params: readonly (string | number)[];
}

export interface ProductionStage {
  readonly tableName: "jvd_ra" | "jvd_se";
  readonly key: string;
  readonly content: string;
  readonly sha256: string;
}

export interface ProductionJob {
  readonly type: "catalog-table";
  readonly provider: "jv";
  readonly runDate: string;
  readonly runId: string;
  readonly tableName: string;
  readonly tableStagingKey: string;
}

export interface ProductionRequest {
  readonly runId: string;
  readonly runDate: string;
  readonly stages: readonly ProductionStage[];
  readonly statements: readonly ProductionStatement[];
  readonly jobs: readonly ProductionJob[];
}

export interface ProductionRequestPorts {
  readonly uploadAndVerify: (stage: ProductionStage) => Promise<void>;
  readonly register: (statements: readonly ProductionStatement[]) => Promise<void>;
  readonly enqueue: (jobs: readonly ProductionJob[]) => Promise<void>;
}

interface StageInput {
  readonly tableName: "jvd_ra" | "jvd_se";
  readonly records: readonly Readonly<Record<string, string | null>>[];
  readonly request: ProductionRequestInput;
  readonly runDate: string;
}

const RACE_KEY_FIELDS: readonly string[] = [
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
];
const RUN_ID_PATTERN: RegExp = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const MAX_RUNNERS: number = 40;

const validateRequest = (input: ProductionRequestInput): string => {
  const date: string = `${input.race.kaisai_nen}${input.race.kaisai_tsukihi}`;
  const isoDate: string = `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}T00:00:00.000Z`;
  if (
    !/^20\d{6}$/.test(date) ||
    Number.isNaN(Date.parse(isoDate)) ||
    new Date(isoDate).toISOString() !== isoDate
  )
    throw new Error("Invalid overseas race date");
  if (!RUN_ID_PATTERN.test(input.runId) || Number.isNaN(Date.parse(input.createdAt)))
    throw new Error("Invalid production request identity");
  if (
    typeof input.race.keibajo_code !== "string" ||
    !/^[A-Z][0-9A-Z]$/.test(input.race.keibajo_code)
  )
    throw new Error("Production request requires an overseas venue");
  if (
    typeof input.race.race_bango !== "string" ||
    !/^(0[1-9]|[1-9][0-9])$/.test(input.race.race_bango)
  )
    throw new Error("Invalid overseas race number");
  if (
    input.runners.length < 1 ||
    input.runners.length > MAX_RUNNERS ||
    Number(input.race.shusso_tosu) !== input.runners.length
  )
    throw new Error("Active runner count does not match the race");
  if (
    input.runners.some((row) => RACE_KEY_FIELDS.some((field) => row[field] !== input.race[field]))
  )
    throw new Error("Production request contains a different race");
  if (
    input.runners.some(
      (row) => typeof row.umaban !== "string" || !/^(0[1-9]|[1-9][0-9])$/.test(row.umaban),
    ) ||
    new Set(input.runners.map((row) => row.umaban)).size !== input.runners.length
  )
    throw new Error("Invalid or duplicate runner number");
  return date;
};

const buildStage = ({ tableName, records, request, runDate }: StageInput): ProductionStage => {
  const columns: readonly string[] = layouts.tables[tableName].columns.map((column) => column.name);
  if (
    records.some(
      (row) =>
        Object.keys(row).length !== columns.length || columns.some((column) => !(column in row)),
    )
  )
    throw new Error("Source columns do not match the production layout");
  const content: string = JSON.stringify({
    formatVersion: 1,
    provider: "jv",
    records,
    runId: request.runId,
    tableName,
  });
  return {
    tableName,
    key: `source-staging/v1/jv/${runDate}/${request.runId}/tables/${tableName}.json`,
    content,
    sha256: createHash("sha256").update(content).digest("hex"),
  };
};

export const buildProductionRequest = (input: ProductionRequestInput): ProductionRequest => {
  const runDate: string = validateRequest(input);
  const stages: readonly ProductionStage[] = [
    buildStage({ tableName: "jvd_ra", records: [input.race], request: input, runDate }),
    buildStage({ tableName: "jvd_se", records: input.runners, request: input, runDate }),
  ];
  const statements: readonly ProductionStatement[] = [
    {
      sql: "insert into sync_runs (run_id,dedupe_key,provider,run_date,trigger_kind,lookback_days,advance_cursor,status,files,records,catalog_tables,created_at,updated_at) values (?,?,'jv',?,'manual',1,0,'catalog_pending',2,?,2,?,?) on conflict(run_id) do nothing",
      params: [
        input.runId,
        `jv:${runDate}:manual:${input.runId}`,
        runDate,
        input.runners.length + 1,
        input.createdAt,
        input.createdAt,
      ],
    },
    ...stages.flatMap((stage): readonly ProductionStatement[] => [
      {
        sql: "insert into sync_run_tables (run_id,table_name,staging_key,source_records,catalog_status,neon_status,updated_at) values (?,?,?,?,'pending','pending',?) on conflict(run_id,table_name) do nothing",
        params: [
          input.runId,
          stage.tableName,
          stage.key,
          stage.tableName === "jvd_ra" ? 1 : input.runners.length,
          input.createdAt,
        ],
      },
      {
        sql: "insert or ignore into sync_run_table_partitions (run_id,table_name,partition_value) values (?,?,?)",
        params: [input.runId, stage.tableName, runDate.slice(0, 4)],
      },
    ]),
  ];
  return {
    runId: input.runId,
    runDate,
    stages,
    statements,
    jobs: stages.map(
      (stage): ProductionJob => ({
        type: "catalog-table",
        provider: "jv",
        runDate,
        runId: input.runId,
        tableName: stage.tableName,
        tableStagingKey: stage.key,
      }),
    ),
  };
};

export const submitProductionRequest = async (
  input: ProductionRequestInput,
  ports: ProductionRequestPorts,
): Promise<ProductionRequest> => {
  const plan: ProductionRequest = buildProductionRequest(input);
  // No control-plane registration or jobs until every immutable object is verified.
  await Promise.all(plan.stages.map((stage) => ports.uploadAndVerify(stage)));
  await ports.register(plan.statements);
  await ports.enqueue(plan.jobs);
  return plan;
};
