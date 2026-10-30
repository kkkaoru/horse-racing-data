// This file runs with Bun. Lists every horse, jockey, trainer and owner of an overseas event and its past
// editions, and writes private history plans for them, so one meeting key drives the whole collection.
import assert from "node:assert/strict";
import { open, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { localHistoryEnvironment } from "./history-local-mirror";
import { createTargetPool, type TargetPool } from "./storage/history-connection";
import type { PostgresQueryResult } from "./storage/pg-client";

export interface EventEntitiesInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export type EntityKind = "horse" | "jockey" | "trainer" | "owner";
export interface EventEntity {
  readonly kind: EntityKind;
  readonly sourceId: string;
}
export interface EventEntitiesReport {
  readonly status: "complete";
  readonly operation: "entities" | "plans";
  readonly races: number;
  readonly entities: number;
  readonly written: number;
}
interface PlanTemplate {
  readonly urlTemplate: string;
  readonly plan: Readonly<Record<string, unknown>>;
}

const KINDS: readonly EntityKind[] = ["horse", "jockey", "trainer", "owner"];
const ID_COLUMNS: Readonly<Record<EntityKind, string>> = {
  horse: "source_horse_id",
  jockey: "source_jockey_id",
  trainer: "source_trainer_id",
  owner: "source_owner_id",
};
const MEETING_PATTERN: RegExp = /^(\d{4}) (\d{4}) ([A-Z][0-9A-Z])$/u;
const ENTITY_LINE_PATTERN: RegExp = /^(horse|jockey|trainer|owner) ([0-9A-Za-z]+)$/u;
const ID_PLACEHOLDER: string = "{ID}";
const FILE_MODE: number = 0o600;
const TEMPLATE_ENV: string = "OVERSEA_HISTORY_PLAN_TEMPLATES_PATH";
const USAGE: string =
  "Use event-entities YEAR MMDD VENUE OUTPUT_FILE or event-plans ENTITIES_FILE PLANS_DIRECTORY.";
// Past editions are earlier JRA overseas races (venue codes start with a letter) whose title, without
// full-width spaces and a grade suffix, equals a title of the target meeting.
const NORMALIZED_TITLE: string =
  "regexp_replace(btrim(replace(ra.kyosomei_hondai, chr(12288), '')), '\\((GI{1,3}|G[123])\\)$', '')";
const EVENT_RUNNERS_SQL: string = `WITH target AS (
  SELECT DISTINCT ${NORMALIZED_TITLE} AS title FROM jvd_ra ra
  WHERE ra.kaisai_nen = $1 AND ra.kaisai_tsukihi = $2 AND ra.keibajo_code = $3
), races AS (
  SELECT ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango FROM jvd_ra ra
  WHERE ra.keibajo_code ~ '^[A-Z]' AND ${NORMALIZED_TITLE} IN (SELECT title FROM target WHERE title <> '')
    AND ra.kaisai_nen || ra.kaisai_tsukihi <= $1 || $2
)
SELECT DISTINCT m.kaisai_nen || m.kaisai_tsukihi || m.keibajo_code || m.race_bango AS race,
  m.source_horse_id::text, m.source_jockey_id::text, m.source_trainer_id::text, m.source_owner_id::text
FROM oversea_runner_source_id m JOIN races r USING (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
WHERE m.source = 'netkeiba'`;

const isEntityKind = (value: string): value is EntityKind => KINDS.some((kind) => kind === value);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isPlanTemplate = (value: unknown): value is PlanTemplate =>
  isRecord(value) &&
  typeof value.urlTemplate === "string" &&
  value.urlTemplate.includes(ID_PLACEHOLDER) &&
  isRecord(value.plan);

const compareEntities = (left: EventEntity, right: EventEntity): number =>
  KINDS.indexOf(left.kind) - KINDS.indexOf(right.kind) ||
  left.sourceId.localeCompare(right.sourceId);

const textValue = (value: unknown): string => (typeof value === "string" ? value.trim() : "");

// Horses first, then people; each id once. Missing ids (partial source rows) are skipped, never invented.
export const eventEntities = (rows: PostgresQueryResult["rows"]): EventEntity[] =>
  [
    ...new Map(
      rows.flatMap((row) =>
        KINDS.flatMap((kind): [string, EventEntity][] => {
          const sourceId: string = textValue(row[ID_COLUMNS[kind]]);
          return sourceId === "" ? [] : [[`${kind} ${sourceId}`, { kind, sourceId }]];
        }),
      ),
    ).values(),
  ].toSorted(compareEntities);

export const parseEntityLines = (text: string): EventEntity[] =>
  text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "")
    .map((line) => {
      const match: RegExpExecArray | null = ENTITY_LINE_PATTERN.exec(line);
      const kind: string = String(match?.[1]);
      assert(match !== null && isEntityKind(kind), `Invalid entity line: ${line}`);
      return { kind, sourceId: String(match[2]) };
    });

const templateFor = (document: Record<string, unknown>, kind: EntityKind): PlanTemplate => {
  const entry: unknown = document[kind];
  assert(
    isPlanTemplate(entry),
    `Plan template for ${kind} needs urlTemplate with ${ID_PLACEHOLDER} and a plan object.`,
  );
  return entry;
};

const parseTemplates = (text: string): Readonly<Record<EntityKind, PlanTemplate>> => {
  const document: unknown = JSON.parse(text);
  assert(isRecord(document), "Plan templates must be an object.");
  return {
    horse: templateFor(document, "horse"),
    jockey: templateFor(document, "jockey"),
    trainer: templateFor(document, "trainer"),
    owner: templateFor(document, "owner"),
  };
};

export const buildEntityPlan = (
  template: PlanTemplate,
  entity: EventEntity,
): Readonly<Record<string, unknown>> => ({
  ...template.plan,
  kind: entity.kind,
  sourceId: entity.sourceId,
  initialUrl: template.urlTemplate.replaceAll(ID_PLACEHOLDER, entity.sourceId),
});

// Existing plans are kept as-is so a resumed collection keeps its source scope.
const writeNewPlan = async (
  path: string,
  plan: Readonly<Record<string, unknown>>,
): Promise<boolean> => {
  try {
    const handle = await open(path, "wx", FILE_MODE);
    try {
      await handle.writeFile(JSON.stringify(plan));
    } finally {
      await handle.close();
    }
    return true;
  } catch (error: unknown) {
    if (error instanceof Error && "code" in error && error.code === "EEXIST") return false;
    throw error;
  }
};

const runEntities = async (
  args: readonly string[],
  env: EventEntitiesInput["env"],
): Promise<EventEntitiesReport> => {
  const match: RegExpExecArray | null = MEETING_PATTERN.exec(args.slice(0, 3).join(" "));
  assert(match !== null && args.length === 4, USAGE);
  const local: TargetPool = createTargetPool(localHistoryEnvironment(env), "local");
  try {
    const result: PostgresQueryResult = await local.pool.query(EVENT_RUNNERS_SQL, [
      String(match[1]),
      String(match[2]),
      String(match[3]),
    ]);
    const entities: EventEntity[] = eventEntities(result.rows);
    assert(entities.length > 0, "No source-id rows exist for this event.");
    await writeFile(
      String(args[3]),
      `${entities.map((entity) => `${entity.kind} ${entity.sourceId}`).join("\n")}\n`,
      { mode: FILE_MODE },
    );
    return {
      status: "complete",
      operation: "entities",
      races: new Set(result.rows.map((row) => textValue(row.race))).size,
      entities: entities.length,
      written: entities.length,
    };
  } finally {
    await local.pool.end();
  }
};

const runPlans = async (
  args: readonly string[],
  env: EventEntitiesInput["env"],
): Promise<EventEntitiesReport> => {
  assert(args.length === 2, USAGE);
  const templatePath: string | undefined = env[TEMPLATE_ENV];
  assert(templatePath !== undefined && templatePath !== "", `${TEMPLATE_ENV} is required.`);
  const templates = parseTemplates(await readFile(templatePath, "utf8"));
  const entities: EventEntity[] = parseEntityLines(await readFile(String(args[0]), "utf8"));
  const written: boolean[] = await Promise.all(
    entities.map((entity) =>
      writeNewPlan(
        join(String(args[1]), `${entity.kind}-${entity.sourceId}.json`),
        buildEntityPlan(templates[entity.kind], entity),
      ),
    ),
  );
  return {
    status: "complete",
    operation: "plans",
    races: 0,
    entities: entities.length,
    written: written.filter(Boolean).length,
  };
};

export const runEventEntities = async ({
  argv,
  env,
}: EventEntitiesInput): Promise<EventEntitiesReport> => {
  const [action, ...rest]: readonly string[] = argv;
  if (action === "event-entities") return runEntities(rest, env);
  assert(action === "event-plans", USAGE);
  return runPlans(rest, env);
};
