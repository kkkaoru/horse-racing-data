// Runs with bun; plan/verify before and after DDL. Arming and writer fencing are separate steps.
import { D1_CAPTURE_TABLE, buildD1CapturePlan, type D1CapturePlan } from "./d1-change-capture";
import { discoverD1CaptureSchema, type D1CaptureMetadataQuery } from "./d1-capture-schema";

export interface D1CaptureInstallInput {
  table: string;
  captureId: string;
  expectedDefinitionHash: string;
  expectedPlanHash: string;
}
export interface D1CaptureInstallation {
  definitionHash: string;
  plan: D1CapturePlan;
  statements: readonly string[];
  // Means only that all exact DDL objects were observed, not that writers are fenced.
  ddlVerified: boolean;
}
export interface D1CaptureInstallIntent {
  table: string;
  captureId: string;
  definitionHash: string;
  planHash: string;
  statements: readonly string[];
}
export interface D1CaptureInstallDependencies {
  query: D1CaptureMetadataQuery;
  persistIntent: (intent: D1CaptureInstallIntent) => Promise<void>;
  executeBatch: (statements: readonly string[]) => Promise<void>;
}
interface ExpectedObject {
  type: "table" | "trigger";
  table: string;
  sql: string;
}

/**
 * Regenerate DDL from fresh metadata, never execute SQL from an old artifact. Unknown
 * triggers, schema drift and changed generator bytes fail closed. Existing exact objects
 * are retained, so an uncertain/partial acknowledgement can be inspected without DROP or
 * IF NOT EXISTS. Call again after execution before recording any capture watermark.
 */
export const prepareD1CaptureInstall = async (
  input: D1CaptureInstallInput,
  query: D1CaptureMetadataQuery,
): Promise<D1CaptureInstallation> => {
  if (
    !/^[a-f0-9]{64}$/u.test(input.expectedDefinitionHash) ||
    !/^[a-f0-9]{64}$/u.test(input.expectedPlanHash)
  )
    throw new Error("Invalid capture installation fingerprints");
  const schema = await discoverD1CaptureSchema(input.table, query);
  if (schema.definitionHash !== input.expectedDefinitionHash)
    throw new Error("Capture source definition changed");
  const plan = buildD1CapturePlan({ ...schema, captureId: input.captureId });
  if (plan.planHash !== input.expectedPlanHash)
    throw new Error("Capture installation plan changed");
  const expected = new Map<string, ExpectedObject>();
  expected.set(D1_CAPTURE_TABLE, {
    type: "table",
    table: D1_CAPTURE_TABLE,
    sql: plan.createOutbox,
  });
  plan.triggers.forEach(({ name, sql }) =>
    expected.set(name, { type: "trigger", table: input.table, sql }),
  );
  const rows = await query({
    sql: "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name = ? OR (type = 'trigger' AND (tbl_name = ? OR tbl_name = ?)) LIMIT 7",
    params: [D1_CAPTURE_TABLE, input.table, D1_CAPTURE_TABLE],
  });
  const seen = new Set<string>();
  for (const row of rows) {
    if (typeof row.name !== "string") throw new Error("Invalid capture schema object");
    const object = expected.get(row.name);
    if (
      object === undefined ||
      seen.has(row.name) ||
      row.type !== object.type ||
      row.tbl_name !== object.table ||
      row.sql !== object.sql
    )
      throw new Error("Unexpected or changed capture schema object");
    seen.add(row.name);
  }
  if (seen.has(D1_CAPTURE_TABLE)) {
    const indexes = await query({ sql: `PRAGMA index_list('${D1_CAPTURE_TABLE}')`, params: [] });
    if (indexes.length !== 0) throw new Error("Unexpected capture journal index");
  }
  if (seen.size > 0 && !seen.has(D1_CAPTURE_TABLE))
    throw new Error("Capture journal missing for existing triggers");
  const statements = [...expected]
    .filter(([name]) => !seen.has(name))
    .map(([, object]) => object.sql);
  return {
    definitionHash: schema.definitionHash,
    plan,
    statements,
    ddlVerified: statements.length === 0,
  };
};

/**
 * Only call after operational writer/capacity checks. Persist immutable, target-scoped
 * intent before I/O; unknown acknowledgements are never retried blindly. Re-discover and
 * verify every object after execution. This intentionally does not create/advance a
 * watermark: a generation must preserve its first start point across installation retries.
 */
export const applyD1CaptureInstall = async (
  input: D1CaptureInstallInput,
  dependencies: D1CaptureInstallDependencies,
): Promise<D1CaptureInstallation> => {
  const prepared = await prepareD1CaptureInstall(input, dependencies.query);
  await dependencies.persistIntent({
    table: input.table,
    captureId: input.captureId,
    definitionHash: prepared.definitionHash,
    planHash: prepared.plan.planHash,
    statements: prepared.statements,
  });
  if (prepared.statements.length > 0) await dependencies.executeBatch(prepared.statements);
  const verified = await prepareD1CaptureInstall(input, dependencies.query);
  if (!verified.ddlVerified) throw new Error("Capture DDL is incomplete after execution");
  return verified;
};
