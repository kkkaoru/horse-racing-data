// Run with bun. Non-mutating Neon write-pool probe for the running-style and
// finish-position prediction tables. Uses getFinishPositionWritePool(env) and
// only SELECT diagnostics. Never logs or returns DSNs or raw driver errors.

import { getFinishPositionWritePool } from "./finish-position-lite-pool";
import type { Env } from "./types";

export type NeonWritePoolSource = "DATABASE_URL_NEON" | "NEON_DATABASE_URL";

export type NeonWritePoolQueryErrorClass = "auth" | "network" | "read_only" | "unknown";

export type NeonWritePoolProbeErrorClass = "unconfigured" | NeonWritePoolQueryErrorClass;

export interface NeonWritePoolProbeSuccess {
  canInsertFinishPosition: boolean;
  canInsertRunningStyle: boolean;
  canSelectFinishPosition: boolean;
  canSelectRunningStyle: boolean;
  canUpdateFinishPosition: boolean;
  canUpdateRunningStyle: boolean;
  canUpsertFinishPosition: boolean;
  canUpsertRunningStyle: boolean;
  defaultTransactionReadOnly: boolean;
  fpTablePresent: boolean;
  inRecovery: boolean;
  ok: true;
  rsTablePresent: boolean;
  source: NeonWritePoolSource;
  transactionReadOnly: boolean;
  writablePrimary: boolean;
}

export interface NeonWritePoolUnconfiguredResult {
  errorClass: "unconfigured";
  ok: false;
}

export interface NeonWritePoolQueryFailure {
  errorClass: NeonWritePoolQueryErrorClass;
  ok: false;
  source: NeonWritePoolSource;
}

export type NeonWritePoolProbeResult =
  | NeonWritePoolProbeSuccess
  | NeonWritePoolUnconfiguredResult
  | NeonWritePoolQueryFailure;

interface NeonWritePoolProbeRow {
  can_insert_finish_position: boolean;
  can_insert_running_style: boolean;
  can_select_finish_position: boolean;
  can_select_running_style: boolean;
  can_update_finish_position: boolean;
  can_update_running_style: boolean;
  default_transaction_read_only: boolean;
  fp_table_present: boolean;
  in_recovery: boolean;
  rs_table_present: boolean;
  transaction_read_only: boolean;
}

interface MessageErrorClassPattern {
  errorClass: NeonWritePoolQueryErrorClass;
  pattern: RegExp;
}

const NEON_WRITE_POOL_PROBE_SQL =
  "select pg_is_in_recovery() as in_recovery, current_setting('transaction_read_only') = 'on' as transaction_read_only, current_setting('default_transaction_read_only') = 'on' as default_transaction_read_only, to_regclass('public.race_running_style_model_predictions') is not null as rs_table_present, to_regclass('public.race_finish_position_model_predictions') is not null as fp_table_present, (to_regclass('public.race_running_style_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_running_style_model_predictions'), 'INSERT'), false)) as can_insert_running_style, (to_regclass('public.race_finish_position_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_finish_position_model_predictions'), 'INSERT'), false)) as can_insert_finish_position, (to_regclass('public.race_running_style_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_running_style_model_predictions'), 'SELECT'), false)) as can_select_running_style, (to_regclass('public.race_finish_position_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_finish_position_model_predictions'), 'SELECT'), false)) as can_select_finish_position, (to_regclass('public.race_running_style_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_running_style_model_predictions'), 'UPDATE'), false)) as can_update_running_style, (to_regclass('public.race_finish_position_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_finish_position_model_predictions'), 'UPDATE'), false)) as can_update_finish_position";

const QUERY_ERROR_CLASS_BY_CODE: ReadonlyMap<string, NeonWritePoolQueryErrorClass> = new Map([
  ["08001", "network"],
  ["08004", "network"],
  ["08006", "network"],
  ["08007", "network"],
  ["25006", "read_only"],
  ["28000", "auth"],
  ["28P01", "auth"],
  ["57P03", "network"],
  ["EAI_AGAIN", "network"],
  ["ECONNREFUSED", "network"],
  ["ECONNRESET", "network"],
  ["EHOSTUNREACH", "network"],
  ["ENETUNREACH", "network"],
  ["ENOTFOUND", "network"],
  ["EPIPE", "network"],
  ["ETIMEDOUT", "network"],
]);

const QUERY_ERROR_CLASS_BY_MESSAGE: ReadonlyArray<MessageErrorClassPattern> = [
  { errorClass: "read_only", pattern: /read-only transaction/i },
  { errorClass: "auth", pattern: /password authentication failed/i },
  { errorClass: "auth", pattern: /no pg_hba\.conf/i },
  { errorClass: "auth", pattern: /SASL/i },
  { errorClass: "auth", pattern: /SCRAM/i },
  { errorClass: "auth", pattern: /invalid authorization/i },
  { errorClass: "network", pattern: /econnrefused/i },
  { errorClass: "network", pattern: /etimedout/i },
  { errorClass: "network", pattern: /enotfound/i },
  { errorClass: "network", pattern: /eai_again/i },
  { errorClass: "network", pattern: /econnreset/i },
  { errorClass: "network", pattern: /enetunreach/i },
  { errorClass: "network", pattern: /ehostunreach/i },
];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const readBooleanField = (
  row: Record<string, unknown>,
  field: keyof NeonWritePoolProbeRow,
): boolean | null => {
  const value: unknown = row[field];
  return typeof value === "boolean" ? value : null;
};

const parseProbeRow = (value: unknown): NeonWritePoolProbeRow | null => {
  if (!isRecord(value)) return null;
  const canInsertFinishPosition: boolean | null = readBooleanField(
    value,
    "can_insert_finish_position",
  );
  const canInsertRunningStyle: boolean | null = readBooleanField(value, "can_insert_running_style");
  const canSelectFinishPosition: boolean | null = readBooleanField(
    value,
    "can_select_finish_position",
  );
  const canSelectRunningStyle: boolean | null = readBooleanField(value, "can_select_running_style");
  const canUpdateFinishPosition: boolean | null = readBooleanField(
    value,
    "can_update_finish_position",
  );
  const canUpdateRunningStyle: boolean | null = readBooleanField(value, "can_update_running_style");
  const defaultTransactionReadOnly: boolean | null = readBooleanField(
    value,
    "default_transaction_read_only",
  );
  const fpTablePresent: boolean | null = readBooleanField(value, "fp_table_present");
  const inRecovery: boolean | null = readBooleanField(value, "in_recovery");
  const rsTablePresent: boolean | null = readBooleanField(value, "rs_table_present");
  const transactionReadOnly: boolean | null = readBooleanField(value, "transaction_read_only");
  if (
    canInsertFinishPosition === null ||
    canInsertRunningStyle === null ||
    canSelectFinishPosition === null ||
    canSelectRunningStyle === null ||
    canUpdateFinishPosition === null ||
    canUpdateRunningStyle === null ||
    defaultTransactionReadOnly === null ||
    fpTablePresent === null ||
    inRecovery === null ||
    rsTablePresent === null ||
    transactionReadOnly === null
  ) {
    return null;
  }
  return {
    can_insert_finish_position: canInsertFinishPosition,
    can_insert_running_style: canInsertRunningStyle,
    can_select_finish_position: canSelectFinishPosition,
    can_select_running_style: canSelectRunningStyle,
    can_update_finish_position: canUpdateFinishPosition,
    can_update_running_style: canUpdateRunningStyle,
    default_transaction_read_only: defaultTransactionReadOnly,
    fp_table_present: fpTablePresent,
    in_recovery: inRecovery,
    rs_table_present: rsTablePresent,
    transaction_read_only: transactionReadOnly,
  };
};

const readQueryRows = (result: unknown): unknown[] | null => {
  if (!isRecord(result)) return null;
  const rows: unknown = result.rows;
  return Array.isArray(rows) ? rows : null;
};

const readErrorCode = (error: unknown): string | null => {
  if (!isRecord(error)) return null;
  const code: unknown = error.code;
  if (typeof code === "string" && code.length > 0) return code.toUpperCase();
  const errno: unknown = error.errno;
  return typeof errno === "string" && errno.length > 0 ? errno.toUpperCase() : null;
};

const readErrorMessage = (error: unknown): string | null => {
  if (!isRecord(error)) return null;
  const message: unknown = error.message;
  return typeof message === "string" ? message : null;
};

const classifyMessageError = (message: string): NeonWritePoolQueryErrorClass => {
  const matched: MessageErrorClassPattern | undefined = QUERY_ERROR_CLASS_BY_MESSAGE.find((entry) =>
    entry.pattern.test(message),
  );
  return matched === undefined ? "unknown" : matched.errorClass;
};

const isWritablePrimary = (row: NeonWritePoolProbeRow): boolean =>
  !row.in_recovery && !row.transaction_read_only;

const canUpsertPredictionTable = (
  tablePresent: boolean,
  canSelect: boolean,
  canInsert: boolean,
  canUpdate: boolean,
): boolean => tablePresent && canSelect && canInsert && canUpdate;

const resolveWritePoolSource = (env: Env): NeonWritePoolSource | null => {
  if (env.DATABASE_URL_NEON) return "DATABASE_URL_NEON";
  if (env.NEON_DATABASE_URL) return "NEON_DATABASE_URL";
  return null;
};

export const classifyNeonWritePoolQueryError = (error: unknown): NeonWritePoolQueryErrorClass => {
  const code: string | null = readErrorCode(error);
  if (code !== null) {
    const classified: NeonWritePoolQueryErrorClass | undefined =
      QUERY_ERROR_CLASS_BY_CODE.get(code);
    return classified === undefined ? "unknown" : classified;
  }
  const message: string | null = readErrorMessage(error);
  return message === null ? "unknown" : classifyMessageError(message);
};

export const probeNeonWritePool = async (env: Env): Promise<NeonWritePoolProbeResult> => {
  const source: NeonWritePoolSource | null = resolveWritePoolSource(env);
  if (source === null) {
    return { errorClass: "unconfigured", ok: false };
  }
  try {
    const result: unknown = await getFinishPositionWritePool(env).query(NEON_WRITE_POOL_PROBE_SQL);
    const rows: unknown[] | null = readQueryRows(result);
    if (rows === null || rows.length === 0) {
      return { errorClass: "unknown", ok: false, source };
    }
    const row: NeonWritePoolProbeRow | null = parseProbeRow(rows[0]);
    if (row === null) {
      return { errorClass: "unknown", ok: false, source };
    }
    return {
      canInsertFinishPosition: row.can_insert_finish_position,
      canInsertRunningStyle: row.can_insert_running_style,
      canSelectFinishPosition: row.can_select_finish_position,
      canSelectRunningStyle: row.can_select_running_style,
      canUpdateFinishPosition: row.can_update_finish_position,
      canUpdateRunningStyle: row.can_update_running_style,
      canUpsertFinishPosition: canUpsertPredictionTable(
        row.fp_table_present,
        row.can_select_finish_position,
        row.can_insert_finish_position,
        row.can_update_finish_position,
      ),
      canUpsertRunningStyle: canUpsertPredictionTable(
        row.rs_table_present,
        row.can_select_running_style,
        row.can_insert_running_style,
        row.can_update_running_style,
      ),
      defaultTransactionReadOnly: row.default_transaction_read_only,
      fpTablePresent: row.fp_table_present,
      inRecovery: row.in_recovery,
      ok: true,
      rsTablePresent: row.rs_table_present,
      source,
      transactionReadOnly: row.transaction_read_only,
      writablePrimary: isWritablePrimary(row),
    };
  } catch (error: unknown) {
    return {
      errorClass: classifyNeonWritePoolQueryError(error),
      ok: false,
      source,
    };
  }
};
