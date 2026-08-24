// Run with bun. Durable focused-full Container completion watch helpers.

import type {
  Env,
  FocusedFullCompletionMessage,
  FocusedFullWatchTickMessage,
  FocusedFullWatchPayload,
  PredictQueueMessage,
} from "./types";
import { clearFocusedFullWatchOutbox, registerFocusedFullWatchOutbox } from "./do-state";

export interface FocusedFullWatchBody extends PredictQueueMessage {
  keibajoCode: string;
  raceBango: string;
}

export interface ValidatedFocusedFullWatchPayload extends FocusedFullWatchPayload {
  body: FocusedFullWatchBody;
}

export interface ValidatedFocusedFullWatchTickMessage extends FocusedFullWatchTickMessage {
  body: FocusedFullWatchBody;
}

export interface FocusedFullWatchTickDependencies {
  now: () => number;
  pollStatus: (body: FocusedFullWatchBody) => Promise<Response>;
}

interface FocusedFullStatusPayload {
  error: string | null;
  raceKey: string;
  status: "error" | "missing" | "running" | "success";
}

interface TerminalWatchResult {
  error?: string;
  outcome: "error" | "missing" | "success" | "timeout";
}

export const WATCH_REQUEST_HEADER: string = "x-focused-full-watch-payload";
export const WATCH_RESPONSE_HEADER: string = "x-focused-full-watch-id";
export const FOCUSED_FULL_WATCH_POLL_SECONDS: number = 30;
export const FOCUSED_FULL_WATCH_BACKUP_SECONDS: number = 150;
export const FOCUSED_FULL_WATCH_TIMEOUT_MS: number = 31 * 60 * 1000;

const PREDICT_PATH: string = "/predict";
const STATUS_PATH: string = "/focused-full-status";
const RESULT_TYPE: string = "result";
const ACCEPTED_STATUS: string = "accepted";
const FULL_MODE: string = "full";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0;

const isPredictCategory = (value: unknown): boolean =>
  value === "jra" || value === "nar" || value === "ban-ei";

const isFocusedFullBody = (value: unknown): value is FocusedFullWatchBody => {
  if (!isRecord(value)) return false;
  return (
    isNonEmptyString(value.runDate) &&
    isNonEmptyString(value.runDateIso) &&
    isNonEmptyString(value.runYmd) &&
    isPredictCategory(value.category) &&
    typeof value.daysAhead === "number" &&
    Number.isFinite(value.daysAhead) &&
    value.mode === FULL_MODE &&
    value.skipDedup === true &&
    isNonEmptyString(value.keibajoCode) &&
    isNonEmptyString(value.raceBango)
  );
};

const isFocusedFullWatchPayload = (value: unknown): value is ValidatedFocusedFullWatchPayload => {
  if (!isRecord(value)) return false;
  return (
    isFocusedFullBody(value.body) &&
    isNonEmptyString(value.doName) &&
    (value.role === "legacy" || value.role === "race-chain") &&
    isNonEmptyString(value.watchId) &&
    isNonEmptyString(value.workKey)
  );
};

const expectedRaceKey = (body: PredictQueueMessage): string =>
  `${body.category}:${body.runYmd}:${body.keibajoCode}:${body.raceBango}`;

const parseStatusPayload = (
  value: unknown,
  body: PredictQueueMessage,
): FocusedFullStatusPayload => {
  if (!isRecord(value)) throw new Error("Focused-full status response is not an object");
  if (
    value.status !== "error" &&
    value.status !== "missing" &&
    value.status !== "running" &&
    value.status !== "success"
  ) {
    throw new Error("Focused-full status response has an invalid status");
  }
  if (value.raceKey !== expectedRaceKey(body)) {
    throw new Error(`Focused-full status race key mismatch expected=${expectedRaceKey(body)}`);
  }
  if (value.error !== null && typeof value.error !== "string") {
    throw new Error("Focused-full status response has an invalid error");
  }
  return { error: value.error, raceKey: value.raceKey, status: value.status };
};

const readStatus = async (
  response: Response,
  body: PredictQueueMessage,
): Promise<FocusedFullStatusPayload> => {
  if (!response.ok) {
    throw new Error(
      `Focused-full status request returned ${response.status}: ${await response.text()}`,
    );
  }
  const value: unknown = await response.json();
  return parseStatusPayload(value, body);
};

const terminalResult = async (
  payload: ValidatedFocusedFullWatchTickMessage,
  dependencies: FocusedFullWatchTickDependencies,
): Promise<TerminalWatchResult | undefined> => {
  if (dependencies.now() >= payload.deadlineAtMs) {
    return { error: "Focused-full completion watch timed out", outcome: "timeout" };
  }
  const status = await readStatus(await dependencies.pollStatus(payload.body), payload.body);
  if (status.status === "running") return undefined;
  if (status.status === "error") {
    return {
      error: status.error ?? `Focused-full detached pipeline failed: ${status.raceKey}`,
      outcome: "error",
    };
  }
  return { outcome: status.status };
};

const completionMessage = (
  payload: ValidatedFocusedFullWatchTickMessage,
  result: TerminalWatchResult,
): FocusedFullCompletionMessage => {
  const message: FocusedFullCompletionMessage = {
    body: payload.body,
    doName: payload.doName,
    outcome: result.outcome,
    role: payload.role,
    type: "focused-full-completion",
    watchId: payload.watchId,
    workKey: payload.workKey,
  };
  return result.error === undefined ? message : { ...message, error: result.error };
};

export const parseFocusedFullWatchHeader = (request: Request): ValidatedFocusedFullWatchPayload => {
  const header = request.headers.get(WATCH_REQUEST_HEADER);
  if (header === null) throw new Error(`Missing ${WATCH_REQUEST_HEADER} header`);
  const value: unknown = JSON.parse(header);
  if (!isFocusedFullWatchPayload(value)) {
    throw new Error(`Invalid ${WATCH_REQUEST_HEADER} header`);
  }
  return value;
};

export const isFocusedFullPredictUrl = (url: URL): boolean =>
  url.pathname === PREDICT_PATH &&
  url.searchParams.get("mode") === FULL_MODE &&
  url.searchParams.has("category") &&
  url.searchParams.has("keibajoCode") &&
  url.searchParams.has("raceBango") &&
  url.searchParams.has("runDate");

export const hasAcceptedResult = async (response: Response): Promise<boolean> => {
  if (!response.ok) return false;
  const lines: string[] = (await response.text()).split("\n").filter(Boolean);
  return lines.some((line) => {
    try {
      const value: unknown = JSON.parse(line);
      return isRecord(value) && value.type === RESULT_TYPE && value.status === ACCEPTED_STATUS;
    } catch {
      return false;
    }
  });
};

export const createFocusedFullWatchTickMessage = (
  payload: ValidatedFocusedFullWatchPayload,
  nowMs: number,
): ValidatedFocusedFullWatchTickMessage => ({
  ...payload,
  deadlineAtMs: nowMs + FOCUSED_FULL_WATCH_TIMEOUT_MS,
  type: "focused-full-watch-tick",
});

export const sendFocusedFullWatchMessageDurably = async (
  env: Env,
  message: FocusedFullCompletionMessage | FocusedFullWatchTickMessage,
  delaySeconds?: number,
): Promise<void> => {
  const queue = env.FOCUSED_FULL_COMPLETION_QUEUE;
  if (queue === undefined) throw new Error("FOCUSED_FULL_COMPLETION_QUEUE binding is missing");
  const outboxId = `${message.type}:${message.watchId}:${
    message.type === "focused-full-watch-tick" ? message.deadlineAtMs : message.outcome
  }`;
  await registerFocusedFullWatchOutbox({ delaySeconds, env, message, outboxId });
  if (delaySeconds === undefined) await queue.send(message);
  else await queue.send(message, { delaySeconds });
  await clearFocusedFullWatchOutbox({ env, outboxId });
};

export const buildFocusedFullStatusUrl = (body: FocusedFullWatchBody): string => {
  const searchParams = new URLSearchParams({
    category: body.category,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
    runDate: body.runYmd,
  });
  return `http://container${STATUS_PATH}?${searchParams.toString()}`;
};

export const pollFocusedFullWatchTick = async (
  payload: ValidatedFocusedFullWatchTickMessage,
  dependencies: FocusedFullWatchTickDependencies,
): Promise<FocusedFullCompletionMessage | undefined> => {
  const result = await terminalResult(payload, dependencies);
  return result === undefined ? undefined : completionMessage(payload, result);
};
