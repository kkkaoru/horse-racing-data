"use client";

import { useChat } from "@ai-sdk/react";
import type { LlmInference } from "@mediapipe/tasks-genai";
import type { ChatTransport, UIMessage, UIMessageChunk } from "ai";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import type { CourseInfo, RaceDetail, RaceListItem, Runner } from "../../../lib/race-types";
import {
  buildRaceAiDataCatalogForPrompt,
  fetchRaceAiExportData,
  type RaceAiDataReadiness,
  type RaceAiExportData,
} from "./race-ai-data";
import { buildGemmaPrompt, RACE_AI_DEFAULT_PROMPT } from "./race-ai-default-prompt";
import {
  ensureRaceAiModelBuffer,
  getRaceAiModelState,
  LATEST_RACE_AI_MODEL,
  subscribeRaceAiModelDownloads,
  type RaceAiModelState,
} from "./race-ai-model-manager";
import {
  deleteRaceAiLog,
  getRaceAiLog,
  getRaceAiSettings,
  saveRaceAiLog,
  subscribeRaceAiSettings,
  type RaceAiMessage,
  type RaceAiSettings,
  type RaceAiThoughtLog,
} from "./race-ai-storage";
import { useRealtimeRaceSelector } from "./realtime-client";

interface RaceAiAssistantProps {
  basePostgresqlData: {
    courseInfo: CourseInfo | null;
    race: RaceDetail;
    raceDayRaces: RaceListItem[];
    runners: Runner[];
  };
  baseProcessedData: Record<string, unknown>;
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

type WebGpuSupportState = "checking" | "supported" | "unsupported";
type ModelStatus = "downloading" | "idle" | "initializing" | "ready";
type GenerationStatus = "generating" | "idle" | "loading-data";
type GenerationStage =
  | "fetching-tool-data"
  | "generating-final"
  | "generating-tool-request"
  | "idle"
  | "loading-data"
  | "model-download"
  | "model-initialize"
  | "preparing-prompt"
  | "queued"
  | "streaming-answer";

interface RaceAiPredictionRow {
  confidence: number | null;
  horseName: string;
  horseNumber: string;
  jockeyName: string;
  rank: number;
  reason: string;
}

interface ParsedRaceAiResponse {
  answer: string;
  format: "json" | "text";
  needsTool: boolean;
  prediction: RaceAiPredictionRow[];
  thoughtLog: string;
  toolJavaScript: string | null;
}

interface ToolResult {
  body: unknown;
  status: number;
  url: string;
}

interface RaceAiDebugCommand {
  dryRun?: boolean;
  id: string;
  messages?: RaceAiMessage[];
  text?: string;
  type: "replace-messages" | "reset" | "send-message" | "simulate-model-initialize";
}

type RaceAiRuntimeLogLevel = "debug" | "error" | "info" | "warn";
type RaceAiRuntimeLogDetails = Record<string, boolean | null | number | string>;

interface RaceAiRuntimeLog {
  at: string;
  details: RaceAiRuntimeLogDetails | null;
  elapsedMs: number;
  id: string;
  level: RaceAiRuntimeLogLevel;
  message: string;
  sequence: number;
}

const WASM_BASE_URL = "https://cdn.jsdelivr.net/npm/@mediapipe/tasks-genai@0.10.27/wasm";
const REALTIME_RETHINK_DELAY_MS = 3_000;
const DEBUG_SYNC_INTERVAL_MS = 1_200;
const SERVER_COMMAND_SYNC_INTERVAL_MS = 5_000;
const MODEL_INITIALIZATION_TIMEOUT_MS = 180_000;
const MODEL_INITIALIZATION_TIMEOUT_SECONDS = MODEL_INITIALIZATION_TIMEOUT_MS / 1000;
const MODEL_INITIALIZATION_WATCHDOG_INTERVAL_MS = 1_000;
const LOCAL_MODEL_INITIALIZATION_TIMEOUT_STORAGE_KEY =
  "pc-keiba-race-ai-model-initialization-timeout-ms";
const LOG_LIMIT = 20;
const RUNTIME_LOG_LIMIT = 300;
const MAX_TOOL_CALLS = 3;
const MAX_INPUT_TOKENS = 10_000;
const PROMPT_CHAR_LIMIT = 11_000;
const MIN_PROMPT_CHAR_LIMIT = 5_500;
const TOOL_RESULT_CHAR_LIMIT = 4_000;
const MIN_TOOL_RESULT_CHAR_LIMIT = 1_500;
const TOOL_ROW_LIMIT = 16;
const TOOL_TEXT_LIMIT = 240;

const GENERATION_STAGE_LABELS: Record<GenerationStage, string> = {
  "fetching-tool-data": "追加データ取得中",
  "generating-final": "最終回答を生成中",
  "generating-tool-request": "必要データを判断中",
  idle: "待機中",
  "loading-data": "レースデータ取得中",
  "model-download": "AIモデル読込中",
  "model-initialize": "AIモデル初期化中",
  "preparing-prompt": "入力データを整理中",
  queued: "処理を開始中",
  "streaming-answer": "回答を表示中",
};

let sharedRaceAiModel: {
  instance: LlmInference;
  modelId: string;
} | null = null;
let sharedRaceAiModelPromise: Promise<LlmInference> | null = null;
let sharedRaceAiModelInitializationGeneration = 0;

const modelInitializationTimeoutMessage = (timeoutMs: number): string =>
  `AIモデル初期化が${Math.round(timeoutMs / 1000)}秒を超えたため停止しました。ブラウザのWebGPUが不安定な可能性があります。ページを再読み込みして再試行してください。`;

const createId = (): string => `${Date.now().toString(36)}-${crypto.randomUUID()}`;

const yieldToBrowser = (): Promise<void> =>
  new Promise((resolve) => {
    window.requestAnimationFrame(() => {
      window.setTimeout(resolve, 0);
    });
  });

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });

const withTimeout = async <T,>({
  message,
  ms,
  promise,
}: {
  message: string;
  ms: number;
  promise: Promise<T>;
}): Promise<T> => {
  let timeoutId: number | null = null;
  const timeoutPromise = new Promise<T>((_, reject) => {
    timeoutId = window.setTimeout(() => reject(new Error(message)), ms);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeoutId !== null) {
      window.clearTimeout(timeoutId);
    }
  }
};

const formatGenerationStageElapsedLabel = (stage: GenerationStage, seconds: number): string => {
  if (stage === "model-initialize" && seconds >= MODEL_INITIALIZATION_TIMEOUT_SECONDS) {
    return `${MODEL_INITIALIZATION_TIMEOUT_SECONDS}秒以上`;
  }
  return `${seconds}秒`;
};

const toRuntimeLogDetails = (
  details: Record<string, unknown> | undefined,
): RaceAiRuntimeLogDetails | null => {
  if (!details) {
    return null;
  }
  const entries: Array<[string, boolean | null | number | string]> = [];
  for (const [key, value] of Object.entries(details)) {
    if (typeof value === "string" || typeof value === "boolean" || value === null) {
      entries.push([key, value]);
      continue;
    }
    if (typeof value === "number") {
      entries.push([key, Number.isFinite(value) ? value : String(value)]);
      continue;
    }
    if (value === undefined) {
      continue;
    }
    try {
      entries.push([key, JSON.stringify(value)]);
    } catch {
      entries.push([key, "unserializable"]);
    }
  }
  return entries.length > 0 ? Object.fromEntries(entries) : null;
};

const formatRuntimeLogDetails = (details: RaceAiRuntimeLogDetails | null): string =>
  details ? JSON.stringify(details) : "";

const raceMessageToUiMessage = (message: RaceAiMessage): UIMessage => ({
  id: message.id,
  metadata: { createdAt: message.createdAt },
  parts: [{ text: message.content, type: "text" }],
  role: message.role,
});

const uiMessageText = (message: UIMessage | undefined): string =>
  (message?.parts ?? [])
    .map((part) => (part.type === "text" ? part.text : ""))
    .join("")
    .trim();

const streamDisplayText = async ({
  abortSignal,
  emit,
  text,
}: {
  abortSignal: AbortSignal | undefined;
  emit: (delta: string) => void;
  text: string;
}) => {
  const normalized = cleanModelText(text);
  const emitNext = (index: number): Promise<void> => {
    if (abortSignal?.aborted) {
      return Promise.reject(new DOMException("AI応答を中断しました。", "AbortError"));
    }
    if (index >= normalized.length) {
      return Promise.resolve();
    }
    emit(normalized.slice(index, index + 8));
    return new Promise((resolve) => {
      window.setTimeout(() => {
        resolve(emitNext(index + 8));
      }, 8);
    });
  };
  await emitNext(0);
};

const stableStringify = (value: unknown): string => {
  try {
    return JSON.stringify(value);
  } catch {
    return "";
  }
};

const buildRealtimeFingerprint = (payload: RealtimeRacePayload | null): string =>
  stableStringify({
    entries: payload?.raceEntries ?? null,
    odds: payload?.odds?.latest ?? null,
    results: payload?.raceResults ?? null,
    trackCondition: payload?.trackCondition ?? null,
  });

const stripJsonFence = (text: string): string => {
  const trimmed = text.trim();
  const fenceMatch = /^```(?:json)?\s*([\s\S]*?)\s*```$/u.exec(trimmed);
  return fenceMatch?.[1]?.trim() ?? trimmed;
};

const cleanModelText = (text: string): string =>
  text
    .replace(/<start_of_turn>|<end_of_turn>|<bos>|<eos>/gu, "")
    .replace(/^```(?:json|markdown|text)?\s*/u, "")
    .replace(/\s*```$/u, "")
    .trim();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isLocalhostBrowser = (): boolean =>
  typeof window !== "undefined" &&
  ["localhost", "127.0.0.1", "0.0.0.0", "::1"].includes(window.location.hostname);

const getModelInitializationTimeoutMs = (): number => {
  if (isLocalhostBrowser()) {
    const timeout = Number(
      window.localStorage.getItem(LOCAL_MODEL_INITIALIZATION_TIMEOUT_STORAGE_KEY),
    );
    if (Number.isFinite(timeout) && timeout >= 1_000) {
      return Math.min(MODEL_INITIALIZATION_TIMEOUT_MS, Math.floor(timeout));
    }
  }
  return MODEL_INITIALIZATION_TIMEOUT_MS;
};

const uiMessageToRaceMessage = (message: UIMessage): RaceAiMessage | null => {
  if (message.role !== "assistant" && message.role !== "user") {
    return null;
  }
  const content = uiMessageText(message);
  if (!content) {
    return null;
  }
  const metadata = isRecord(message.metadata) ? message.metadata : null;
  return {
    content,
    createdAt:
      typeof metadata?.createdAt === "string" ? metadata.createdAt : new Date().toISOString(),
    id: message.id,
    role: message.role,
  };
};

const parseDebugRaceMessage = (value: unknown): RaceAiMessage | null => {
  if (!isRecord(value)) {
    return null;
  }
  if (value.role !== "assistant" && value.role !== "user") {
    return null;
  }
  return {
    content: typeof value.content === "string" ? value.content : "",
    createdAt: typeof value.createdAt === "string" ? value.createdAt : new Date().toISOString(),
    id: typeof value.id === "string" ? value.id : createId(),
    role: value.role,
  };
};

const parseDebugCommand = (value: unknown): RaceAiDebugCommand | null => {
  if (!isRecord(value)) {
    return null;
  }
  if (
    value.type !== "replace-messages" &&
    value.type !== "reset" &&
    value.type !== "send-message" &&
    value.type !== "simulate-model-initialize"
  ) {
    return null;
  }
  return {
    id: typeof value.id === "string" ? value.id : createId(),
    dryRun: value.dryRun === true,
    messages: Array.isArray(value.messages)
      ? value.messages
          .map(parseDebugRaceMessage)
          .filter((message): message is RaceAiMessage => message !== null)
      : undefined,
    text: typeof value.text === "string" ? value.text : undefined,
    type: value.type,
  };
};

const truncatePromptText = (text: string, maxLength: number): string =>
  text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;

const compactPromptValue = (value: unknown, depth = 0): unknown => {
  if (
    value === null ||
    value === undefined ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "string") {
    return truncatePromptText(value, TOOL_TEXT_LIMIT);
  }
  if (Array.isArray(value)) {
    const rows = value.slice(0, TOOL_ROW_LIMIT).map((row) => compactPromptValue(row, depth + 1));
    return value.length > rows.length
      ? [...rows, { omittedItems: value.length - rows.length }]
      : rows;
  }
  if (!isRecord(value)) {
    return null;
  }
  if (depth >= 4) {
    return "[omitted]";
  }
  return Object.fromEntries(
    Object.entries(value)
      .filter(([key]) => key !== "details" && key !== "dataFingerprint")
      .map(([key, entryValue]) => [key, compactPromptValue(entryValue, depth + 1)]),
  );
};

const formatMessagesForPrompt = (messages: RaceAiMessage[]): unknown =>
  messages.slice(-3).map((message) => ({
    content: truncatePromptText(message.content, 360),
    createdAt: message.createdAt,
    role: message.role,
  }));

const formatThoughtLogsForPrompt = (thoughtLogs: RaceAiThoughtLog[]): unknown =>
  thoughtLogs.slice(-1).map((log) => ({
    content: truncatePromptText(log.content, 360),
    createdAt: log.createdAt,
    modelVersion: log.modelVersion,
    trigger: log.trigger,
  }));

const getRecord = (record: Record<string, unknown> | null, key: string) => {
  const value = record?.[key];
  return isRecord(value) ? value : null;
};

const getArray = (record: Record<string, unknown> | null, key: string): unknown[] => {
  const value = record?.[key];
  return Array.isArray(value) ? value : [];
};

const pickFields = (value: unknown, fields: string[]): Record<string, unknown> | null => {
  if (!isRecord(value)) {
    return null;
  }
  return Object.fromEntries(
    fields
      .filter((field) => field in value)
      .map((field) => [field, compactPromptValue(value[field], 1)]),
  );
};

const pickRows = (rows: unknown[], fields: string[]): unknown[] =>
  rows
    .slice(0, TOOL_ROW_LIMIT)
    .map((row) => pickFields(row, fields))
    .filter((row): row is Record<string, unknown> => row !== null);

const compactApiSpecForPrompt = (body: unknown): unknown => {
  if (!isRecord(body)) {
    return compactPromptValue(body);
  }
  const paths = isRecord(body.paths) ? body.paths : {};
  return {
    info: pickFields(body.info, ["title", "version", "description"]),
    paths: Object.entries(paths).map(([path, methods]) => ({
      methods: isRecord(methods) ? Object.keys(methods) : [],
      path,
    })),
  };
};

const compactAiDataForPrompt = (body: unknown): unknown => {
  if (!isRecord(body)) {
    return compactPromptValue(body);
  }
  const finishPrediction = getRecord(body, "finishPrediction");
  const overallScore = getRecord(body, "overallScore");
  const timeScore = getRecord(body, "timeScore");
  const realtime = getRecord(body, "realtime");
  const courseDisplay = getRecord(body, "courseDisplay");
  return compactPromptValue({
    courseDisplay: courseDisplay
      ? {
          facts: getArray(courseDisplay, "facts").slice(0, 6),
          imagePath: courseDisplay.imagePath ?? null,
          paragraphs: getArray(courseDisplay, "paragraphs").slice(0, 2),
        }
      : undefined,
    courseInfo: pickFields(getRecord(body, "courseInfo"), [
      "courseKaishuNengappi",
      "courseSetsumei",
    ]),
    finishPrediction: finishPrediction
      ? {
          evaluation: pickFields(getRecord(finishPrediction, "evaluation"), [
            "categoryLabel",
            "raceCount",
            "top1Accuracy",
            "top3BoxAccuracy",
            "pairScore",
          ]),
          rows: pickRows(getArray(finishPrediction, "rows"), [
            "horseNumber",
            "horseName",
            "jockeyName",
            "predictedRank",
            "confidence",
            "finishPredictionScore",
            "overallEvaluationScore",
            "paddockScore",
            "odds",
            "popularity",
            "winProbability",
            "showProbability",
            "entryStatus",
          ]),
        }
      : undefined,
    meta: body.meta ?? null,
    overallScore: overallScore
      ? {
          rows: pickRows(getArray(overallScore, "rows"), [
            "horseNumber",
            "horseName",
            "jockeyName",
            "score",
            "odds",
            "popularity",
            "entryStatus",
          ]),
        }
      : undefined,
    race: pickFields(body.race, [
      "source",
      "kaisaiNen",
      "kaisaiTsukihi",
      "keibajoCode",
      "raceBango",
      "kyosomeiHondai",
      "kyosoJokenMeisho",
      "kyori",
      "trackCode",
      "hassoJikoku",
      "shussoTosu",
    ]),
    raceDayRaces: pickRows(getArray(body, "raceDayRaces"), [
      "source",
      "keibajoCode",
      "raceBango",
      "kyosomeiHondai",
      "hassoJikoku",
    ]),
    realtime: realtime
      ? {
          entries: pickFields(getRecord(realtime, "entries"), ["fetchedAt", "horses"]),
          oddsFukusho: pickFields(getRecord(realtime, "oddsFukusho"), ["fetchedAt", "rows"]),
          oddsTansho: pickFields(getRecord(realtime, "oddsTansho"), ["fetchedAt", "rows"]),
          results: pickFields(getRecord(realtime, "results"), ["fetchedAt", "horses"]),
          trackCondition: realtime.trackCondition ?? null,
          weights: pickFields(getRecord(realtime, "weights"), ["fetchedAt", "horses"]),
        }
      : undefined,
    runners: pickRows(getArray(body, "runners"), [
      "umaban",
      "wakuban",
      "bamei",
      "kishumeiRyakusho",
      "chokyoshimeiRyakusho",
      "banushimei",
      "bataiju",
      "tanshoOdds",
      "tanshoNinkijun",
      "kakuteiChakujun",
    ]),
    timeScore: timeScore
      ? {
          bloodlineRows: pickRows(getArray(timeScore, "bloodlineRows"), [
            "horseNumber",
            "horseName",
            "score",
            "sireScore",
            "broodmareSireScore",
          ]),
          correlationRows: pickRows(getArray(timeScore, "correlationRows"), [
            "horseNumber",
            "horseName",
            "score",
          ]),
          rows: pickRows(getArray(timeScore, "rows"), ["horseNumber", "horseName", "score"]),
          similarRows: pickRows(getArray(timeScore, "similarRows"), [
            "horseNumber",
            "horseName",
            "score",
          ]),
        }
      : undefined,
  });
};

const compactToolResultForPrompt = (result: ToolResult): Record<string, unknown> => {
  const body = result.url.startsWith("/api/spec")
    ? compactApiSpecForPrompt(result.body)
    : result.url.includes("/ai/data")
      ? compactAiDataForPrompt(result.body)
      : compactPromptValue(result.body);
  return {
    body,
    status: result.status,
    url: result.url,
  };
};

const parseModelResponse = (text: string): ParsedRaceAiResponse => {
  const cleaned = cleanModelText(stripJsonFence(text));
  const start = cleaned.indexOf("{");
  const end = cleaned.lastIndexOf("}");
  const jsonText = start >= 0 && end > start ? cleaned.slice(start, end + 1) : cleaned;
  try {
    const parsed: unknown = JSON.parse(jsonText);
    if (!isRecord(parsed)) {
      throw new Error("model response is not an object");
    }
    const prediction = Array.isArray(parsed.prediction)
      ? parsed.prediction
          .map((row) => {
            if (!isRecord(row)) {
              return null;
            }
            return {
              confidence:
                typeof row.confidence === "number" && Number.isFinite(row.confidence)
                  ? row.confidence
                  : null,
              horseName: typeof row.horseName === "string" ? row.horseName : "-",
              horseNumber: typeof row.horseNumber === "string" ? row.horseNumber : "-",
              jockeyName: typeof row.jockeyName === "string" ? row.jockeyName : "-",
              rank: typeof row.rank === "number" && Number.isFinite(row.rank) ? row.rank : 0,
              reason: typeof row.reason === "string" ? row.reason : "",
            } satisfies RaceAiPredictionRow;
          })
          .filter((row): row is RaceAiPredictionRow => row !== null && row.rank > 0)
      : [];
    return {
      answer: typeof parsed.answer === "string" ? parsed.answer : cleaned,
      format: "json",
      needsTool: parsed.needsTool === true,
      prediction,
      thoughtLog: typeof parsed.thoughtLog === "string" ? parsed.thoughtLog : "",
      toolJavaScript:
        typeof parsed.toolJavaScript === "string" && parsed.toolJavaScript.trim()
          ? parsed.toolJavaScript
          : null,
    };
  } catch {
    return {
      answer:
        cleaned || "AIの応答をJSONとして解析できませんでした。データ取得後に再試行してください。",
      format: "text",
      needsTool: false,
      prediction: [],
      thoughtLog:
        "モデルが指定形式のJSONではなく自然文を返したため、回答本文を表示用に整形しました。",
      toolJavaScript: null,
    };
  }
};

const buildDefaultRaceAiDataUrl = (data: RaceAiExportData): string => {
  const route = data.meta.route;
  const searchParams = new URLSearchParams(
    typeof window === "undefined" ? "" : window.location.search,
  );
  searchParams.set("source", route.source);
  searchParams.set("parts", "race,runners,finishPrediction,overallScore,realtime");
  searchParams.set("realtimeParts", "entries,oddsTansho,weights,results");
  return `/api/races/${route.year}/${route.month}/${route.day}/${route.keibajoCode}/${route.raceNumber}/ai/data?${searchParams.toString()}`;
};

const answerBlocks = (text: string): string[] =>
  cleanModelText(text)
    .split(/\n{2,}/u)
    .map((block) => block.trim())
    .filter(Boolean);

const responseNeedsFinalAnswer = (toolResults: ToolResult[], formatRetryCount: number): boolean =>
  toolResults.length > 0 && formatRetryCount > 0;

const buildPrompt = ({
  data,
  messages,
  promptCharLimit = PROMPT_CHAR_LIMIT,
  request,
  thoughtLogs,
  toolResultCharLimit = TOOL_RESULT_CHAR_LIMIT,
  toolResults,
}: {
  data: RaceAiExportData;
  messages: RaceAiMessage[];
  promptCharLimit?: number;
  request: string;
  thoughtLogs: RaceAiThoughtLog[];
  toolResultCharLimit?: number;
  toolResults: ToolResult[];
}): string => {
  const hasToolResults = toolResults.length > 0;
  const toolResultsText = hasToolResults
    ? truncatePromptText(
        JSON.stringify(toolResults.map((result) => compactToolResultForPrompt(result))),
        toolResultCharLimit,
      )
    : "なし";
  const promptBody = [
    RACE_AI_DEFAULT_PROMPT,
    "現在のユーザー依頼:",
    request,
    "データ取得方針:",
    hasToolResults
      ? "取得済みツール結果を根拠に回答してください。まだ不足する場合だけ追加でfetchJsonを1回要求できます。"
      : "初回入力には実データがありません。具体的な予想や事実回答には、まずtoolJavaScriptで必要最小限のfetchJsonを1回要求してください。",
    "直近の対話ログ:",
    JSON.stringify(formatMessagesForPrompt(messages)),
    "直近の思考ログ:",
    JSON.stringify(formatThoughtLogsForPrompt(thoughtLogs)),
    "AI向けデータカタログ:",
    JSON.stringify(buildRaceAiDataCatalogForPrompt(data)),
    "取得済みツール結果:",
    toolResultsText,
  ].join("\n\n");
  return buildGemmaPrompt(truncatePromptText(promptBody, promptCharLimit));
};

const buildTokenSafePrompt = ({
  data,
  llm,
  messages,
  request,
  thoughtLogs,
  toolResults,
}: {
  data: RaceAiExportData;
  llm: LlmInference;
  messages: RaceAiMessage[];
  request: string;
  thoughtLogs: RaceAiThoughtLog[];
  toolResults: ToolResult[];
}): string => {
  let promptCharLimit = PROMPT_CHAR_LIMIT;
  let toolResultCharLimit = TOOL_RESULT_CHAR_LIMIT;
  let prompt = buildPrompt({
    data,
    messages,
    promptCharLimit,
    request,
    thoughtLogs,
    toolResultCharLimit,
    toolResults,
  });

  for (let attempt = 0; attempt < 4; attempt += 1) {
    const tokenCount = llm.sizeInTokens(prompt);
    if (!tokenCount || tokenCount <= MAX_INPUT_TOKENS) {
      return prompt;
    }
    const ratio = Math.max(0.42, (MAX_INPUT_TOKENS / tokenCount) * 0.82);
    promptCharLimit = Math.max(MIN_PROMPT_CHAR_LIMIT, Math.floor(promptCharLimit * ratio));
    toolResultCharLimit = Math.max(
      MIN_TOOL_RESULT_CHAR_LIMIT,
      Math.floor(toolResultCharLimit * ratio),
    );
    prompt = buildPrompt({
      data,
      messages,
      promptCharLimit,
      request,
      thoughtLogs,
      toolResultCharLimit,
      toolResults,
    });
  }

  return prompt;
};

const callOptionalLlmMethod = (
  llm: LlmInference,
  methodName: "cancelProcessing" | "clearCancelSignals",
) => {
  const method: unknown = Reflect.get(llm, methodName);
  if (typeof method === "function") {
    method.call(llm);
  }
};

const generateModelResponse = async ({
  abortSignal,
  llm,
  onProgress,
  prompt,
}: {
  abortSignal: AbortSignal | undefined;
  llm: LlmInference;
  onProgress?: (partialResult: string, done: boolean) => void;
  prompt: string;
}): Promise<string> => {
  if (abortSignal?.aborted) {
    throw new DOMException("AI応答を中断しました。", "AbortError");
  }
  const cancelProcessing = () => {
    callOptionalLlmMethod(llm, "cancelProcessing");
  };
  callOptionalLlmMethod(llm, "clearCancelSignals");
  abortSignal?.addEventListener("abort", cancelProcessing, { once: true });
  try {
    return await llm.generateResponse(prompt, onProgress);
  } finally {
    abortSignal?.removeEventListener("abort", cancelProcessing);
    callOptionalLlmMethod(llm, "clearCancelSignals");
  }
};

const runToolJavaScript = async (code: string): Promise<ToolResult> => {
  const normalized = code.replace(/```(?:tool-js|javascript|js)?/gu, "").trim();
  const match = /fetchJson\s*\(\s*["']([^"']+)["']\s*\)/u.exec(normalized);
  if (!match?.[1]) {
    throw new Error('fetchJson("/api/...") の単一呼び出しだけ実行できます。');
  }
  const url = new URL(match[1], window.location.origin);
  if (url.origin !== window.location.origin || !url.pathname.startsWith("/api/")) {
    throw new Error("fetchJson は同一オリジンの /api/ のみ参照できます。");
  }
  const response = await fetch(`${url.pathname}${url.search}`, { cache: "no-store" });
  return {
    body: await response.json(),
    status: response.status,
    url: `${url.pathname}${url.search}`,
  };
};

const createLocalChatStream = ({
  abortSignal,
  execute,
}: {
  abortSignal: AbortSignal | undefined;
  execute: (emit: (delta: string) => void) => Promise<void>;
}): ReadableStream<UIMessageChunk> =>
  new ReadableStream<UIMessageChunk>({
    async start(controller) {
      const textId = createId();
      controller.enqueue({ type: "start" });
      controller.enqueue({ type: "start-step" });
      controller.enqueue({ id: textId, type: "text-start" });
      try {
        await yieldToBrowser();
        await execute((delta) => {
          if (!abortSignal?.aborted && delta) {
            controller.enqueue({ delta, id: textId, type: "text-delta" });
          }
        });
        controller.enqueue({ id: textId, type: "text-end" });
        controller.enqueue({ type: "finish-step" });
        controller.enqueue({ finishReason: "stop", type: "finish" });
      } catch (caught) {
        const message = caught instanceof Error ? caught.message : String(caught);
        controller.enqueue({ errorText: message, type: "error" });
        controller.enqueue({ type: "finish-step" });
        controller.enqueue({ finishReason: "error", type: "finish" });
      } finally {
        controller.close();
      }
    },
  });

const appendLimited = <T,>(rows: T[], row: T): T[] => [...rows, row].slice(-LOG_LIMIT);

export function RaceAiAssistant(props: RaceAiAssistantProps) {
  const [supportState, setSupportState] = useState<WebGpuSupportState>("checking");
  const [modelStatus, setModelStatus] = useState<ModelStatus>("idle");
  const [generationStatus, setGenerationStatus] = useState<GenerationStatus>("idle");
  const [generationStage, setGenerationStage] = useState<GenerationStage>("idle");
  const [generationStageStartedAt, setGenerationStageStartedAt] = useState<number | null>(null);
  const [runtimeNow, setRuntimeNow] = useState(() => Date.now());
  const [aiSectionOpen, setAiSectionOpen] = useState(true);
  const [dataReadiness, setDataReadiness] = useState<RaceAiDataReadiness | null>(null);
  const [dataReadinessStatus, setDataReadinessStatus] = useState<"idle" | "loading">("idle");
  const [settings, setSettings] = useState<RaceAiSettings | null>(null);
  const [modelState, setModelState] = useState<RaceAiModelState | null>(null);
  const [messages, setMessages] = useState<RaceAiMessage[]>([]);
  const [thoughtLogs, setThoughtLogs] = useState<RaceAiThoughtLog[]>([]);
  const [prediction, setPrediction] = useState<RaceAiPredictionRow[]>([]);
  const [answer, setAnswer] = useState<string>("");
  const [input, setInput] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [modelPartialLength, setModelPartialLength] = useState(0);
  const [debugEnabled, setDebugEnabled] = useState(false);
  const [chatSessionVersion, setChatSessionVersion] = useState(0);
  const [runtimeLogs, setRuntimeLogs] = useState<RaceAiRuntimeLog[]>([]);
  const llmRef = useRef<LlmInference | null>(null);
  const autoStartedRaceKeyRef = useRef<string | null>(null);
  const runningRef = useRef(false);
  const resetVersionRef = useRef(0);
  const runtimeLogBaseAtRef = useRef(Date.now());
  const runtimeLogSequenceRef = useRef(0);
  const runtimeLogsRef = useRef<RaceAiRuntimeLog[]>([]);
  const lastModelStateLogRef = useRef("");
  const lastRuntimeUiSnapshotLogRef = useRef("");
  const modelInitializationTimeoutHandledAtRef = useRef<number | null>(null);
  const messagesRef = useRef<RaceAiMessage[]>([]);
  const thoughtLogsRef = useRef<RaceAiThoughtLog[]>([]);
  const suppressChatErrorUntilRef = useRef(0);
  const handledDebugCommandIdsRef = useRef<Set<string>>(new Set());
  const handledServerCommandIdsRef = useRef<Set<string>>(new Set());
  const hydratedRaceKeyRef = useRef<string | null>(null);
  const modelWarmupStartedRef = useRef(false);
  const lastDebugSnapshotRef = useRef("");
  const lastRealtimeFingerprintRef = useRef("");
  const lastUserRequestRef = useRef("このレースの着順を予想してください。");
  const realtimePayload = useRealtimeRaceSelector((state) => state.payload);
  const realtimeFingerprint = useMemo(
    () => buildRealtimeFingerprint(realtimePayload),
    [realtimePayload],
  );

  const raceKey = `${props.source}:${props.year}${props.month}${props.day}:${props.keibajoCode}:${props.raceNumber}`;
  const chatId = `${raceKey}:${chatSessionVersion}`;

  const resetRuntimeLogs = useCallback(
    (reason: string, details?: Record<string, unknown>): void => {
      const now = Date.now();
      runtimeLogBaseAtRef.current = now;
      runtimeLogSequenceRef.current = 1;
      lastModelStateLogRef.current = "";
      lastRuntimeUiSnapshotLogRef.current = "";
      const log: RaceAiRuntimeLog = {
        at: new Date(now).toISOString(),
        details: toRuntimeLogDetails({
          raceKey,
          reason,
          route: `${props.year}/${props.month}/${props.day}/${props.keibajoCode}/${props.raceNumber}`,
          source: props.source,
          ...details,
        }),
        elapsedMs: 0,
        id: createId(),
        level: "info",
        message: "race AI runtime log initialized",
        sequence: 1,
      };
      runtimeLogsRef.current = [log];
      setRuntimeLogs([log]);
      console.info("[race-ai] race AI runtime log initialized", {
        details: log.details,
        elapsedMs: log.elapsedMs,
        raceKey,
        sequence: log.sequence,
      });
    },
    [
      props.day,
      props.keibajoCode,
      props.month,
      props.raceNumber,
      props.source,
      props.year,
      raceKey,
    ],
  );

  const addRuntimeLog = useCallback(
    (level: RaceAiRuntimeLogLevel, message: string, details?: Record<string, unknown>): void => {
      const now = Date.now();
      const sequence = runtimeLogSequenceRef.current + 1;
      runtimeLogSequenceRef.current = sequence;
      const log: RaceAiRuntimeLog = {
        at: new Date(now).toISOString(),
        details: toRuntimeLogDetails(details),
        elapsedMs: Math.max(0, now - runtimeLogBaseAtRef.current),
        id: createId(),
        level,
        message,
        sequence,
      };
      const nextLogs = [...runtimeLogsRef.current, log].slice(-RUNTIME_LOG_LIMIT);
      runtimeLogsRef.current = nextLogs;
      setRuntimeLogs(nextLogs);
      const consolePayload = {
        details: log.details,
        elapsedMs: log.elapsedMs,
        raceKey,
        sequence,
      };
      if (level === "error") {
        console.error(`[race-ai] ${message}`, consolePayload);
      } else if (level === "warn") {
        console.warn(`[race-ai] ${message}`, consolePayload);
      } else if (level === "info") {
        console.info(`[race-ai] ${message}`, consolePayload);
      } else {
        console.debug(`[race-ai] ${message}`, consolePayload);
      }
    },
    [raceKey],
  );

  const setRuntimeStage = useCallback((stage: GenerationStage) => {
    const now = Date.now();
    setGenerationStage(stage);
    setGenerationStageStartedAt(stage === "idle" ? null : now);
    setRuntimeNow(now);
  }, []);

  useEffect(() => {
    resetRuntimeLogs("race-mounted-or-changed");
  }, [resetRuntimeLogs]);

  useEffect(() => {
    if (generationStage === "idle") {
      return undefined;
    }
    const timer = window.setInterval(() => {
      setRuntimeNow(Date.now());
    }, 500);
    return () => {
      window.clearInterval(timer);
    };
  }, [generationStage]);

  useEffect(() => {
    setDebugEnabled(isLocalhostBrowser());
  }, []);

  useEffect(() => {
    addRuntimeLog("info", "checking WebGPU support", {
      hasNavigator: typeof navigator !== "undefined",
      hasWebGpu: typeof navigator !== "undefined" && "gpu" in navigator,
    });
    setSupportState("checking");
    if (typeof navigator === "undefined" || !("gpu" in navigator)) {
      setSupportState("unsupported");
      addRuntimeLog("warn", "WebGPU unsupported");
      return;
    }
    setSupportState("supported");
    addRuntimeLog("info", "WebGPU supported");
  }, [addRuntimeLog]);

  useEffect(() => {
    const refreshModelState = () => {
      void getRaceAiModelState(LATEST_RACE_AI_MODEL)
        .then(setModelState)
        .catch((caught: unknown) => {
          addRuntimeLog("error", "failed to refresh AI model state", {
            error: caught instanceof Error ? caught.message : String(caught),
          });
        });
    };
    const nextSettings = getRaceAiSettings();
    setSettings(nextSettings);
    addRuntimeLog("info", "AI settings loaded", {
      autoStart: nextSettings.autoStart,
      consent: nextSettings.consent,
      modelVersion: LATEST_RACE_AI_MODEL.version,
    });
    refreshModelState();
    const unsubscribeSettings = subscribeRaceAiSettings(setSettings);
    const unsubscribeDownloads = subscribeRaceAiModelDownloads(refreshModelState);
    return () => {
      unsubscribeSettings();
      unsubscribeDownloads();
    };
  }, [addRuntimeLog]);

  useEffect(() => {
    if (!modelState) {
      return;
    }
    const progressBucket =
      modelState.progress === null || modelState.progress === undefined
        ? "none"
        : `${Math.floor(modelState.progress * 10) * 10}%`;
    const fingerprint = [
      modelState.status,
      progressBucket,
      modelState.error ?? "",
      modelState.totalBytes ?? "",
    ].join(":");
    if (lastModelStateLogRef.current === fingerprint) {
      return;
    }
    lastModelStateLogRef.current = fingerprint;
    addRuntimeLog(modelState.error ? "error" : "debug", "AI model state changed", {
      downloadedBytes: modelState.downloadedBytes,
      error: modelState.error,
      progress:
        modelState.progress === null || modelState.progress === undefined
          ? null
          : Math.round(modelState.progress * 100),
      status: modelState.status,
      totalBytes: modelState.totalBytes,
    });
  }, [addRuntimeLog, modelState]);

  useEffect(() => {
    let cancelled = false;
    const loadResetVersion = resetVersionRef.current;
    void (async () => {
      const log = await getRaceAiLog(raceKey);
      if (cancelled || resetVersionRef.current !== loadResetVersion) {
        return;
      }
      setMessages(log.messages);
      setThoughtLogs(log.thoughtLogs);
      messagesRef.current = log.messages;
      thoughtLogsRef.current = log.thoughtLogs;
      hydratedRaceKeyRef.current = raceKey;
    })();
    return () => {
      cancelled = true;
    };
  }, [raceKey]);

  useEffect(() => {
    autoStartedRaceKeyRef.current = null;
  }, [raceKey]);

  const persistLogs = useCallback(
    async (nextMessages: RaceAiMessage[], nextThoughtLogs: RaceAiThoughtLog[]) => {
      messagesRef.current = nextMessages;
      thoughtLogsRef.current = nextThoughtLogs;
      setMessages(nextMessages);
      setThoughtLogs(nextThoughtLogs);
      await saveRaceAiLog({
        messages: nextMessages,
        raceKey,
        thoughtLogs: nextThoughtLogs,
        updatedAt: new Date().toISOString(),
      });
    },
    [raceKey],
  );

  const ensureModel = useCallback(async (): Promise<LlmInference> => {
    addRuntimeLog("info", "ensure AI model started", {
      hasLocalInstance: Boolean(llmRef.current),
      hasSharedInstance: sharedRaceAiModel?.modelId === LATEST_RACE_AI_MODEL.id,
      hasSharedPromise: Boolean(sharedRaceAiModelPromise),
    });
    if (llmRef.current) {
      addRuntimeLog("debug", "using local AI model instance");
      return llmRef.current;
    }
    if (sharedRaceAiModel?.modelId === LATEST_RACE_AI_MODEL.id) {
      llmRef.current = sharedRaceAiModel.instance;
      setModelStatus("ready");
      addRuntimeLog("debug", "using shared AI model instance", {
        modelId: sharedRaceAiModel.modelId,
      });
      return sharedRaceAiModel.instance;
    }
    if (getRaceAiSettings().consent !== "granted") {
      addRuntimeLog("warn", "AI model initialization blocked by consent setting");
      throw new Error("AI利用が許可されていません。マイページでAI利用を許可してください。");
    }
    if (sharedRaceAiModelPromise) {
      const timeoutMs = getModelInitializationTimeoutMs();
      const timeoutMessage = modelInitializationTimeoutMessage(timeoutMs);
      const existingPromise = sharedRaceAiModelPromise;
      addRuntimeLog("info", "waiting for existing AI model initialization");
      setRuntimeStage("model-initialize");
      setModelStatus("initializing");
      await yieldToBrowser();
      try {
        const sharedInstance = await withTimeout({
          message: timeoutMessage,
          ms: timeoutMs,
          promise: existingPromise,
        });
        llmRef.current = sharedInstance;
        setModelStatus("ready");
        addRuntimeLog("info", "existing AI model initialization completed");
        return sharedInstance;
      } catch (caught) {
        if (
          caught instanceof Error &&
          caught.message === timeoutMessage &&
          sharedRaceAiModel === null
        ) {
          sharedRaceAiModelInitializationGeneration += 1;
          sharedRaceAiModelPromise = null;
        }
        addRuntimeLog("error", "existing AI model initialization failed", {
          error: caught instanceof Error ? caught.message : String(caught),
        });
        setModelStatus("idle");
        throw caught;
      }
    }
    setError(null);
    setRuntimeStage("model-download");
    setModelStatus("downloading");
    await yieldToBrowser();
    const downloadStartedAt = Date.now();
    addRuntimeLog("info", "AI model buffer ensure started", {
      modelSizeBytes: LATEST_RACE_AI_MODEL.sizeBytes,
      modelVersion: LATEST_RACE_AI_MODEL.version,
    });
    const buffer = await ensureRaceAiModelBuffer({
      confirmDownload: true,
      model: LATEST_RACE_AI_MODEL,
    });
    addRuntimeLog("info", "AI model buffer ready", {
      bufferBytes: buffer.byteLength,
      elapsedMs: Date.now() - downloadStartedAt,
    });
    setRuntimeStage("model-initialize");
    setModelStatus("initializing");
    await yieldToBrowser();
    const timeoutMs = getModelInitializationTimeoutMs();
    const timeoutMessage = modelInitializationTimeoutMessage(timeoutMs);
    const initializationGeneration = sharedRaceAiModelInitializationGeneration + 1;
    sharedRaceAiModelInitializationGeneration = initializationGeneration;
    const initializationPromise = (async () => {
      const initializationStartedAt = Date.now();
      addRuntimeLog("info", "MediaPipe tasks-genai import started");
      const { FilesetResolver, LlmInference: LlmInferenceClass } =
        await import("@mediapipe/tasks-genai");
      addRuntimeLog("info", "MediaPipe tasks-genai import completed", {
        elapsedMs: Date.now() - initializationStartedAt,
      });
      addRuntimeLog("info", "MediaPipe WASM resolver started", {
        wasmBaseUrl: WASM_BASE_URL,
      });
      const genai = await FilesetResolver.forGenAiTasks(WASM_BASE_URL);
      addRuntimeLog("info", "MediaPipe WASM resolver completed", {
        elapsedMs: Date.now() - initializationStartedAt,
      });
      addRuntimeLog("info", "WebGPU device creation started");
      const device = await LlmInferenceClass.createWebGpuDevice();
      addRuntimeLog("info", "WebGPU device creation completed", {
        elapsedMs: Date.now() - initializationStartedAt,
      });
      addRuntimeLog("info", "LlmInference createFromOptions started", {
        maxTokens: 16_384,
        modelBytes: buffer.byteLength,
      });
      const instance = await LlmInferenceClass.createFromOptions(genai, {
        baseOptions: {
          delegate: "GPU",
          gpuOptions: { device },
          modelAssetBuffer: new Uint8Array(buffer),
        },
        maxTokens: 16_384,
        randomSeed: 20260518,
        temperature: 0.25,
        topK: 40,
      });
      if (initializationGeneration !== sharedRaceAiModelInitializationGeneration) {
        callOptionalLlmMethod(instance, "cancelProcessing");
        throw new Error("AIモデル初期化がキャンセルされました。");
      }
      addRuntimeLog("info", "LlmInference createFromOptions completed", {
        elapsedMs: Date.now() - initializationStartedAt,
      });
      sharedRaceAiModel = {
        instance,
        modelId: LATEST_RACE_AI_MODEL.id,
      };
      return instance;
    })();
    sharedRaceAiModelPromise = initializationPromise;
    try {
      const instance = await withTimeout({
        message: timeoutMessage,
        ms: timeoutMs,
        promise: initializationPromise,
      });
      llmRef.current = instance;
      setModelStatus("ready");
      addRuntimeLog("info", "AI model ready", {
        modelId: LATEST_RACE_AI_MODEL.id,
      });
      return instance;
    } catch (caught) {
      if (
        caught instanceof Error &&
        caught.message === timeoutMessage &&
        sharedRaceAiModel === null
      ) {
        sharedRaceAiModelInitializationGeneration += 1;
      }
      addRuntimeLog("error", "AI model initialization failed", {
        error: caught instanceof Error ? caught.message : String(caught),
      });
      setModelStatus("idle");
      throw caught;
    } finally {
      if (sharedRaceAiModelPromise === initializationPromise) {
        sharedRaceAiModelPromise = null;
      }
    }
  }, [addRuntimeLog, setRuntimeStage]);

  useEffect(() => {
    if (
      supportState !== "supported" ||
      settings?.consent !== "granted" ||
      modelState?.status !== "downloaded" ||
      modelWarmupStartedRef.current ||
      llmRef.current
    ) {
      return undefined;
    }
    modelWarmupStartedRef.current = true;
    let cancelled = false;
    const timer = window.setTimeout(() => {
      void ensureModel()
        .catch((caught) => {
          if (!cancelled) {
            setError(caught instanceof Error ? caught.message : String(caught));
          }
        })
        .finally(() => {
          if (!cancelled && !runningRef.current) {
            setRuntimeStage("idle");
          }
        });
    }, 1_200);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [ensureModel, modelState?.status, settings?.consent, setRuntimeStage, supportState]);

  const loadRaceDataSnapshot = useCallback(async (): Promise<RaceAiExportData> => {
    setDataReadinessStatus("loading");
    try {
      const data = await fetchRaceAiExportData(props);
      setDataReadiness(data.aiReady.dataReadiness);
      return data;
    } finally {
      setDataReadinessStatus("idle");
    }
  }, [props]);

  useEffect(() => {
    if (supportState !== "supported" || settings?.consent !== "granted") {
      return undefined;
    }
    let cancelled = false;
    setDataReadinessStatus("loading");
    void (async () => {
      try {
        const data = await fetchRaceAiExportData(props);
        if (!cancelled) {
          setDataReadiness(data.aiReady.dataReadiness);
        }
      } finally {
        if (!cancelled) {
          setDataReadinessStatus("idle");
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [props, settings?.consent, supportState]);

  const runAi = useCallback(
    async (
      request: string,
      trigger: string,
      emit?: (delta: string) => void,
      abortSignal?: AbortSignal,
    ) => {
      if (runningRef.current) {
        throw new Error("AI応答を生成中です。");
      }
      runningRef.current = true;
      const runResetVersion = resetVersionRef.current;
      setError(null);
      setModelPartialLength(0);
      setRuntimeStage("queued");
      lastUserRequestRef.current = request;
      addRuntimeLog("info", "AI request queued", {
        requestLength: request.length,
        trigger,
      });
      try {
        await yieldToBrowser();
        if (trigger === "debug-dry-run") {
          addRuntimeLog("info", "debug dry-run started");
          const dryRunStages: GenerationStage[] = [
            "loading-data",
            "preparing-prompt",
            "generating-tool-request",
            "fetching-tool-data",
            "generating-final",
            "streaming-answer",
          ];
          const playDryRunStage = (index: number): Promise<boolean> => {
            const stage = dryRunStages[index];
            if (stage === undefined) {
              return Promise.resolve(true);
            }
            if (abortSignal?.aborted || resetVersionRef.current !== runResetVersion) {
              return Promise.resolve(false);
            }
            setRuntimeStage(stage);
            setGenerationStatus(stage === "loading-data" ? "loading-data" : "generating");
            setModelPartialLength(stage === "generating-final" ? 128 : 0);
            return sleep(450).then(() => playDryRunStage(index + 1));
          };
          const dryRunCompleted = await playDryRunStage(0);
          if (!dryRunCompleted) {
            addRuntimeLog("warn", "debug dry-run stopped before completion");
            return null;
          }
          const now = new Date().toISOString();
          const dryRunAnswer =
            "debug dry-run: AI処理の状態表示とチャットSDK経由の応答表示を確認しました。実モデルは実行していません。";
          const userMessage: RaceAiMessage = {
            content: request,
            createdAt: now,
            id: createId(),
            role: "user",
          };
          const assistantMessage: RaceAiMessage = {
            content: dryRunAnswer,
            createdAt: now,
            id: createId(),
            role: "assistant",
          };
          const thoughtLog: RaceAiThoughtLog = {
            content: "localhost debug dry-run",
            createdAt: now,
            dataFingerprint: "debug-dry-run",
            id: createId(),
            modelVersion: LATEST_RACE_AI_MODEL.version,
            trigger,
          };
          const nextMessages = appendLimited(
            appendLimited(messagesRef.current, userMessage),
            assistantMessage,
          );
          const nextThoughtLogs = appendLimited(thoughtLogsRef.current, thoughtLog);
          setPrediction([]);
          setAnswer(dryRunAnswer);
          await persistLogs(nextMessages, nextThoughtLogs);
          if (emit) {
            await streamDisplayText({
              abortSignal,
              emit,
              text: dryRunAnswer,
            });
          }
          addRuntimeLog("info", "debug dry-run completed");
          return {
            answer: dryRunAnswer,
            format: "text",
            needsTool: false,
            prediction: [],
            thoughtLog: "localhost debug dry-run",
            toolJavaScript: null,
          } satisfies ParsedRaceAiResponse;
        }
        const llm = await ensureModel();
        setRuntimeStage("loading-data");
        setGenerationStatus("loading-data");
        await yieldToBrowser();
        const dataLoadStartedAt = Date.now();
        addRuntimeLog("info", "race AI data load started");
        const data = await loadRaceDataSnapshot();
        addRuntimeLog("info", "race AI data load completed", {
          elapsedMs: Date.now() - dataLoadStartedAt,
          missingPercent: data.aiReady.dataReadiness.missingPercent,
          preparedPercent: data.aiReady.dataReadiness.preparedPercent,
        });
        const dataFingerprint = stableStringify({
          aiReady: data.aiReady.currentOutput,
          dataReadiness: data.aiReady.dataReadiness,
          realtime: data.realtime,
        });
        setGenerationStatus("generating");
        const runWithToolResults = async ({
          formatRetryCount,
          toolResults,
        }: {
          formatRetryCount: number;
          toolResults: ToolResult[];
        }): Promise<ParsedRaceAiResponse> => {
          const promptRequest = responseNeedsFinalAnswer(toolResults, formatRetryCount)
            ? `${request}\n\n直前の応答がJSONではありませんでした。取得済みデータだけを使い、必ず指定されたJSONだけを返してください。`
            : toolResults.length >= MAX_TOOL_CALLS
              ? `${request}\n\n取得済みデータだけで最終回答を作成してください。`
              : request;
          setRuntimeStage("preparing-prompt");
          await yieldToBrowser();
          const promptStartedAt = Date.now();
          addRuntimeLog("debug", "prompt build started", {
            formatRetryCount,
            toolResultCount: toolResults.length,
          });
          const prompt = buildTokenSafePrompt({
            data,
            llm,
            messages: messagesRef.current,
            request: promptRequest,
            thoughtLogs: thoughtLogsRef.current,
            toolResults,
          });
          const promptTokenCount = llm.sizeInTokens(prompt);
          addRuntimeLog("debug", "prompt build completed", {
            elapsedMs: Date.now() - promptStartedAt,
            promptChars: prompt.length,
            promptTokens: promptTokenCount,
          });
          setRuntimeStage(
            toolResults.length === 0 ? "generating-tool-request" : "generating-final",
          );
          await yieldToBrowser();
          const generationStartedAt = Date.now();
          addRuntimeLog("info", "model generation started", {
            mode: toolResults.length === 0 ? "tool-request" : "final-answer",
          });
          const response = parseModelResponse(
            await generateModelResponse({
              abortSignal,
              llm,
              onProgress: (partialResult) => {
                if (!abortSignal?.aborted && resetVersionRef.current === runResetVersion) {
                  setModelPartialLength(partialResult.length);
                }
              },
              prompt,
            }),
          );
          addRuntimeLog("info", "model generation completed", {
            elapsedMs: Date.now() - generationStartedAt,
            format: response.format,
            needsTool: response.needsTool,
            predictionCount: response.prediction.length,
            toolJavaScriptLength: response.toolJavaScript?.length ?? 0,
          });
          if (response.format === "text" && toolResults.length === 0) {
            setRuntimeStage("fetching-tool-data");
            await yieldToBrowser();
            addRuntimeLog("warn", "model returned text before tool request; fetching default data");
            const toolResult = await runToolJavaScript(
              `return await fetchJson("${buildDefaultRaceAiDataUrl(data)}");`,
            );
            addRuntimeLog("info", "default tool data fetched", {
              status: toolResult.status,
              url: toolResult.url,
            });
            return runWithToolResults({ formatRetryCount: 0, toolResults: [toolResult] });
          }
          if (response.format === "text" && formatRetryCount < 1) {
            addRuntimeLog("warn", "model returned text; retrying JSON response", {
              formatRetryCount,
            });
            return runWithToolResults({
              formatRetryCount: formatRetryCount + 1,
              toolResults,
            });
          }
          if (!response.needsTool || !response.toolJavaScript) {
            return response;
          }
          if (toolResults.length >= MAX_TOOL_CALLS) {
            return response;
          }
          setRuntimeStage("fetching-tool-data");
          await yieldToBrowser();
          addRuntimeLog("info", "tool JavaScript fetch started", {
            toolJavaScriptLength: response.toolJavaScript.length,
          });
          const toolResult = await runToolJavaScript(response.toolJavaScript);
          addRuntimeLog("info", "tool JavaScript fetch completed", {
            status: toolResult.status,
            url: toolResult.url,
          });
          return runWithToolResults({
            formatRetryCount: 0,
            toolResults: [...toolResults, toolResult],
          });
        };
        let finalResponse = await runWithToolResults({ formatRetryCount: 0, toolResults: [] });
        if (finalResponse.needsTool) {
          addRuntimeLog("warn", "tool request limit reached; finalizing with fetched data");
          finalResponse = {
            ...finalResponse,
            answer: `${finalResponse.answer}\n\n追加データ取得の上限に達したため、取得済みデータの範囲で回答しました。`,
            needsTool: false,
            toolJavaScript: null,
          };
        }
        if (resetVersionRef.current !== runResetVersion) {
          addRuntimeLog("warn", "AI request ignored because runtime was reset");
          return null;
        }

        const now = new Date().toISOString();
        const userMessage: RaceAiMessage = {
          content: request,
          createdAt: now,
          id: createId(),
          role: "user",
        };
        const assistantMessage: RaceAiMessage = {
          content: finalResponse.answer,
          createdAt: now,
          id: createId(),
          role: "assistant",
        };
        const thoughtLog: RaceAiThoughtLog = {
          content: finalResponse.thoughtLog || finalResponse.answer,
          createdAt: now,
          dataFingerprint,
          id: createId(),
          modelVersion: LATEST_RACE_AI_MODEL.version,
          trigger,
        };
        const nextMessages = appendLimited(
          appendLimited(messagesRef.current, userMessage),
          assistantMessage,
        );
        const nextThoughtLogs = appendLimited(thoughtLogsRef.current, thoughtLog);
        setPrediction(finalResponse.prediction);
        setAnswer(finalResponse.answer);
        await persistLogs(nextMessages, nextThoughtLogs);
        addRuntimeLog("info", "AI response persisted", {
          answerLength: finalResponse.answer.length,
          predictionCount: finalResponse.prediction.length,
          thoughtLogLength: thoughtLog.content.length,
        });
        if (emit) {
          setRuntimeStage("streaming-answer");
          await yieldToBrowser();
          await streamDisplayText({
            abortSignal,
            emit,
            text: finalResponse.answer,
          });
        }
        addRuntimeLog("info", "AI request completed");
        return finalResponse;
      } catch (caught) {
        if (resetVersionRef.current === runResetVersion) {
          setError(caught instanceof Error ? caught.message : String(caught));
        }
        addRuntimeLog("error", "AI request failed", {
          error: caught instanceof Error ? caught.message : String(caught),
          trigger,
        });
        if (emit) {
          throw caught;
        }
        return null;
      } finally {
        runningRef.current = false;
        setGenerationStatus("idle");
        setRuntimeStage("idle");
      }
    },
    [addRuntimeLog, ensureModel, loadRaceDataSnapshot, persistLogs, setRuntimeStage],
  );

  const localChatTransport = useMemo<ChatTransport<UIMessage>>(
    () => ({
      reconnectToStream: async () => null,
      sendMessages: async ({ abortSignal, body, messages: uiMessages }) =>
        createLocalChatStream({
          abortSignal,
          execute: async (emit) => {
            const request =
              uiMessageText(uiMessages.at(-1)) || "このレースの着順を予想してください。";
            const trigger =
              isRecord(body) && typeof body.trigger === "string" ? body.trigger : "chat";
            await runAi(request, trigger, emit, abortSignal);
          },
        }),
    }),
    [runAi],
  );

  const {
    error: chatError,
    messages: chatMessages,
    sendMessage,
    setMessages: setChatMessages,
    status: chatStatus,
    stop: stopChat,
  } = useChat({
    id: chatId,
    onError: (caught) => {
      if (Date.now() < suppressChatErrorUntilRef.current) {
        return;
      }
      setError(caught.message);
    },
    transport: localChatTransport,
  });

  useEffect(() => {
    if (modelStatus !== "initializing") {
      return undefined;
    }
    if (generationStage !== "model-initialize" || generationStageStartedAt === null) {
      addRuntimeLog("warn", "orphaned AI model initialization state cleared", {
        generationStage,
        modelStatus,
      });
      setModelStatus("idle");
      return undefined;
    }

    const initializationStartedAt = generationStageStartedAt;
    const timeoutMs = getModelInitializationTimeoutMs();
    const timeoutMessage = modelInitializationTimeoutMessage(timeoutMs);
    const stopInitialization = () => {
      const elapsedMs = Date.now() - initializationStartedAt;
      if (elapsedMs < timeoutMs) {
        return;
      }
      if (modelInitializationTimeoutHandledAtRef.current === initializationStartedAt) {
        return;
      }
      modelInitializationTimeoutHandledAtRef.current = initializationStartedAt;
      sharedRaceAiModelInitializationGeneration += 1;
      sharedRaceAiModelPromise = null;
      runningRef.current = false;
      suppressChatErrorUntilRef.current = Date.now() + 1_500;
      void stopChat();
      if (llmRef.current) {
        callOptionalLlmMethod(llmRef.current, "cancelProcessing");
      }
      setModelStatus("idle");
      setGenerationStatus("idle");
      setRuntimeStage("idle");
      setModelPartialLength(0);
      setError(timeoutMessage);
      addRuntimeLog("error", "AI model initialization watchdog timeout", {
        elapsedMs,
        timeoutMs,
      });
    };

    stopInitialization();
    const timer = window.setInterval(stopInitialization, MODEL_INITIALIZATION_WATCHDOG_INTERVAL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [
    addRuntimeLog,
    generationStage,
    generationStageStartedAt,
    modelStatus,
    setRuntimeStage,
    stopChat,
  ]);

  const clearRaceAiState = useCallback(() => {
    resetRuntimeLogs("race-ai-state-reset", {
      previousMessageCount: messagesRef.current.length,
      previousThoughtLogCount: thoughtLogsRef.current.length,
      realtimeFingerprint,
    });
    resetVersionRef.current += 1;
    suppressChatErrorUntilRef.current = Date.now() + 1_500;
    void stopChat();
    if (llmRef.current) {
      callOptionalLlmMethod(llmRef.current, "cancelProcessing");
    }
    const nextMessages: RaceAiMessage[] = [];
    const nextThoughtLogs: RaceAiThoughtLog[] = [];
    runningRef.current = false;
    messagesRef.current = nextMessages;
    thoughtLogsRef.current = nextThoughtLogs;
    setMessages(nextMessages);
    setThoughtLogs(nextThoughtLogs);
    setChatMessages([]);
    setChatSessionVersion((current) => current + 1);
    setPrediction([]);
    setAnswer("");
    setError(null);
    setModelPartialLength(0);
    setGenerationStatus("idle");
    setRuntimeStage("idle");
    autoStartedRaceKeyRef.current = raceKey;
    lastDebugSnapshotRef.current = "";
    lastRealtimeFingerprintRef.current = realtimeFingerprint;
    lastUserRequestRef.current = "このレースの着順を予想してください。";
    void deleteRaceAiLog(raceKey).catch(() =>
      saveRaceAiLog({
        messages: nextMessages,
        raceKey,
        thoughtLogs: nextThoughtLogs,
        updatedAt: new Date().toISOString(),
      }).catch(() => {}),
    );
  }, [raceKey, realtimeFingerprint, resetRuntimeLogs, setChatMessages, setRuntimeStage, stopChat]);

  const resetRaceAiState = useCallback(() => {
    if (!window.confirm("このレースのAI予想、対話ログ、思考ログをリセットしますか？")) {
      return;
    }
    clearRaceAiState();
  }, [clearRaceAiState]);

  useEffect(() => {
    if (hydratedRaceKeyRef.current !== raceKey) {
      return;
    }
    if (chatStatus === "submitted" || chatStatus === "streaming") {
      return;
    }
    setChatMessages(messages.map(raceMessageToUiMessage));
  }, [chatStatus, messages, raceKey, setChatMessages]);

  useEffect(() => {
    if (!realtimeFingerprint) {
      return undefined;
    }
    if (!lastRealtimeFingerprintRef.current) {
      lastRealtimeFingerprintRef.current = realtimeFingerprint;
      return undefined;
    }
    if (
      lastRealtimeFingerprintRef.current === realtimeFingerprint ||
      !llmRef.current ||
      generationStatus !== "idle"
    ) {
      lastRealtimeFingerprintRef.current = realtimeFingerprint;
      return undefined;
    }
    const timer = window.setTimeout(() => {
      void sendMessage(
        {
          text: `${lastUserRequestRef.current}\n\nリアルタイムデータが更新されました。最新データで再評価してください。`,
        },
        {
          body: {
            trigger: "realtime-update",
          },
        },
      );
    }, REALTIME_RETHINK_DELAY_MS);
    lastRealtimeFingerprintRef.current = realtimeFingerprint;
    return () => {
      window.clearTimeout(timer);
    };
  }, [generationStatus, realtimeFingerprint, sendMessage]);

  useEffect(() => {
    if (
      supportState !== "supported" ||
      settings?.consent !== "granted" ||
      !settings.autoStart ||
      modelState?.status !== "downloaded" ||
      generationStatus !== "idle" ||
      hydratedRaceKeyRef.current !== raceKey ||
      autoStartedRaceKeyRef.current === raceKey
    ) {
      return;
    }
    autoStartedRaceKeyRef.current = raceKey;
    void sendMessage(
      { text: "このレースの着順を予想してください。" },
      {
        body: {
          trigger: "auto-start",
        },
      },
    );
  }, [generationStatus, modelState?.status, raceKey, sendMessage, settings, supportState]);

  const progressLabel =
    modelState?.progress === null || modelState?.progress === undefined
      ? "取得中"
      : `${Math.round(modelState.progress * 100)}%`;
  const isModelDownloaded = modelState?.status === "downloaded";
  const isModelDownloading = modelState?.status === "downloading";
  const isBusy =
    isModelDownloading ||
    modelStatus === "downloading" ||
    modelStatus === "initializing" ||
    generationStatus !== "idle" ||
    chatStatus === "submitted" ||
    chatStatus === "streaming";
  const modelStatusLabel =
    modelStatus === "ready"
      ? "読み込み済み"
      : modelStatus === "downloading"
        ? `ダウンロード ${progressLabel}`
        : modelStatus === "initializing"
          ? "初期化中"
          : isModelDownloaded
            ? "ダウンロード済み"
            : isModelDownloading
              ? `ダウンロード ${progressLabel}`
              : "未ダウンロード";
  const dataAvailabilityLabel =
    dataReadinessStatus === "loading"
      ? "データ構造を確認中"
      : dataReadiness
        ? `参照可能 ${dataReadiness.preparedPercent.toFixed(1)}% / 未取得または未準備 ${dataReadiness.missingPercent.toFixed(1)}%`
        : "AIが必要時にデータを取得します";
  const generationStageElapsedSeconds =
    generationStageStartedAt === null
      ? 0
      : Math.max(0, Math.floor((runtimeNow - generationStageStartedAt) / 1000));
  const generationStageElapsedLabel = formatGenerationStageElapsedLabel(
    generationStage,
    generationStageElapsedSeconds,
  );
  const generationStageLabel =
    generationStage === "generating-final" && modelPartialLength > 0
      ? `${GENERATION_STAGE_LABELS[generationStage]} / モデル応答 ${modelPartialLength.toLocaleString("ja-JP")} 文字受信`
      : GENERATION_STAGE_LABELS[generationStage];
  const runtimeStageLabel =
    generationStage === "idle"
      ? GENERATION_STAGE_LABELS.idle
      : `${generationStageLabel}（${generationStageElapsedLabel}）`;
  const dataStatusLabel =
    generationStage !== "idle"
      ? runtimeStageLabel
      : generationStatus === "loading-data"
        ? "取得中"
        : generationStatus === "generating"
          ? "予想中"
          : dataAvailabilityLabel;
  const chatRuntimeLabel =
    chatStatus === "submitted"
      ? runtimeStageLabel
      : chatStatus === "streaming"
        ? generationStage === "idle"
          ? "応答を生成中"
          : runtimeStageLabel
        : dataAvailabilityLabel;
  const readinessPreparedPercent = dataReadiness?.preparedPercent ?? 0;
  const missingDataLabel =
    dataReadiness && dataReadiness.missingPercent > 0
      ? "未準備のデータが必要な場合は、AIが追加取得を試み、取得できない場合は不足理由を回答します。"
      : "必要なデータはAIが必要に応じて取得します。";
  const hasRaceAiState =
    answer ||
    prediction.length > 0 ||
    messages.length > 0 ||
    thoughtLogs.length > 0 ||
    chatMessages.length > 0;
  const debugUrl = `/api/debug/ai-chat?raceKey=${encodeURIComponent(raceKey)}`;
  const serverLogUrl = `/api/races/${props.year}/${props.month}/${props.day}/${props.keibajoCode}/${props.raceNumber}/ai/logs?source=${encodeURIComponent(props.source)}`;
  const debugStatus = chatStatus === "ready" ? generationStatus : chatStatus;

  useEffect(() => {
    const visibleAiState = isBusy ? chatRuntimeLabel : "待機中";
    const fingerprint = stableStringify({
      chatRuntimeLabel,
      chatStatus,
      dataStatusLabel,
      generationStage,
      generationStageElapsedSeconds,
      generationStatus,
      isBusy,
      modelPartialLength,
      modelStatus,
      modelStatusLabel,
      supportState,
      visibleAiState,
    });
    if (lastRuntimeUiSnapshotLogRef.current === fingerprint) {
      return;
    }
    lastRuntimeUiSnapshotLogRef.current = fingerprint;
    addRuntimeLog("debug", "UI runtime display committed", {
      chatRuntimeLabel,
      chatStatus,
      dataStatusLabel,
      generationStage,
      generationStageElapsedSeconds,
      generationStatus,
      isBusy,
      modelPartialLength,
      modelStatus,
      modelStatusLabel,
      supportState,
      visibleAiState,
    });
  }, [
    addRuntimeLog,
    chatRuntimeLabel,
    chatStatus,
    dataStatusLabel,
    generationStage,
    generationStageElapsedSeconds,
    generationStatus,
    isBusy,
    modelPartialLength,
    modelStatus,
    modelStatusLabel,
    supportState,
  ]);

  useEffect(() => {
    const handleServerCommand = async (command: unknown) => {
      if (!isRecord(command) || command.action !== "reset" || typeof command.id !== "string") {
        return;
      }
      if (handledServerCommandIdsRef.current.has(command.id)) {
        return;
      }
      handledServerCommandIdsRef.current.add(command.id);
      clearRaceAiState();
      await fetch(serverLogUrl, {
        body: JSON.stringify({ ackCommandId: command.id }),
        headers: { "content-type": "application/json" },
        method: "POST",
      }).catch(() => {});
    };
    const pollCommand = () => {
      void (async () => {
        const response = await fetch(serverLogUrl, { cache: "no-store" });
        if (!response.ok) {
          return;
        }
        const payload: unknown = await response.json();
        await handleServerCommand(isRecord(payload) ? payload.command : null);
      })().catch(() => {});
    };
    pollCommand();
    const timer = window.setInterval(pollCommand, SERVER_COMMAND_SYNC_INTERVAL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [clearRaceAiState, serverLogUrl]);

  useEffect(() => {
    if (!debugEnabled) {
      return;
    }
    const chatRaceMessages = chatMessages
      .map(uiMessageToRaceMessage)
      .filter((message): message is RaceAiMessage => message !== null);
    const snapshot = {
      answer,
      chatMessages: chatRaceMessages,
      dataStatus: {
        availableSummary: dataAvailabilityLabel,
        dataReadiness,
        modelStatus: modelStatusLabel,
        runtimeStatus: dataStatusLabel,
      },
      error: error ?? chatError?.message ?? null,
      messages,
      prediction,
      raceKey,
      route: {
        day: props.day,
        keibajoCode: props.keibajoCode,
        month: props.month,
        raceNumber: props.raceNumber,
        source: props.source,
        year: props.year,
      },
      runtime: {
        chatStatus,
        generationStage,
        generationStageElapsedSeconds,
        generationStageLabel,
        generationStatus,
        isBusy,
        isRunning: runningRef.current,
        lastUserRequest: lastUserRequestRef.current,
        modelPartialLength,
        runtimeLogs,
        runtimeStageLabel,
      },
      status: debugStatus,
      thoughtLogs,
    };
    const fingerprint = stableStringify(snapshot);
    if (lastDebugSnapshotRef.current === fingerprint) {
      return;
    }
    lastDebugSnapshotRef.current = fingerprint;
    void fetch("/api/debug/ai-chat", {
      body: JSON.stringify(snapshot),
      headers: { "content-type": "application/json" },
      method: "PUT",
    }).catch(() => {});
  }, [
    answer,
    chatError,
    chatMessages,
    chatStatus,
    dataAvailabilityLabel,
    dataReadiness,
    dataStatusLabel,
    debugEnabled,
    debugStatus,
    error,
    generationStage,
    generationStageElapsedSeconds,
    generationStageLabel,
    generationStatus,
    isBusy,
    messages,
    modelStatusLabel,
    modelPartialLength,
    prediction,
    props.day,
    props.keibajoCode,
    props.month,
    props.raceNumber,
    props.source,
    props.year,
    raceKey,
    runtimeStageLabel,
    runtimeLogs,
    thoughtLogs,
  ]);

  useEffect(() => {
    if (!debugEnabled) {
      return undefined;
    }
    const handleCommand = async (command: RaceAiDebugCommand) => {
      if (handledDebugCommandIdsRef.current.has(command.id)) {
        return;
      }
      handledDebugCommandIdsRef.current.add(command.id);
      if (command.type === "send-message" && command.text?.trim()) {
        await sendMessage(
          { text: command.text.trim() },
          {
            body: {
              trigger: command.dryRun ? "debug-dry-run" : "debug",
            },
          },
        );
      } else if (command.type === "simulate-model-initialize") {
        setError(null);
        runningRef.current = true;
        setGenerationStatus("generating");
        setModelStatus("initializing");
        setRuntimeStage("model-initialize");
        addRuntimeLog("warn", "debug simulated AI model initialization stall");
      } else if (command.type === "reset") {
        clearRaceAiState();
      } else if (command.type === "replace-messages" && command.messages) {
        await persistLogs(command.messages, thoughtLogsRef.current);
      }
      await fetch("/api/debug/ai-chat", {
        body: JSON.stringify({ ackCommandId: command.id, raceKey }),
        headers: { "content-type": "application/json" },
        method: "PUT",
      }).catch(() => {});
    };
    const pollCommand = () => {
      void (async () => {
        const response = await fetch(debugUrl, { cache: "no-store" });
        if (!response.ok) {
          return;
        }
        const payload: unknown = await response.json();
        const command = isRecord(payload) ? parseDebugCommand(payload.command) : null;
        if (command) {
          await handleCommand(command);
        }
      })().catch(() => {});
    };
    pollCommand();
    const timer = window.setInterval(pollCommand, DEBUG_SYNC_INTERVAL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [
    addRuntimeLog,
    clearRaceAiState,
    debugEnabled,
    debugUrl,
    persistLogs,
    raceKey,
    sendMessage,
    setRuntimeStage,
  ]);

  if (supportState !== "supported" || settings?.consent !== "granted") {
    return null;
  }

  return (
    <details
      className="race-ai-assistant-section"
      open={aiSectionOpen}
      onToggle={(event) => {
        setAiSectionOpen(event.currentTarget.open);
      }}
    >
      <summary className="section-heading compact race-ai-summary">
        <h2>WebGPU AI予想</h2>
        <span>
          {LATEST_RACE_AI_MODEL.name} / {LATEST_RACE_AI_MODEL.version} /{" "}
          {aiSectionOpen ? "閉じる" : "表示する"}
        </span>
      </summary>
      <div className="race-ai-readiness-panel">
        <div className="race-ai-readiness-summary">
          <span className="race-ai-readiness-summary-title">
            <strong>AIデータ状態</strong>
            <small>必要時取得</small>
          </span>
          <span className="race-ai-readiness-overall">
            <progress value={readinessPreparedPercent} max={100} />
            <span>現在参照可能 {readinessPreparedPercent.toFixed(1)}%</span>
          </span>
        </div>
        <div className="race-ai-readiness-status-grid">
          <div>
            <span>モデル</span>
            <strong>{modelStatusLabel}</strong>
          </div>
          <div>
            <span>データ参照</span>
            <strong>{dataStatusLabel}</strong>
          </div>
        </div>
        <p className="race-ai-readiness-note">{missingDataLabel}</p>
      </div>
      <div className="race-ai-runtime-panel" aria-live="polite">
        <span>AI状態</span>
        <strong>{isBusy ? chatRuntimeLabel : "待機中"}</strong>
        {isBusy ? <small>送信後の処理はこの表示で追跡できます。</small> : null}
      </div>
      <details className="race-ai-diagnostic-log" open={isBusy || Boolean(error || chatError)}>
        <summary>
          <strong>診断ログ</strong>
          <span>{runtimeLogs.length}件</span>
        </summary>
        {runtimeLogs.length === 0 ? (
          <p>診断ログはまだありません。</p>
        ) : (
          <ol>
            {runtimeLogs.map((log) => (
              <li className={`race-ai-diagnostic-log-${log.level}`} key={log.id}>
                <time dateTime={log.at}>{log.at.slice(11, 19)}</time>
                <strong>{log.level}</strong>
                <span>{log.message}</span>
                {log.details ? <code>{formatRuntimeLogDetails(log.details)}</code> : null}
              </li>
            ))}
          </ol>
        )}
      </details>
      {error || chatError ? <p className="race-ai-error">{error ?? chatError?.message}</p> : null}
      {prediction.length > 0 ? (
        <div className="race-ai-table-wrap">
          <table className="race-ai-prediction-table">
            <thead>
              <tr>
                <th>予想</th>
                <th>馬番</th>
                <th>馬名</th>
                <th>騎手</th>
                <th>信頼度</th>
                <th>根拠</th>
              </tr>
            </thead>
            <tbody>
              {prediction.map((row) => (
                <tr key={`${row.rank}-${row.horseNumber}`}>
                  <td>{row.rank}</td>
                  <td>{row.horseNumber}</td>
                  <td>{row.horseName}</td>
                  <td>{row.jockeyName}</td>
                  <td>{row.confidence === null ? "-" : row.confidence.toFixed(2)}</td>
                  <td>{row.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
      <div className="race-ai-chat-thread" aria-live="polite">
        {chatMessages.length === 0 ? (
          <p className="empty-state">AIとのやりとりはまだありません。</p>
        ) : (
          chatMessages.map((message) => (
            <article className={`race-ai-chat-message ${message.role}`} key={message.id}>
              <span>{message.role === "user" ? "あなた" : "AI"}</span>
              <div>
                {answerBlocks(uiMessageText(message)).map((block) => (
                  <p key={block}>{block}</p>
                ))}
              </div>
            </article>
          ))
        )}
        {chatStatus === "submitted" ? (
          <p className="race-ai-stream-status">{chatRuntimeLabel}</p>
        ) : null}
        {chatStatus === "streaming" ? (
          <p className="race-ai-stream-status">{chatRuntimeLabel}</p>
        ) : null}
      </div>
      {answer && chatMessages.length === 0 ? (
        <div className="race-ai-answer">
          {answerBlocks(answer).map((block) => (
            <p key={block}>{block}</p>
          ))}
        </div>
      ) : null}
      <form
        className="race-ai-chat-form"
        onSubmit={(event) => {
          event.preventDefault();
          const value = input.trim();
          if (!value || isBusy) {
            return;
          }
          setInput("");
          void sendMessage({ text: value });
        }}
      >
        <label>
          AIに質問
          <textarea
            value={input}
            rows={3}
            onChange={(event) => {
              setInput(event.currentTarget.value);
            }}
          />
        </label>
        <button type="submit" disabled={isBusy || !input.trim()}>
          {isBusy ? "送信中" : "送信"}
        </button>
      </form>
      {isBusy ? (
        <div className="race-ai-stop-actions">
          <button type="button" onClick={stopChat}>
            応答を停止
          </button>
        </div>
      ) : null}
      {debugEnabled ? (
        <p className="race-ai-debug-note">
          localhost debug: <code>{debugUrl}</code>
        </p>
      ) : null}
      <div className="race-ai-reset-actions">
        <button type="button" disabled={!hasRaceAiState} onClick={resetRaceAiState}>
          このレースのAIログをリセット
        </button>
      </div>
    </details>
  );
}
