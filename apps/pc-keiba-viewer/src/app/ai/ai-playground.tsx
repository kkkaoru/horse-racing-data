"use client";

import { useChat } from "@ai-sdk/react";
import type { LlmInference } from "@mediapipe/tasks-genai";
import type { ChatTransport, UIMessage, UIMessageChunk } from "ai";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { buildGemmaPrompt } from "../races/detail/race-ai-default-prompt";
import {
  ensureRaceAiModelBuffer,
  formatRaceAiModelSize,
  getRaceAiModelState,
  LATEST_RACE_AI_MODEL,
  subscribeRaceAiModelDownloads,
  type RaceAiModelState,
} from "../races/detail/race-ai-model-manager";
import {
  getRaceAiSettings,
  RACE_AI_MODEL_DOWNLOAD_CONFIRM_MESSAGE,
  subscribeRaceAiSettings,
  type RaceAiSettings,
} from "../races/detail/race-ai-storage";

type AiPlaygroundModelStatus = "error" | "idle" | "loading" | "ready";
type WebGpuSupportState = "checking" | "supported" | "unsupported";
type DebugLogLevel = "debug" | "error" | "info" | "warn";
type DebugPrimitive = boolean | null | number | string;

interface AiPlaygroundDebugLog {
  at: string;
  details: Record<string, DebugPrimitive> | null;
  event: string;
  level: DebugLogLevel;
  sequence: number;
}

interface AiPlaygroundClientSnapshot {
  chatStatus: string;
  error: string | null;
  lastAssistantTextLength: number;
  lastUserTextLength: number;
  mockMode: boolean;
  modelCacheStatus: string;
  modelPartialLength: number;
  modelStatus: string;
  statusMessage: string;
  supportState: string;
}

interface ServerDebugAck {
  logCount: number;
  serverNow: string;
  serverSequence: number;
}

let sharedAiPlaygroundModel: LlmInference | null = null;
let sharedAiPlaygroundModelPromise: Promise<LlmInference> | null = null;

const WASM_BASE_URL = "https://cdn.jsdelivr.net/npm/@mediapipe/tasks-genai@0.10.27/wasm";
const LOCAL_MOCK_SEARCH_PARAM = "mock";
const DEBUG_LOG_LIMIT = 300;
const DEBUG_HEARTBEAT_INTERVAL_MS = 3_000;
const DEBUG_FLUSH_DEBOUNCE_MS = 250;
const MODEL_PROGRESS_LOG_STEP = 256;
const MODEL_HEAD_TIMEOUT_MS = 10_000;
const DEBUG_TEXT_LIMIT = 2_000;
const DEBUG_SAMPLE_TEXT_LIMIT = 240;

const createId = (): string => `${Date.now().toString(36)}-${crypto.randomUUID()}`;

const createSafeId = (): string => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return createId();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
};

const formatCaughtError = (caught: unknown): string =>
  caught instanceof Error ? caught.message : String(caught);

const truncateDebugText = (text: string, limit = DEBUG_TEXT_LIMIT): string =>
  text.length > limit ? `${text.slice(0, limit)}...` : text;

const truncateDebugSample = (text: string): string =>
  truncateDebugText(text, DEBUG_SAMPLE_TEXT_LIMIT);

const normalizeDebugDetails = (
  details?: Record<string, unknown>,
): Record<string, DebugPrimitive> | null => {
  if (!details) {
    return null;
  }
  const entries: Array<[string, DebugPrimitive]> = [];
  for (const [key, value] of Object.entries(details)) {
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean" ||
      value === null
    ) {
      entries.push([key, value]);
    } else if (value instanceof Error) {
      entries.push([key, value.message]);
    } else if (value !== undefined) {
      entries.push([key, stringifyDebugValue(value)]);
    }
  }
  return entries.length > 0 ? Object.fromEntries(entries) : null;
};

const stringifyDebugValue = (value: unknown): string => {
  if (value instanceof Error) {
    return value.message;
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const formatDebugLogDetails = (details: Record<string, DebugPrimitive> | null): string | null => {
  if (!details) {
    return null;
  }
  return JSON.stringify(details);
};

const toConsoleArgumentText = (value: unknown): string => {
  if (value instanceof Error) {
    return value.stack ?? value.message;
  }
  if (typeof value === "string") {
    return value;
  }
  return stringifyDebugValue(value);
};

const copyTextToClipboard = async (text: string): Promise<void> => {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.left = "-9999px";
  textarea.style.position = "fixed";
  textarea.style.top = "0";
  document.body.append(textarea);
  textarea.select();
  try {
    if (!document.execCommand("copy")) {
      throw new Error("clipboard copy command failed");
    }
  } finally {
    textarea.remove();
  }
};

const uiMessageText = (message: UIMessage | undefined): string =>
  (message?.parts ?? [])
    .map((part) => (part.type === "text" ? part.text : ""))
    .join("")
    .trim();

const MODEL_CONTROL_TOKEN_PATTERN = /<\/?(?:start_of_turn|end_of_turn|bos|eos)>/gu;
const MODEL_CONTROL_TOKEN_FRAGMENT_PATTERN = /<\/?(?:start|end)_[a-z_]*>?/gu;
const MODEL_CONTROL_TOKEN_FRAGMENT_START_PATTERN = /^<\/?(?:start|end)_[a-z_]*>?/u;
const MODEL_PROTOCOL_ROLE_HEADER_LINE_PATTERN =
  /(?:^|\n)[ \t\r]*(?:user|model|assistant)[ \t\r]*(?::)?(?=\n|$)/giu;
const MODEL_CONTROL_TOKENS = [
  "<start_of_turn>",
  "</start_of_turn>",
  "<end_of_turn>",
  "</end_of_turn>",
  "<bos>",
  "</bos>",
  "<eos>",
  "</eos>",
] as const;
const MODEL_PROTOCOL_ROLES = ["user", "model", "assistant"] as const;
const EMPTY_ASSISTANT_FALLBACK =
  "AIから表示できる本文が返りませんでした。質問を少し具体的にして、もう一度送信してください。";

const cleanModelText = (text: string): string =>
  text
    .replace(MODEL_CONTROL_TOKEN_PATTERN, "")
    .replace(MODEL_CONTROL_TOKEN_FRAGMENT_PATTERN, "")
    .replace(MODEL_PROTOCOL_ROLE_HEADER_LINE_PATTERN, "\n")
    .replace(/^```(?:json|markdown|text)?\s*/u, "")
    .replace(/\s*```$/u, "")
    .trim();

const isModelProtocolRole = (
  value: string | undefined,
): value is (typeof MODEL_PROTOCOL_ROLES)[number] =>
  MODEL_PROTOCOL_ROLES.some((role) => role === value);

const matchProtocolRoleHeader = (
  text: string,
  flush: boolean,
): { length: number; role: (typeof MODEL_PROTOCOL_ROLES)[number] } | null => {
  const match = /^[ \t\r]*(user|model|assistant)[ \t\r]*(?::)?(\n|$)/iu.exec(text);
  if (!match) {
    return null;
  }
  if (!flush && match[2] !== "\n") {
    return null;
  }
  const role = match[1]?.toLowerCase();
  if (!isModelProtocolRole(role)) {
    return null;
  }
  return { length: match[0].length, role };
};

const isProtocolRoleHeaderPrefix = (text: string): boolean => {
  const candidate = text.replace(/^[ \t\r]*/u, "").toLowerCase();
  if (!candidate) {
    return true;
  }
  return MODEL_PROTOCOL_ROLES.some(
    (role) =>
      role.startsWith(candidate) ||
      candidate === role ||
      candidate.startsWith(`${role} `) ||
      candidate.startsWith(`${role}:`),
  );
};

const createModelStreamCleaner = (): {
  flush: () => string;
  push: (chunk: string) => string;
} => {
  let buffer = "";
  let protocolBuffer = "";
  let protocolLineStart = true;
  let suppressUserTurn = false;
  const filterProtocolText = (text: string, flush: boolean): string => {
    protocolBuffer += text;
    let output = "";
    while (protocolBuffer) {
      if (protocolLineStart) {
        const roleHeader = matchProtocolRoleHeader(protocolBuffer, flush);
        if (roleHeader) {
          protocolBuffer = protocolBuffer.slice(roleHeader.length);
          suppressUserTurn = roleHeader.role === "user";
          protocolLineStart = true;
          continue;
        }
        if (!flush && isProtocolRoleHeaderPrefix(protocolBuffer)) {
          break;
        }
      }
      const nextChar = protocolBuffer[0];
      protocolBuffer = protocolBuffer.slice(1);
      if (!suppressUserTurn) {
        output += nextChar;
      }
      protocolLineStart = nextChar === "\n";
    }
    return output;
  };
  const drain = (flush: boolean): string => {
    let output = "";
    while (buffer) {
      const tagStart = buffer.indexOf("<");
      if (tagStart < 0) {
        output += buffer;
        buffer = "";
        break;
      }
      if (tagStart > 0) {
        output += buffer.slice(0, tagStart);
        buffer = buffer.slice(tagStart);
        continue;
      }
      const token = MODEL_CONTROL_TOKENS.find((candidate) => buffer.startsWith(candidate));
      if (token) {
        buffer = buffer.slice(token.length);
        continue;
      }
      if (!flush && MODEL_CONTROL_TOKENS.some((candidate) => candidate.startsWith(buffer))) {
        break;
      }
      const fragmentMatch = MODEL_CONTROL_TOKEN_FRAGMENT_START_PATTERN.exec(buffer);
      if (fragmentMatch) {
        buffer = buffer.slice(fragmentMatch[0].length);
        continue;
      }
      output += buffer[0];
      buffer = buffer.slice(1);
    }
    const cleaned = output
      .replace(MODEL_CONTROL_TOKEN_PATTERN, "")
      .replace(MODEL_CONTROL_TOKEN_FRAGMENT_PATTERN, "");
    return filterProtocolText(cleaned, flush);
  };
  return {
    flush: () => drain(true),
    push: (chunk: string) => {
      buffer += chunk;
      return drain(false);
    },
  };
};

const assistantDisplayText = (message: UIMessage): string => {
  const text = uiMessageText(message);
  if (message.role !== "assistant") {
    return text;
  }
  return cleanModelText(text) || EMPTY_ASSISTANT_FALLBACK;
};

const isLocalhostBrowser = (): boolean =>
  typeof window !== "undefined" &&
  ["localhost", "127.0.0.1", "0.0.0.0", "::1"].includes(window.location.hostname);

const isDebugDisplayAllowed = (): boolean => {
  if (typeof window === "undefined") {
    return false;
  }
  const normalizedPathname = window.location.pathname.replace(/\/$/u, "");
  return window.location.origin === "https://192.168.1.219" && normalizedPathname === "/ai";
};

const isMockMode = (): boolean =>
  isLocalhostBrowser() &&
  new URLSearchParams(window.location.search).get(LOCAL_MOCK_SEARCH_PARAM) === "1";

const statusLabel = (status: RaceAiModelState["status"] | undefined): string => {
  if (status === "downloaded") {
    return "ダウンロード済み";
  }
  if (status === "downloading") {
    return "ダウンロード中";
  }
  return "未ダウンロード";
};

const progressLabel = (state: RaceAiModelState | null): string => {
  if (!state) {
    return "確認中";
  }
  if (state.status === "downloaded") {
    return "100%";
  }
  if (state.status !== "downloading") {
    return "-";
  }
  return state.progress === null ? "取得中" : `${Math.round(state.progress * 100)}%`;
};

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
        controller.enqueue({ id: textId, type: "text-end" });
        controller.enqueue({ type: "finish-step" });
        controller.enqueue({ finishReason: "error", type: "finish" });
      } finally {
        controller.close();
      }
    },
  });

const promptRoleLabel = (role: UIMessage["role"]): string => {
  if (role === "user") {
    return "ユーザー";
  }
  if (role === "assistant") {
    return "AI";
  }
  return role;
};

const buildPrompt = (messages: UIMessage[], request: string): string => {
  const currentRequest = request.trim();
  const priorMessages =
    messages.at(-1)?.role === "user" && uiMessageText(messages.at(-1)).trim() === currentRequest
      ? messages.slice(0, -1)
      : messages;
  const recentConversation =
    priorMessages
      .slice(-6)
      .map((message) => ({
        role: promptRoleLabel(message.role),
        text:
          message.role === "assistant"
            ? cleanModelText(uiMessageText(message))
            : uiMessageText(message),
      }))
      .filter((message) => message.text.length > 0)
      .map((message) => `${message.role}: ${message.text}`)
      .join("\n") || "なし";
  return buildGemmaPrompt(
    [
      "あなたはPC-KEIBA Viewerの動作確認用AIです。",
      "次のユーザー入力に対する回答本文だけを日本語で出力してください。",
      "ユーザー入力を引用、復唱、翻訳しないでください。",
      "制御トークン、XML風タグ、Markdownコードフェンス、role名（user/model/assistant）は出力しないでください。",
      "このページは外部情報を取得しません。天気や最新情報など外部情報が必要な質問では、取得できないことを短く伝えてください。",
      "競馬データの取得や予想はこのページでは行わず、モデル応答の疎通確認として回答してください。",
      "過去の会話（参考）:",
      recentConversation,
      "ユーザー入力:",
      currentRequest || request,
      "回答:",
    ].join("\n\n"),
  );
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

const fetchModelHead = async (): Promise<{
  contentLength: string | null;
  contentType: string | null;
  ok: boolean;
  status: number;
}> => {
  const controller = new AbortController();
  const timer = window.setTimeout(() => {
    controller.abort();
  }, MODEL_HEAD_TIMEOUT_MS);
  try {
    const response = await fetch(LATEST_RACE_AI_MODEL.url, {
      cache: "no-store",
      method: "HEAD",
      signal: controller.signal,
    });
    return {
      contentLength: response.headers.get("content-length"),
      contentType: response.headers.get("content-type"),
      ok: response.ok,
      status: response.status,
    };
  } finally {
    window.clearTimeout(timer);
  }
};

export function AiPlayground() {
  const [supportState, setSupportState] = useState<WebGpuSupportState>("checking");
  const [modelState, setModelState] = useState<RaceAiModelState | null>(null);
  const [modelStatus, setModelStatus] = useState<AiPlaygroundModelStatus>("idle");
  const [statusMessage, setStatusMessage] = useState("AIの状態を確認しています。");
  const [input, setInput] = useState("こんにちは。短く自己紹介してください。");
  const [error, setError] = useState<string | null>(null);
  const [mockMode, setMockMode] = useState(false);
  const [modelPartialLength, setModelPartialLength] = useState(0);
  const [sessionId, setSessionId] = useState("");
  const [debugEnabled, setDebugEnabled] = useState(false);
  const [debugLogs, setDebugLogs] = useState<AiPlaygroundDebugLog[]>([]);
  const [serverDebugAck, setServerDebugAck] = useState<ServerDebugAck | null>(null);
  const [debugFlushError, setDebugFlushError] = useState<string | null>(null);
  const [settings, setSettings] = useState<RaceAiSettings | null>(null);
  const [copyLogStatus, setCopyLogStatus] = useState<string | null>(null);
  const llmRef = useRef<LlmInference | null>(null);
  const sessionIdRef = useRef("");
  const debugLogsRef = useRef<AiPlaygroundDebugLog[]>([]);
  const debugSequenceRef = useRef(0);
  const debugFlushTimerRef = useRef<number | null>(null);
  const latestClientSnapshotRef = useRef<AiPlaygroundClientSnapshot | null>(null);
  const lastDebugPayloadRef = useRef("");
  const lastHeartbeatReasonRef = useRef("initial");
  const lastModelProgressLogLengthRef = useRef(0);
  const autoInitializeStartedRef = useRef(false);
  const chatThreadRef = useRef<HTMLDivElement | null>(null);

  const addDebugLog = useCallback(
    (level: DebugLogLevel, event: string, details?: Record<string, unknown>) => {
      const log = {
        at: new Date().toISOString(),
        details: normalizeDebugDetails(details),
        event,
        level,
        sequence: debugSequenceRef.current + 1,
      } satisfies AiPlaygroundDebugLog;
      debugSequenceRef.current = log.sequence;
      debugLogsRef.current = [...debugLogsRef.current, log].slice(-DEBUG_LOG_LIMIT);
      lastHeartbeatReasonRef.current = event;
      setDebugLogs(debugLogsRef.current);
    },
    [],
  );

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    setDebugEnabled(isDebugDisplayAllowed());
    const storageKey = "pc-keiba-ai-playground-session-id";
    const existingSessionId = window.sessionStorage.getItem(storageKey);
    const nextSessionId = existingSessionId || createSafeId();
    window.sessionStorage.setItem(storageKey, nextSessionId);
    sessionIdRef.current = nextSessionId;
    setSessionId(nextSessionId);
    addDebugLog("info", "debug session initialized", {
      mockMode: isMockMode(),
      route: window.location.pathname,
      sessionId: nextSessionId,
      userAgent: window.navigator.userAgent,
    });
  }, [addDebugLog]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return undefined;
    }
    setSettings(getRaceAiSettings());
    return subscribeRaceAiSettings((nextSettings) => {
      setSettings(nextSettings);
      addDebugLog("info", "ai settings changed", {
        autoStart: nextSettings.autoStart,
        consent: nextSettings.consent,
      });
    });
  }, [addDebugLog]);

  useEffect(() => {
    const handleError = (event: ErrorEvent) => {
      addDebugLog("error", "window error", {
        column: event.colno,
        filename: event.filename,
        line: event.lineno,
        message: event.message,
        stack: event.error instanceof Error ? event.error.stack : null,
      });
    };
    const handleRejection = (event: PromiseRejectionEvent) => {
      addDebugLog("error", "unhandled rejection", {
        reason: formatCaughtError(event.reason),
        stack: event.reason instanceof Error ? event.reason.stack : null,
      });
    };
    const originalConsole = {
      debug: console.debug,
      error: console.error,
      info: console.info,
      log: console.log,
      warn: console.warn,
    };
    const wrapConsole =
      (method: keyof typeof originalConsole, level: DebugLogLevel) =>
      (...args: unknown[]) => {
        originalConsole[method](...args);
        addDebugLog(level, `console.${method}`, {
          message: args.map(toConsoleArgumentText).join(" "),
        });
      };
    console.debug = wrapConsole("debug", "debug");
    console.error = wrapConsole("error", "error");
    console.info = wrapConsole("info", "info");
    console.log = wrapConsole("log", "debug");
    console.warn = wrapConsole("warn", "warn");
    window.addEventListener("error", handleError);
    window.addEventListener("unhandledrejection", handleRejection);
    return () => {
      console.debug = originalConsole.debug;
      console.error = originalConsole.error;
      console.info = originalConsole.info;
      console.log = originalConsole.log;
      console.warn = originalConsole.warn;
      window.removeEventListener("error", handleError);
      window.removeEventListener("unhandledrejection", handleRejection);
    };
  }, [addDebugLog]);

  const refreshModelState = useCallback(() => {
    addDebugLog("debug", "model cache state refresh requested");
    void getRaceAiModelState(LATEST_RACE_AI_MODEL)
      .then((state) => {
        setModelState(state);
        addDebugLog("debug", "model cache state refresh completed", {
          progress: state.progress,
          status: state.status,
          totalBytes: state.totalBytes,
        });
        return undefined;
      })
      .catch((caught: unknown) => {
        const message = formatCaughtError(caught);
        setError(message);
        addDebugLog("error", "model cache state refresh failed", {
          message,
        });
      });
  }, [addDebugLog]);

  useEffect(() => {
    const nextMockMode = isMockMode();
    setMockMode(nextMockMode);
    addDebugLog("info", "support check started", {
      hasNavigator: typeof navigator !== "undefined",
      mockMode: nextMockMode,
    });
    if (nextMockMode) {
      setSupportState("supported");
      setStatusMessage("localhost mockでAI SDKの動作を確認できます。");
      addDebugLog("info", "support check completed", {
        mode: "localhost mock",
        supportState: "supported",
      });
      refreshModelState();
      const unsubscribe = subscribeRaceAiModelDownloads(refreshModelState);
      return () => {
        unsubscribe();
      };
    }
    if (typeof navigator === "undefined" || !("gpu" in navigator)) {
      setSupportState("unsupported");
      setStatusMessage("このブラウザはWebGPUに対応していません。");
      addDebugLog("warn", "support check completed", {
        supportState: "unsupported",
      });
      return undefined;
    }
    setSupportState("supported");
    setStatusMessage("AIを読み込めます。");
    addDebugLog("info", "support check completed", {
      supportState: "supported",
    });
    refreshModelState();
    const unsubscribe = subscribeRaceAiModelDownloads(refreshModelState);
    return () => {
      unsubscribe();
    };
  }, [addDebugLog, refreshModelState]);

  const ensureModel = useCallback(async (): Promise<LlmInference | null> => {
    addDebugLog("info", "ensure model requested", {
      hasComponentModel: Boolean(llmRef.current),
      hasSharedModel: Boolean(sharedAiPlaygroundModel),
      hasSharedPromise: Boolean(sharedAiPlaygroundModelPromise),
      mockMode: isMockMode(),
    });
    if (isMockMode()) {
      setModelStatus("ready");
      setStatusMessage("localhost mockでAI SDKの応答表示を確認できます。");
      addDebugLog("info", "ensure model skipped for mock mode");
      return null;
    }
    if (llmRef.current) {
      addDebugLog("debug", "ensure model reused component instance");
      return llmRef.current;
    }
    if (sharedAiPlaygroundModel) {
      llmRef.current = sharedAiPlaygroundModel;
      setModelStatus("ready");
      setStatusMessage("AIを読み込み済みです。");
      addDebugLog("debug", "ensure model reused shared instance");
      return sharedAiPlaygroundModel;
    }
    if (sharedAiPlaygroundModelPromise) {
      setModelStatus("loading");
      setStatusMessage("他の処理でAIを読み込み中です。");
      addDebugLog("info", "ensure model awaiting shared initialization");
      const model = await sharedAiPlaygroundModelPromise;
      llmRef.current = model;
      setModelStatus("ready");
      setStatusMessage("AIを読み込み済みです。");
      addDebugLog("info", "ensure model shared initialization completed");
      return model;
    }

    setError(null);
    setModelStatus("loading");
    setStatusMessage("AIモデルを読み込んでいます。");
    addDebugLog("info", "model buffer ensure started", {
      modelName: LATEST_RACE_AI_MODEL.name,
      modelVersion: LATEST_RACE_AI_MODEL.version,
      sizeBytes: LATEST_RACE_AI_MODEL.sizeBytes,
    });
    await new Promise<void>((resolve) => {
      window.requestAnimationFrame(() => {
        window.setTimeout(resolve, 0);
      });
    });
    let buffer: ArrayBuffer;
    try {
      const beforeDownloadState = await getRaceAiModelState(LATEST_RACE_AI_MODEL);
      addDebugLog("info", "model cache state before buffer ensure", {
        downloadedBytes: beforeDownloadState.downloadedBytes,
        progress: beforeDownloadState.progress,
        status: beforeDownloadState.status,
        totalBytes: beforeDownloadState.totalBytes,
      });
      if (
        beforeDownloadState.status !== "downloaded" &&
        beforeDownloadState.status !== "downloading"
      ) {
        addDebugLog("info", "model route head check started", {
          url: LATEST_RACE_AI_MODEL.url,
        });
        await fetchModelHead()
          .then((result) => {
            addDebugLog("info", "model route head check completed", result);
            return undefined;
          })
          .catch((caught: unknown) => {
            addDebugLog("warn", "model route head check failed", {
              message: formatCaughtError(caught),
            });
          });
        addDebugLog("info", "model download confirmation requested", {
          sizeBytes: LATEST_RACE_AI_MODEL.sizeBytes,
        });
        const allowed = window.confirm(RACE_AI_MODEL_DOWNLOAD_CONFIRM_MESSAGE);
        addDebugLog(allowed ? "info" : "warn", "model download confirmation answered", {
          allowed,
        });
        if (!allowed) {
          throw new Error("AIモデルのダウンロードがキャンセルされました。");
        }
      }
      buffer = await ensureRaceAiModelBuffer({
        confirmDownload: false,
        model: LATEST_RACE_AI_MODEL,
      });
    } catch (caught) {
      const message = formatCaughtError(caught);
      setModelStatus("error");
      setStatusMessage("AIモデルを読み込めませんでした。");
      setError(message);
      addDebugLog("error", "model buffer ensure failed", {
        message,
      });
      throw caught;
    }
    addDebugLog("info", "model buffer ensure completed", {
      byteLength: buffer.byteLength,
    });
    setStatusMessage("AIランタイムを初期化しています。");
    sharedAiPlaygroundModelPromise = (async () => {
      addDebugLog("info", "mediapipe import started");
      const { FilesetResolver, LlmInference: LlmInferenceClass } =
        await import("@mediapipe/tasks-genai");
      addDebugLog("info", "mediapipe import completed");
      addDebugLog("info", "mediapipe wasm resolver started", {
        wasmBaseUrl: WASM_BASE_URL,
      });
      const genai = await FilesetResolver.forGenAiTasks(WASM_BASE_URL);
      addDebugLog("info", "mediapipe wasm resolver completed");
      addDebugLog("info", "webgpu device creation started");
      const device = await LlmInferenceClass.createWebGpuDevice();
      addDebugLog("info", "webgpu device creation completed");
      addDebugLog("info", "llm createFromOptions started");
      const model = await LlmInferenceClass.createFromOptions(genai, {
        baseOptions: {
          delegate: "GPU",
          gpuOptions: { device },
          modelAssetBuffer: new Uint8Array(buffer),
        },
        maxTokens: 2_048,
        randomSeed: 20260518,
        temperature: 0.35,
        topK: 40,
      });
      sharedAiPlaygroundModel = model;
      addDebugLog("info", "llm createFromOptions completed");
      return model;
    })();
    try {
      const model = await sharedAiPlaygroundModelPromise;
      llmRef.current = model;
      setModelStatus("ready");
      setStatusMessage("AIを読み込み済みです。");
      addDebugLog("info", "ensure model completed");
      return model;
    } catch (caught) {
      const message = formatCaughtError(caught);
      setModelStatus("error");
      setError(message);
      addDebugLog("error", "ensure model failed", {
        message,
      });
      throw caught;
    } finally {
      sharedAiPlaygroundModelPromise = null;
      refreshModelState();
    }
  }, [addDebugLog, refreshModelState]);

  useEffect(() => {
    const shouldAutoInitialize =
      isMockMode() || settings?.consent === "granted" || modelState?.status === "downloaded";
    if (supportState !== "supported" || autoInitializeStartedRef.current || !shouldAutoInitialize) {
      return;
    }
    autoInitializeStartedRef.current = true;
    addDebugLog("info", "auto model initialization started", {
      mockMode: isMockMode(),
      modelCacheStatus: modelState?.status ?? "unknown",
      modelStatus,
      settingsConsent: settings?.consent ?? "unknown",
    });
    void ensureModel().catch((caught: unknown) => {
      addDebugLog("error", "auto model initialization failed", {
        message: formatCaughtError(caught),
      });
    });
  }, [addDebugLog, ensureModel, modelState?.status, modelStatus, settings?.consent, supportState]);

  const generateAnswer = useCallback(
    async ({
      abortSignal,
      emit,
      messages,
      request,
    }: {
      abortSignal: AbortSignal | undefined;
      emit: (delta: string) => void;
      messages: UIMessage[];
      request: string;
    }) => {
      setError(null);
      setModelPartialLength(0);
      lastModelProgressLogLengthRef.current = 0;
      setStatusMessage("AIに送信しています。");
      addDebugLog("info", "chat send started", {
        messageCount: messages.length,
        request: truncateDebugText(request),
        requestLength: request.length,
      });
      if (isMockMode()) {
        addDebugLog("info", "mock response stream started");
        await streamDisplayText({
          abortSignal,
          emit,
          text: `mock応答: 「${request}」を受け取りました。AI SDKの入力、送信、ストリーミング表示は動作しています。`,
        });
        setStatusMessage("mock応答が完了しました。");
        addDebugLog("info", "mock response stream completed");
        return;
      }
      try {
        const llm = await ensureModel();
        if (!llm) {
          throw new Error("AIモデルを読み込めませんでした。");
        }
        callOptionalLlmMethod(llm, "clearCancelSignals");
        const cancelProcessing = () => {
          addDebugLog("warn", "model generation cancel requested");
          callOptionalLlmMethod(llm, "cancelProcessing");
        };
        abortSignal?.addEventListener("abort", cancelProcessing, { once: true });
        try {
          const prompt = buildPrompt(messages, request);
          const streamCleaner = createModelStreamCleaner();
          let emittedText = "";
          let emittedTextLength = 0;
          let rawStreamedText = "";
          addDebugLog("info", "model generation started", {
            promptLength: prompt.length,
            promptSample: truncateDebugSample(prompt),
          });
          const response = await llm.generateResponse(prompt, (partialResult, done) => {
            if (abortSignal?.aborted) {
              return;
            }
            const delta =
              rawStreamedText && partialResult.startsWith(rawStreamedText)
                ? partialResult.slice(rawStreamedText.length)
                : partialResult;
            rawStreamedText += delta;
            const visibleDelta = streamCleaner.push(delta);
            if (visibleDelta) {
              emittedText += visibleDelta;
              emittedTextLength += visibleDelta.length;
              emit(visibleDelta);
            }
            setModelPartialLength(rawStreamedText.length);
            setStatusMessage(
              done
                ? `AI生成の最終処理中です。${emittedTextLength.toLocaleString("ja-JP")}文字`
                : `AI生成中です。${emittedTextLength.toLocaleString("ja-JP")}文字受信`,
            );
            if (
              rawStreamedText.length - lastModelProgressLogLengthRef.current >=
                MODEL_PROGRESS_LOG_STEP ||
              done
            ) {
              lastModelProgressLogLengthRef.current = rawStreamedText.length;
              addDebugLog("debug", "model generation progress", {
                done,
                emittedTextLength,
                partialDeltaLength: delta.length,
                partialLength: partialResult.length,
                rawSample: done ? truncateDebugSample(rawStreamedText) : undefined,
                rawLength: rawStreamedText.length,
                visibleSample: done ? truncateDebugSample(cleanModelText(emittedText)) : undefined,
              });
            }
          });
          const finalDelta =
            response && rawStreamedText
              ? response.startsWith(rawStreamedText)
                ? response.slice(rawStreamedText.length)
                : ""
              : response;
          if (finalDelta) {
            rawStreamedText += finalDelta;
            const visibleFinalDelta = streamCleaner.push(finalDelta);
            if (visibleFinalDelta) {
              emittedText += visibleFinalDelta;
              emittedTextLength += visibleFinalDelta.length;
              emit(visibleFinalDelta);
            }
          }
          const tail = streamCleaner.flush();
          if (tail) {
            emittedText += tail;
            emittedTextLength += tail.length;
            emit(tail);
          }
          if (emittedTextLength === 0 && !abortSignal?.aborted) {
            emittedText = EMPTY_ASSISTANT_FALLBACK;
            emit(EMPTY_ASSISTANT_FALLBACK);
            setStatusMessage("AIから表示できる本文が返りませんでした。");
            addDebugLog("warn", "model response empty after cleanup", {
              rawSample: truncateDebugSample(rawStreamedText),
              responseLength: rawStreamedText.length,
            });
          }
          setModelPartialLength(rawStreamedText.length);
          addDebugLog("info", "model generation completed", {
            emittedSample: truncateDebugSample(cleanModelText(emittedText)),
            emittedLength: emittedTextLength,
            rawSample: truncateDebugSample(rawStreamedText),
            responseLength: rawStreamedText.length,
          });
        } finally {
          abortSignal?.removeEventListener("abort", cancelProcessing);
          callOptionalLlmMethod(llm, "clearCancelSignals");
        }
        setStatusMessage("AIからの返答が完了しました。");
        addDebugLog("info", "chat send completed");
      } catch (caught) {
        const message = formatCaughtError(caught);
        setError(message);
        setStatusMessage("AIからの返答でエラーが発生しました。");
        addDebugLog("error", "chat send failed", {
          message,
        });
        throw caught;
      } finally {
        if (abortSignal?.aborted) {
          addDebugLog("warn", "chat send aborted");
        }
      }
    },
    [addDebugLog, ensureModel],
  );

  const localChatTransport = useMemo<ChatTransport<UIMessage>>(
    () => ({
      reconnectToStream: async () => null,
      sendMessages: async ({ abortSignal, messages }) =>
        createLocalChatStream({
          abortSignal,
          execute: async (emit) => {
            const request = uiMessageText(messages.at(-1)) || "こんにちは。";
            addDebugLog("info", "chat transport execute started", {
              request: truncateDebugText(request),
              requestLength: request.length,
            });
            await generateAnswer({ abortSignal, emit, messages, request });
          },
        }),
    }),
    [addDebugLog, generateAnswer],
  );

  const {
    error: chatError,
    messages,
    sendMessage,
    setMessages,
    status: chatStatus,
    stop,
  } = useChat({
    id: "ai-playground",
    onError: (caught) => {
      setError(caught.message);
      setStatusMessage("AIからの返答でエラーが発生しました。");
      addDebugLog("error", "ai sdk chat error", {
        message: caught.message,
      });
    },
    transport: localChatTransport,
  });

  const lastAssistantText = useMemo(() => {
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      if (messages[index]?.role === "assistant") {
        return uiMessageText(messages[index]);
      }
    }
    return "";
  }, [messages]);
  const lastUserText = useMemo(() => {
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      if (messages[index]?.role === "user") {
        return uiMessageText(messages[index]);
      }
    }
    return "";
  }, [messages]);
  const clientSnapshot = useMemo<AiPlaygroundClientSnapshot>(
    () => ({
      chatStatus,
      error: error ?? chatError?.message ?? null,
      lastAssistantTextLength: lastAssistantText.length,
      lastUserTextLength: lastUserText.length,
      mockMode,
      modelCacheStatus: modelState?.status ?? "unknown",
      modelPartialLength,
      modelStatus,
      statusMessage,
      supportState,
    }),
    [
      chatError,
      chatStatus,
      error,
      lastAssistantText.length,
      lastUserText.length,
      mockMode,
      modelPartialLength,
      modelState?.status,
      modelStatus,
      statusMessage,
      supportState,
    ],
  );

  const flushDebugSnapshot = useCallback(async (reason: string) => {
    const activeSessionId = sessionIdRef.current;
    const latestClientSnapshot = latestClientSnapshotRef.current;
    if (!activeSessionId || !latestClientSnapshot || typeof window === "undefined") {
      return;
    }
    const payload = {
      client: latestClientSnapshot,
      logs: debugLogsRef.current,
      reason,
      route: `${window.location.pathname}${window.location.search}`,
      sessionId: activeSessionId,
      userAgent: window.navigator.userAgent,
    };
    const fingerprint = JSON.stringify({
      client: payload.client,
      lastLogSequence: payload.logs.at(-1)?.sequence ?? 0,
      logCount: payload.logs.length,
      reason,
      sessionId: activeSessionId,
    });
    if (reason !== "heartbeat" && lastDebugPayloadRef.current === fingerprint) {
      return;
    }
    lastDebugPayloadRef.current = fingerprint;
    try {
      const response = await fetch("/api/debug/ai-playground", {
        body: JSON.stringify(payload),
        cache: "no-store",
        headers: { "content-type": "application/json" },
        method: "PUT",
      });
      if (!response.ok) {
        throw new Error(`debug sync failed: ${response.status}`);
      }
      const result: unknown = await response.json();
      if (
        result &&
        typeof result === "object" &&
        "serverSequence" in result &&
        typeof result.serverSequence === "number" &&
        "serverNow" in result &&
        typeof result.serverNow === "string" &&
        "logCount" in result &&
        typeof result.logCount === "number"
      ) {
        setServerDebugAck({
          logCount: result.logCount,
          serverNow: result.serverNow,
          serverSequence: result.serverSequence,
        });
      }
      setDebugFlushError(null);
    } catch (caught) {
      setDebugFlushError(formatCaughtError(caught));
    }
  }, []);

  const scheduleDebugFlush = useCallback(
    (reason: string) => {
      if (typeof window === "undefined") {
        return;
      }
      if (debugFlushTimerRef.current !== null) {
        window.clearTimeout(debugFlushTimerRef.current);
      }
      debugFlushTimerRef.current = window.setTimeout(() => {
        debugFlushTimerRef.current = null;
        void flushDebugSnapshot(reason);
      }, DEBUG_FLUSH_DEBOUNCE_MS);
    },
    [flushDebugSnapshot],
  );

  const resetDebugLogs = useCallback(() => {
    const activeSessionId = sessionIdRef.current;
    debugLogsRef.current = [];
    debugSequenceRef.current = 0;
    setDebugLogs([]);
    setServerDebugAck(null);
    setDebugFlushError(null);
    if (activeSessionId) {
      void fetch("/api/debug/ai-playground", {
        body: JSON.stringify({ action: "clear", sessionId: activeSessionId }),
        cache: "no-store",
        headers: { "content-type": "application/json" },
        method: "POST",
      }).catch(() => {});
    }
    addDebugLog("info", "debug logs reset", { sessionId: activeSessionId });
  }, [addDebugLog]);

  const copyAllDebugLogs = useCallback(async () => {
    const activeSessionId = sessionIdRef.current;
    addDebugLog("info", "copy all debug logs requested", {
      localLogCount: debugLogsRef.current.length,
      sessionId: activeSessionId,
    });
    setCopyLogStatus("全ログを準備しています。");
    let serverSnapshot: unknown = null;
    let serverSnapshotError: string | null = null;
    try {
      await flushDebugSnapshot("copy-all-logs");
      if (activeSessionId) {
        const response = await fetch(
          `/api/debug/ai-playground?sessionId=${encodeURIComponent(activeSessionId)}`,
          { cache: "no-store" },
        );
        if (!response.ok) {
          throw new Error(`debug snapshot fetch failed: ${response.status}`);
        }
        serverSnapshot = await response.json();
      }
    } catch (caught) {
      serverSnapshotError = formatCaughtError(caught);
      addDebugLog("warn", "copy all debug logs server snapshot failed", {
        message: serverSnapshotError,
      });
    }

    const payload = {
      clientSnapshot: latestClientSnapshotRef.current,
      copiedAt: new Date().toISOString(),
      debugUrl: activeSessionId
        ? `/api/debug/ai-playground?sessionId=${encodeURIComponent(activeSessionId)}`
        : "/api/debug/ai-playground",
      localLogs: debugLogsRef.current,
      location:
        typeof window === "undefined"
          ? null
          : {
              href: window.location.href,
              userAgent: window.navigator.userAgent,
            },
      messages: messages.map((message) => ({
        id: message.id,
        role: message.role,
        text: uiMessageText(message),
      })),
      modelState,
      serverDebugAck,
      serverSnapshot,
      serverSnapshotError,
      sessionId: activeSessionId,
      settings,
    };

    try {
      const text = JSON.stringify(payload, null, 2);
      await copyTextToClipboard(text);
      setCopyLogStatus(`全ログをコピーしました。${debugLogsRef.current.length}件`);
      addDebugLog("info", "copy all debug logs completed", {
        localLogCount: debugLogsRef.current.length,
        textLength: text.length,
      });
      void flushDebugSnapshot("copy-all-logs-completed");
    } catch (caught) {
      const message = formatCaughtError(caught);
      setCopyLogStatus(`コピーに失敗しました: ${message}`);
      addDebugLog("error", "copy all debug logs failed", {
        message,
      });
      void flushDebugSnapshot("copy-all-logs-failed");
    }
  }, [addDebugLog, flushDebugSnapshot, messages, modelState, serverDebugAck, settings]);

  useEffect(() => {
    latestClientSnapshotRef.current = clientSnapshot;
  }, [clientSnapshot]);

  useEffect(() => {
    addDebugLog("debug", "client ui state changed", { ...clientSnapshot });
  }, [addDebugLog, clientSnapshot]);

  useEffect(() => {
    if (debugLogs.length === 0) {
      return;
    }
    scheduleDebugFlush(lastHeartbeatReasonRef.current);
  }, [debugLogs, scheduleDebugFlush]);

  useEffect(() => {
    void flushDebugSnapshot("mount");
    const timer = window.setInterval(() => {
      void flushDebugSnapshot("heartbeat");
    }, DEBUG_HEARTBEAT_INTERVAL_MS);
    return () => {
      window.clearInterval(timer);
      if (debugFlushTimerRef.current !== null) {
        window.clearTimeout(debugFlushTimerRef.current);
      }
    };
  }, [flushDebugSnapshot]);

  const isSending = chatStatus === "submitted" || chatStatus === "streaming";
  const submitChatInput = useCallback(() => {
    const value = input.trim();
    if (!value || isSending) {
      return;
    }
    addDebugLog("info", "chat form submitted", {
      input: truncateDebugText(value),
      inputLength: value.length,
    });
    setInput("");
    void sendMessage({ text: value });
  }, [addDebugLog, input, isSending, sendMessage]);

  useEffect(() => {
    const chatThread = chatThreadRef.current;
    if (!chatThread) {
      return;
    }
    chatThread.scrollTop = chatThread.scrollHeight;
  }, [messages, isSending, statusMessage]);

  const debugUrl = sessionId
    ? `/api/debug/ai-playground?sessionId=${encodeURIComponent(sessionId)}`
    : "/api/debug/ai-playground";
  const serverSyncLabel = serverDebugAck
    ? `server #${serverDebugAck.serverSequence} / ${serverDebugAck.logCount}件 / ${serverDebugAck.serverNow.slice(11, 19)}`
    : "server未同期";
  const modelReadyLabel = mockMode
    ? "mock ready"
    : modelStatus === "ready"
      ? "読み込み済み"
      : modelStatus === "loading"
        ? "読み込み中"
        : modelStatus === "error"
          ? "エラー"
          : "未読み込み";
  const diagnosticLog = (
    <details
      className="race-ai-diagnostic-log"
      open={isSending || Boolean(error || chatError) || modelStatus === "loading"}
    >
      <summary>
        <strong>診断ログ</strong>
        <span>
          {debugLogs.length}件 / {serverSyncLabel}
        </span>
      </summary>
      <p>
        サーバー確認: <code>{debugUrl}</code>
      </p>
      {debugFlushError ? <p className="race-ai-diagnostic-log-error">{debugFlushError}</p> : null}
      {debugLogs.length === 0 ? (
        <p>診断ログはまだありません。</p>
      ) : (
        <ol>
          {debugLogs.map((log) => (
            <li className={`race-ai-diagnostic-log-${log.level}`} key={log.sequence}>
              <time dateTime={log.at}>{log.at.slice(11, 19)}</time>
              <strong>{log.level}</strong>
              <span>{log.event}</span>
              {log.details ? <code>{formatDebugLogDetails(log.details)}</code> : null}
            </li>
          ))}
        </ol>
      )}
      {copyLogStatus ? <p aria-live="polite">{copyLogStatus}</p> : null}
      <div className="race-ai-actions">
        <button
          type="button"
          onClick={() => {
            void copyAllDebugLogs();
          }}
        >
          全ログをコピー
        </button>
        <button type="button" onClick={resetDebugLogs}>
          診断ログをリセット
        </button>
        <button
          type="button"
          onClick={() => {
            void flushDebugSnapshot("manual");
          }}
        >
          サーバーへ同期
        </button>
      </div>
    </details>
  );

  if (supportState === "unsupported" && !mockMode) {
    return (
      <section className="race-ai-assistant-section ai-playground-panel">
        <div className="race-ai-readiness-panel">
          <strong>WebGPU非対応</strong>
          <p className="race-ai-readiness-note">
            このブラウザではWebGPU AIの動作確認はできません。
          </p>
        </div>
        {debugEnabled ? diagnosticLog : null}
      </section>
    );
  }

  return (
    <section className="race-ai-assistant-section ai-playground-panel">
      <div className="race-ai-readiness-panel">
        <div className="race-ai-readiness-summary">
          <span className="race-ai-readiness-summary-title">
            <strong>AI読み込み</strong>
            <small>{LATEST_RACE_AI_MODEL.name}</small>
          </span>
          <span className="race-ai-readiness-overall">
            <progress value={modelState?.progress ?? undefined} max={1} />
            <span>{progressLabel(modelState)}</span>
          </span>
        </div>
        {debugEnabled ? (
          <div className="race-ai-readiness-status-grid">
            <div>
              <span>WebGPU</span>
              <strong>{mockMode ? "mock" : "対応"}</strong>
            </div>
            <div>
              <span>モデル</span>
              <strong>{modelReadyLabel}</strong>
            </div>
            <div>
              <span>キャッシュ</span>
              <strong>{mockMode ? "mock" : statusLabel(modelState?.status)}</strong>
            </div>
            <div>
              <span>サイズ</span>
              <strong>
                {formatRaceAiModelSize(modelState?.totalBytes ?? LATEST_RACE_AI_MODEL.sizeBytes)}
              </strong>
            </div>
          </div>
        ) : null}
        <p className="race-ai-readiness-note">{statusMessage}</p>
        <div className="race-ai-actions">
          <button
            type="button"
            disabled={modelStatus === "loading" || isSending}
            onClick={() => {
              addDebugLog("info", "manual model load clicked");
              void ensureModel().catch(() => {});
            }}
          >
            {modelStatus === "loading" ? "読み込み中" : "AIを読み込む"}
          </button>
          <button
            type="button"
            disabled={isSending || messages.length === 0}
            onClick={() => {
              addDebugLog("info", "chat clear clicked", {
                messageCount: messages.length,
              });
              setMessages([]);
              setStatusMessage(
                modelStatus === "ready" ? "AIを読み込み済みです。" : "AIを読み込めます。",
              );
            }}
          >
            会話をクリア
          </button>
        </div>
      </div>

      <div className="race-ai-runtime-panel" aria-live="polite">
        <span>AI状態</span>
        <strong>
          {isSending ? statusMessage : modelStatus === "loading" ? "AIを読み込み中" : "待機中"}
        </strong>
        {modelPartialLength > 0 || debugEnabled ? (
          <small>
            {modelPartialLength > 0
              ? `モデル応答 ${modelPartialLength.toLocaleString("ja-JP")} 文字受信`
              : `debug ${sessionId || "initializing"}`}
          </small>
        ) : null}
      </div>

      {debugEnabled ? diagnosticLog : null}

      <div className="race-ai-chat-thread" ref={chatThreadRef} aria-live="polite">
        {messages.length === 0 ? (
          <p className="empty-state">AIへの入力と返答はここに表示されます。</p>
        ) : (
          messages.map((message) => (
            <article className={`race-ai-chat-message ${message.role}`} key={message.id}>
              <span>{message.role === "user" ? "あなた" : "AI"}</span>
              <div>
                <p>{assistantDisplayText(message)}</p>
              </div>
            </article>
          ))
        )}
        {isSending ? <p className="race-ai-stream-status">{statusMessage}</p> : null}
      </div>

      {error || chatError ? <p className="race-ai-error">{error ?? chatError?.message}</p> : null}

      <form
        className="race-ai-chat-form"
        onSubmit={(event) => {
          event.preventDefault();
          submitChatInput();
        }}
      >
        <label>
          AIに送るテキスト
          <textarea
            value={input}
            rows={4}
            onChange={(event) => {
              setInput(event.currentTarget.value);
            }}
            onKeyDown={(event) => {
              if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing) {
                return;
              }
              event.preventDefault();
              submitChatInput();
            }}
          />
        </label>
        <button type="submit" disabled={isSending || !input.trim()}>
          {isSending ? "送信中" : "送信"}
        </button>
      </form>
      {isSending ? (
        <div className="race-ai-stop-actions">
          <button
            type="button"
            onClick={() => {
              addDebugLog("warn", "chat stop clicked");
              void stop();
            }}
          >
            応答を停止
          </button>
        </div>
      ) : null}
    </section>
  );
}
