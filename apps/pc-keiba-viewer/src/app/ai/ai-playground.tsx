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

const createId = (): string => `${Date.now().toString(36)}-${crypto.randomUUID()}`;

const createSafeId = (): string => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return createId();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
};

const formatCaughtError = (caught: unknown): string =>
  caught instanceof Error ? caught.message : String(caught);

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

const uiMessageText = (message: UIMessage | undefined): string =>
  (message?.parts ?? [])
    .map((part) => (part.type === "text" ? part.text : ""))
    .join("")
    .trim();

const cleanModelText = (text: string): string =>
  text
    .replace(/<start_of_turn>|<end_of_turn>|<bos>|<eos>/gu, "")
    .replace(/^```(?:json|markdown|text)?\s*/u, "")
    .replace(/\s*```$/u, "")
    .trim();

const isLocalhostBrowser = (): boolean =>
  typeof window !== "undefined" &&
  ["localhost", "127.0.0.1", "0.0.0.0", "::1"].includes(window.location.hostname);

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

const buildPrompt = (messages: UIMessage[], request: string): string => {
  const recentMessages = messages.slice(-8).map((message) => ({
    role: message.role,
    text: uiMessageText(message),
  }));
  return buildGemmaPrompt(
    [
      "あなたはPC-KEIBA Viewerの動作確認用AIです。",
      "日本語で、簡潔に、ユーザーの入力に直接返答してください。",
      "競馬データの取得や予想はこのページでは行わず、モデル応答の疎通確認として回答してください。",
      "直近の対話:",
      JSON.stringify(recentMessages),
      "今回のユーザー入力:",
      request,
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
  const [debugLogs, setDebugLogs] = useState<AiPlaygroundDebugLog[]>([]);
  const [serverDebugAck, setServerDebugAck] = useState<ServerDebugAck | null>(null);
  const [debugFlushError, setDebugFlushError] = useState<string | null>(null);
  const llmRef = useRef<LlmInference | null>(null);
  const sessionIdRef = useRef("");
  const debugLogsRef = useRef<AiPlaygroundDebugLog[]>([]);
  const debugSequenceRef = useRef(0);
  const debugFlushTimerRef = useRef<number | null>(null);
  const latestClientSnapshotRef = useRef<AiPlaygroundClientSnapshot | null>(null);
  const lastDebugPayloadRef = useRef("");
  const lastHeartbeatReasonRef = useRef("initial");
  const lastModelProgressLogLengthRef = useRef(0);

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
    const buffer = await ensureRaceAiModelBuffer({
      confirmDownload: true,
      model: LATEST_RACE_AI_MODEL,
    });
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
          let emittedLength = 0;
          addDebugLog("info", "model generation started", {
            promptLength: prompt.length,
          });
          const response = await llm.generateResponse(prompt, (partialResult, done) => {
            if (abortSignal?.aborted) {
              return;
            }
            const cleaned = cleanModelText(partialResult);
            setModelPartialLength(partialResult.length);
            setStatusMessage(
              done
                ? `AI生成の最終処理中です。${cleaned.length.toLocaleString("ja-JP")}文字`
                : `AI生成中です。${cleaned.length.toLocaleString("ja-JP")}文字受信`,
            );
            if (
              partialResult.length - lastModelProgressLogLengthRef.current >=
                MODEL_PROGRESS_LOG_STEP ||
              done
            ) {
              lastModelProgressLogLengthRef.current = partialResult.length;
              addDebugLog("debug", "model generation progress", {
                cleanedLength: cleaned.length,
                done,
                partialLength: partialResult.length,
              });
            }
            const delta = cleaned.slice(emittedLength);
            if (delta) {
              emittedLength = cleaned.length;
              emit(delta);
            }
          });
          const finalText = cleanModelText(response);
          const remaining = finalText.slice(emittedLength);
          if (remaining) {
            emit(remaining);
          }
          setModelPartialLength(response.length);
          addDebugLog("info", "model generation completed", {
            emittedLength: Math.max(emittedLength, finalText.length),
            responseLength: response.length,
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
      <div className="race-ai-actions">
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
        {diagnosticLog}
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
        <small>
          {modelPartialLength > 0
            ? `モデル応答 ${modelPartialLength.toLocaleString("ja-JP")} 文字受信`
            : `debug ${sessionId || "initializing"}`}
        </small>
      </div>

      {diagnosticLog}

      <div className="race-ai-chat-thread" aria-live="polite">
        {messages.length === 0 ? (
          <p className="empty-state">AIへの入力と返答はここに表示されます。</p>
        ) : (
          messages.map((message) => (
            <article className={`race-ai-chat-message ${message.role}`} key={message.id}>
              <span>{message.role === "user" ? "あなた" : "AI"}</span>
              <div>
                <p>{uiMessageText(message)}</p>
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
          const value = input.trim();
          if (!value || isSending) {
            return;
          }
          addDebugLog("info", "chat form submitted", {
            inputLength: value.length,
          });
          setInput("");
          void sendMessage({ text: value });
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
