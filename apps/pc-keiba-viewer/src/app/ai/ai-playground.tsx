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

let sharedAiPlaygroundModel: LlmInference | null = null;
let sharedAiPlaygroundModelPromise: Promise<LlmInference> | null = null;

const WASM_BASE_URL = "https://cdn.jsdelivr.net/npm/@mediapipe/tasks-genai@0.10.27/wasm";
const LOCAL_MOCK_SEARCH_PARAM = "mock";

const createId = (): string => `${Date.now().toString(36)}-${crypto.randomUUID()}`;

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
  const llmRef = useRef<LlmInference | null>(null);

  const refreshModelState = useCallback(() => {
    void getRaceAiModelState(LATEST_RACE_AI_MODEL)
      .then(setModelState)
      .catch((caught: unknown) => {
        setError(caught instanceof Error ? caught.message : String(caught));
      });
  }, []);

  useEffect(() => {
    const nextMockMode = isMockMode();
    setMockMode(nextMockMode);
    if (nextMockMode) {
      setSupportState("supported");
      setStatusMessage("localhost mockでAI SDKの動作を確認できます。");
      refreshModelState();
      const unsubscribe = subscribeRaceAiModelDownloads(refreshModelState);
      return () => {
        unsubscribe();
      };
    }
    if (typeof navigator === "undefined" || !("gpu" in navigator)) {
      setSupportState("unsupported");
      setStatusMessage("このブラウザはWebGPUに対応していません。");
      return undefined;
    }
    setSupportState("supported");
    setStatusMessage("AIを読み込めます。");
    refreshModelState();
    const unsubscribe = subscribeRaceAiModelDownloads(refreshModelState);
    return () => {
      unsubscribe();
    };
  }, [refreshModelState]);

  const ensureModel = useCallback(async (): Promise<LlmInference | null> => {
    if (isMockMode()) {
      setModelStatus("ready");
      setStatusMessage("localhost mockでAI SDKの応答表示を確認できます。");
      return null;
    }
    if (llmRef.current) {
      return llmRef.current;
    }
    if (sharedAiPlaygroundModel) {
      llmRef.current = sharedAiPlaygroundModel;
      setModelStatus("ready");
      setStatusMessage("AIを読み込み済みです。");
      return sharedAiPlaygroundModel;
    }
    if (sharedAiPlaygroundModelPromise) {
      setModelStatus("loading");
      setStatusMessage("他の処理でAIを読み込み中です。");
      const model = await sharedAiPlaygroundModelPromise;
      llmRef.current = model;
      setModelStatus("ready");
      setStatusMessage("AIを読み込み済みです。");
      return model;
    }

    setError(null);
    setModelStatus("loading");
    setStatusMessage("AIモデルを読み込んでいます。");
    await new Promise<void>((resolve) => {
      window.requestAnimationFrame(() => {
        window.setTimeout(resolve, 0);
      });
    });
    const buffer = await ensureRaceAiModelBuffer({
      confirmDownload: true,
      model: LATEST_RACE_AI_MODEL,
    });
    setStatusMessage("AIランタイムを初期化しています。");
    sharedAiPlaygroundModelPromise = (async () => {
      const { FilesetResolver, LlmInference: LlmInferenceClass } =
        await import("@mediapipe/tasks-genai");
      const genai = await FilesetResolver.forGenAiTasks(WASM_BASE_URL);
      const device = await LlmInferenceClass.createWebGpuDevice();
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
      return model;
    })();
    try {
      const model = await sharedAiPlaygroundModelPromise;
      llmRef.current = model;
      setModelStatus("ready");
      setStatusMessage("AIを読み込み済みです。");
      return model;
    } catch (caught) {
      setModelStatus("error");
      setError(caught instanceof Error ? caught.message : String(caught));
      throw caught;
    } finally {
      sharedAiPlaygroundModelPromise = null;
      refreshModelState();
    }
  }, [refreshModelState]);

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
      setStatusMessage("AIに送信しています。");
      if (isMockMode()) {
        await streamDisplayText({
          abortSignal,
          emit,
          text: `mock応答: 「${request}」を受け取りました。AI SDKの入力、送信、ストリーミング表示は動作しています。`,
        });
        setStatusMessage("mock応答が完了しました。");
        return;
      }
      const llm = await ensureModel();
      if (!llm) {
        throw new Error("AIモデルを読み込めませんでした。");
      }
      callOptionalLlmMethod(llm, "clearCancelSignals");
      const cancelProcessing = () => {
        callOptionalLlmMethod(llm, "cancelProcessing");
      };
      abortSignal?.addEventListener("abort", cancelProcessing, { once: true });
      try {
        const response = await llm.generateResponse(buildPrompt(messages, request));
        await streamDisplayText({ abortSignal, emit, text: response });
        setStatusMessage("AIからの返答が完了しました。");
      } finally {
        abortSignal?.removeEventListener("abort", cancelProcessing);
        callOptionalLlmMethod(llm, "clearCancelSignals");
      }
    },
    [ensureModel],
  );

  const localChatTransport = useMemo<ChatTransport<UIMessage>>(
    () => ({
      reconnectToStream: async () => null,
      sendMessages: async ({ abortSignal, messages }) =>
        createLocalChatStream({
          abortSignal,
          execute: async (emit) => {
            const request = uiMessageText(messages.at(-1)) || "こんにちは。";
            await generateAnswer({ abortSignal, emit, messages, request });
          },
        }),
    }),
    [generateAnswer],
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
    },
    transport: localChatTransport,
  });

  const isSending = chatStatus === "submitted" || chatStatus === "streaming";
  const modelReadyLabel = mockMode
    ? "mock ready"
    : modelStatus === "ready"
      ? "読み込み済み"
      : modelStatus === "loading"
        ? "読み込み中"
        : modelStatus === "error"
          ? "エラー"
          : "未読み込み";

  if (supportState === "unsupported" && !mockMode) {
    return (
      <section className="race-ai-assistant-section ai-playground-panel">
        <div className="race-ai-readiness-panel">
          <strong>WebGPU非対応</strong>
          <p className="race-ai-readiness-note">
            このブラウザではWebGPU AIの動作確認はできません。
          </p>
        </div>
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
              void ensureModel().catch(() => {});
            }}
          >
            {modelStatus === "loading" ? "読み込み中" : "AIを読み込む"}
          </button>
          <button
            type="button"
            disabled={isSending || messages.length === 0}
            onClick={() => {
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
          <button type="button" onClick={stop}>
            応答を停止
          </button>
        </div>
      ) : null}
    </section>
  );
}
