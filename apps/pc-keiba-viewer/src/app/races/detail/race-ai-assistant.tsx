"use client";

import type { LlmInference } from "@mediapipe/tasks-genai";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import type { CourseInfo, RaceDetail, RaceListItem, Runner } from "../../../lib/race-types";
import {
  compactRaceAiDataForPrompt,
  fetchRaceAiExportData,
  type RaceAiExportData,
} from "./race-ai-data";
import { buildGemmaPrompt, RACE_AI_DEFAULT_PROMPT } from "./race-ai-default-prompt";
import {
  getRaceAiLog,
  loadCachedModel,
  saveCachedModel,
  saveRaceAiLog,
  type RaceAiMessage,
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

const MODEL_VERSION = "v20260518";
const MODEL_FILE_NAME = "gemma-4-E2B-it-web.task";
const MODEL_URL = `/api/models/gemma-4-e2b/${MODEL_VERSION}/${MODEL_FILE_NAME}`;
const MODEL_CACHE_KEY = `gemma-4-e2b:${MODEL_VERSION}:${MODEL_FILE_NAME}`;
const WASM_BASE_URL = "https://cdn.jsdelivr.net/npm/@mediapipe/tasks-genai@0.10.27/wasm";
const REALTIME_RETHINK_DELAY_MS = 3_000;
const LOG_LIMIT = 20;

const createId = (): string => `${Date.now().toString(36)}-${crypto.randomUUID()}`;

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

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const parseModelResponse = (text: string): ParsedRaceAiResponse => {
  const cleaned = stripJsonFence(text);
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
      answer: cleaned,
      needsTool: false,
      prediction: [],
      thoughtLog: "JSONとして解析できなかったため、モデルの回答本文をそのまま表示しました。",
      toolJavaScript: null,
    };
  }
};

const fetchArrayBufferWithProgress = async (
  url: string,
  onProgress: (progress: number | null) => void,
): Promise<ArrayBuffer> => {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`model api ${response.status}`);
  }
  const total = Number(response.headers.get("content-length"));
  if (!response.body || !Number.isFinite(total) || total <= 0) {
    onProgress(null);
    return response.arrayBuffer();
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let received = 0;
  const readChunk = async (): Promise<void> => {
    const { done, value } = await reader.read();
    if (done) {
      return;
    }
    if (value) {
      chunks.push(value);
      received += value.byteLength;
      onProgress(received / total);
    }
    await readChunk();
  };
  await readChunk();

  const buffer = new Uint8Array(received);
  let offset = 0;
  for (const chunk of chunks) {
    buffer.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return buffer.buffer;
};

const loadModelBuffer = async (onProgress: (progress: number | null) => void) => {
  const cached = await loadCachedModel(MODEL_CACHE_KEY);
  if (cached) {
    onProgress(1);
    return { buffer: cached, source: "cache" as const };
  }
  const buffer = await fetchArrayBufferWithProgress(MODEL_URL, onProgress);
  await saveCachedModel({
    buffer,
    key: MODEL_CACHE_KEY,
    modelVersion: MODEL_VERSION,
    sourceUrl: MODEL_URL,
  });
  return { buffer, source: "network" as const };
};

const buildPrompt = ({
  data,
  messages,
  request,
  thoughtLogs,
  toolResult,
}: {
  data: RaceAiExportData;
  messages: RaceAiMessage[];
  request: string;
  thoughtLogs: RaceAiThoughtLog[];
  toolResult: ToolResult | null;
}): string => {
  const promptBody = [
    RACE_AI_DEFAULT_PROMPT,
    "現在のユーザー依頼:",
    request,
    "直近の対話ログ:",
    JSON.stringify(messages.slice(-8), null, 2),
    "直近の思考ログ:",
    JSON.stringify(thoughtLogs.slice(-3), null, 2),
    "AI向けJSONの主要データ:",
    JSON.stringify(compactRaceAiDataForPrompt(data), null, 2),
    toolResult
      ? ["ツール実行結果:", JSON.stringify(toolResult, null, 2)].join("\n")
      : "ツール実行結果: なし",
  ].join("\n\n");
  return buildGemmaPrompt(promptBody);
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

const appendLimited = <T,>(rows: T[], row: T): T[] => [...rows, row].slice(-LOG_LIMIT);

export function RaceAiAssistant(props: RaceAiAssistantProps) {
  const [supportState, setSupportState] = useState<WebGpuSupportState>("checking");
  const [modelStatus, setModelStatus] = useState<ModelStatus>("idle");
  const [generationStatus, setGenerationStatus] = useState<GenerationStatus>("idle");
  const [downloadProgress, setDownloadProgress] = useState<number | null>(null);
  const [modelSource, setModelSource] = useState<"cache" | "network" | null>(null);
  const [messages, setMessages] = useState<RaceAiMessage[]>([]);
  const [thoughtLogs, setThoughtLogs] = useState<RaceAiThoughtLog[]>([]);
  const [prediction, setPrediction] = useState<RaceAiPredictionRow[]>([]);
  const [answer, setAnswer] = useState<string>("");
  const [input, setInput] = useState("");
  const [error, setError] = useState<string | null>(null);
  const llmRef = useRef<LlmInference | null>(null);
  const runningRef = useRef(false);
  const messagesRef = useRef<RaceAiMessage[]>([]);
  const thoughtLogsRef = useRef<RaceAiThoughtLog[]>([]);
  const lastRealtimeFingerprintRef = useRef("");
  const lastUserRequestRef = useRef("このレースの着順を予想してください。");
  const realtimePayload = useRealtimeRaceSelector((state) => state.payload);
  const realtimeFingerprint = useMemo(
    () => buildRealtimeFingerprint(realtimePayload),
    [realtimePayload],
  );

  const raceKey = `${props.source}:${props.year}${props.month}${props.day}:${props.keibajoCode}:${props.raceNumber}`;

  useEffect(() => {
    setSupportState("checking");
    if (typeof navigator === "undefined" || !("gpu" in navigator)) {
      setSupportState("unsupported");
      return;
    }
    setSupportState("supported");
  }, []);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const log = await getRaceAiLog(raceKey);
      if (cancelled) {
        return;
      }
      setMessages(log.messages);
      setThoughtLogs(log.thoughtLogs);
      messagesRef.current = log.messages;
      thoughtLogsRef.current = log.thoughtLogs;
    })();
    return () => {
      cancelled = true;
    };
  }, [raceKey]);

  useEffect(
    () => () => {
      llmRef.current?.close();
    },
    [],
  );

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
    if (llmRef.current) {
      return llmRef.current;
    }
    setError(null);
    setModelStatus("downloading");
    const { buffer, source } = await loadModelBuffer((progress) => {
      setDownloadProgress(progress);
    });
    setModelSource(source);
    setModelStatus("initializing");
    const { FilesetResolver, LlmInference: LlmInferenceClass } =
      await import("@mediapipe/tasks-genai");
    const genai = await FilesetResolver.forGenAiTasks(WASM_BASE_URL);
    const device = await LlmInferenceClass.createWebGpuDevice();
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
    llmRef.current = instance;
    setModelStatus("ready");
    return instance;
  }, []);

  const runAi = useCallback(
    async (request: string, trigger: string) => {
      if (runningRef.current) {
        return;
      }
      runningRef.current = true;
      setError(null);
      lastUserRequestRef.current = request;
      try {
        const llm = await ensureModel();
        setGenerationStatus("loading-data");
        const data = await fetchRaceAiExportData(props);
        const dataFingerprint = stableStringify({
          aiReady: data.aiReady.currentOutput,
          realtime: data.realtime,
        });
        setGenerationStatus("generating");
        const firstPrompt = buildPrompt({
          data,
          messages: messagesRef.current,
          request,
          thoughtLogs: thoughtLogsRef.current,
          toolResult: null,
        });
        const firstResponse = parseModelResponse(await llm.generateResponse(firstPrompt));
        let finalResponse = firstResponse;
        if (firstResponse.needsTool && firstResponse.toolJavaScript) {
          const toolResult = await runToolJavaScript(firstResponse.toolJavaScript);
          const secondPrompt = buildPrompt({
            data,
            messages: messagesRef.current,
            request: `${request}\n\n上記のツール結果を反映して最終回答を作成してください。`,
            thoughtLogs: thoughtLogsRef.current,
            toolResult,
          });
          finalResponse = parseModelResponse(await llm.generateResponse(secondPrompt));
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
          modelVersion: MODEL_VERSION,
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
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : String(caught));
      } finally {
        runningRef.current = false;
        setGenerationStatus("idle");
      }
    },
    [ensureModel, persistLogs, props],
  );

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
      void runAi(
        `${lastUserRequestRef.current}\n\nリアルタイムデータが更新されました。最新データで再評価してください。`,
        "realtime-update",
      );
    }, REALTIME_RETHINK_DELAY_MS);
    lastRealtimeFingerprintRef.current = realtimeFingerprint;
    return () => {
      window.clearTimeout(timer);
    };
  }, [generationStatus, realtimeFingerprint, runAi]);

  if (supportState !== "supported") {
    return null;
  }

  const isBusy =
    modelStatus === "downloading" || modelStatus === "initializing" || generationStatus !== "idle";
  const progressLabel =
    downloadProgress === null ? "取得中" : `${Math.round(downloadProgress * 100)}%`;

  return (
    <section className="race-ai-assistant-section">
      <div className="section-heading compact">
        <h2>WebGPU AI予想</h2>
        <span>Gemma 4 E2B / {MODEL_VERSION}</span>
      </div>
      <div className="race-ai-status-grid">
        <div>
          <span>モデル</span>
          <strong>
            {modelStatus === "ready"
              ? modelSource === "cache"
                ? "ローカルキャッシュ"
                : "読み込み済み"
              : modelStatus === "downloading"
                ? `ダウンロード ${progressLabel}`
                : modelStatus === "initializing"
                  ? "初期化中"
                  : "未起動"}
          </strong>
        </div>
        <div>
          <span>データ</span>
          <strong>
            {generationStatus === "loading-data"
              ? "取得中"
              : generationStatus === "generating"
                ? "予想中"
                : "待機中"}
          </strong>
        </div>
      </div>
      <div className="race-ai-actions">
        <button
          type="button"
          disabled={isBusy}
          onClick={() => void runAi("このレースの着順を予想してください。", "manual")}
        >
          {modelStatus === "ready" ? "AI予想を更新" : "AIを起動して予想"}
        </button>
        {error ? <span className="race-ai-error">{error}</span> : null}
      </div>
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
      {answer ? <p className="race-ai-answer">{answer}</p> : null}
      <form
        className="race-ai-chat-form"
        onSubmit={(event) => {
          event.preventDefault();
          const value = input.trim();
          if (!value || isBusy) {
            return;
          }
          setInput("");
          void runAi(value, "chat");
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
          送信
        </button>
      </form>
      <div className="race-ai-log-grid">
        <details>
          <summary>対話ログ</summary>
          <ol>
            {messages.map((message) => (
              <li key={message.id}>
                <span>{message.role === "user" ? "ユーザー" : "AI"}</span>
                <p>{message.content}</p>
              </li>
            ))}
          </ol>
        </details>
        <details>
          <summary>思考ログ</summary>
          <ol>
            {thoughtLogs.map((log) => (
              <li key={log.id}>
                <span>
                  {log.trigger} / {new Date(log.createdAt).toLocaleString("ja-JP")}
                </span>
                <p>{log.content}</p>
              </li>
            ))}
          </ol>
        </details>
      </div>
    </section>
  );
}
