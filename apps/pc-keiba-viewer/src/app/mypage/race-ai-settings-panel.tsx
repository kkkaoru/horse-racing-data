"use client";

import { useEffect, useState } from "react";

import {
  abortRaceAiModelDownload,
  deleteRaceAiModel,
  downloadRaceAiModel,
  formatRaceAiModelSize,
  getRaceAiModelStates,
  RACE_AI_MODELS,
  subscribeRaceAiModelDownloads,
  type RaceAiModelState,
} from "../races/detail/race-ai-model-manager";
import {
  getRaceAiSettings,
  RACE_AI_MODEL_DOWNLOAD_CONFIRM_MESSAGE,
  requestRaceAiConsent,
  saveRaceAiSettings,
  subscribeRaceAiSettings,
  type RaceAiSettings,
} from "../races/detail/race-ai-storage";

const DELETE_MODEL_CONFIRM_MESSAGE = "ダウンロード済みのAIモデルをこのブラウザから削除しますか？";
const ABORT_MODEL_CONFIRM_MESSAGE = "AIモデルのダウンロードを中止しますか？";
const TONE_PROMPT_MAX_LENGTH = 4_000;

type TonePromptStatus = "idle" | "loading" | "saved" | "saving";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const readTonePromptFromPayload = (payload: unknown): string => {
  if (!isRecord(payload) || typeof payload.prompt !== "string") {
    return "";
  }
  return payload.prompt;
};

const statusLabel = (state: RaceAiModelState): string => {
  if (state.status === "downloaded") {
    return "ダウンロード済み";
  }
  if (state.status === "downloading") {
    return "ダウンロード中";
  }
  if (state.cachedAt) {
    return "保存済み / 利用不可";
  }
  return "未ダウンロード";
};

const progressLabel = (state: RaceAiModelState): string => {
  if (state.status === "downloaded") {
    return "100%";
  }
  if (state.status !== "downloading") {
    return "-";
  }
  return state.progress === null ? "取得中" : `${Math.round(state.progress * 100)}%`;
};

const formatModelBytes = (bytes: number | null): string => {
  if (bytes === null) {
    return "-";
  }
  if (bytes <= 0) {
    return "0 B";
  }
  return formatRaceAiModelSize(bytes);
};

const modelDownloadedLabel = (state: RaceAiModelState): string =>
  `${formatModelBytes(state.downloadedBytes)} / ${formatModelBytes(state.totalBytes)}`;

const attemptLabel = (state: RaceAiModelState): string | null =>
  state.attempt && state.maxAttempts ? `試行 ${state.attempt}/${state.maxAttempts}` : null;

const isCachedModelDeleteAction = (state: RaceAiModelState): boolean =>
  state.status === "downloaded" || (state.status !== "downloading" && state.cachedAt !== null);

const isWebGpuSupported = (): boolean => typeof navigator !== "undefined" && "gpu" in navigator;

export function RaceAiSettingsPanel() {
  const [settings, setSettings] = useState<RaceAiSettings | null>(null);
  const [modelStates, setModelStates] = useState<RaceAiModelState[]>([]);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [supported, setSupported] = useState(false);
  const [tonePrompt, setTonePrompt] = useState("");
  const [tonePromptDraft, setTonePromptDraft] = useState("");
  const [tonePromptError, setTonePromptError] = useState<string | null>(null);
  const [tonePromptStatus, setTonePromptStatus] = useState<TonePromptStatus>("loading");

  useEffect(() => {
    setSupported(isWebGpuSupported());
    const initialSettings = getRaceAiSettings();
    setSettings(initialSettings);
    setSettingsOpen(initialSettings.consent === "granted" && initialSettings.autoStart);
    const refreshModelStates = () => {
      void getRaceAiModelStates().then(setModelStates);
    };
    refreshModelStates();
    const unsubscribeSettings = subscribeRaceAiSettings(setSettings);
    const unsubscribeModels = subscribeRaceAiModelDownloads(refreshModelStates);
    return () => {
      unsubscribeSettings();
      unsubscribeModels();
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setTonePromptStatus("loading");
    void (async () => {
      try {
        const response = await fetch("/api/race-ai/tone-prompt", { cache: "no-store" });
        if (!response.ok) {
          throw new Error("口調プロンプトを読み込めませんでした。");
        }
        const payload: unknown = await response.json();
        if (cancelled) {
          return;
        }
        const nextTonePrompt = readTonePromptFromPayload(payload);
        setTonePrompt(nextTonePrompt);
        setTonePromptDraft(nextTonePrompt);
        setTonePromptError(null);
        setTonePromptStatus("idle");
      } catch (caught) {
        if (cancelled) {
          return;
        }
        setTonePromptError(caught instanceof Error ? caught.message : String(caught));
        setTonePromptStatus("idle");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const enablePermission = () => {
    const nextSettings = requestRaceAiConsent();
    setSettings(nextSettings);
    if (nextSettings.consent === "granted") {
      void downloadRaceAiModel().catch(() => {});
    }
  };

  const disablePermission = () => {
    const nextSettings = saveRaceAiSettings({
      autoStart: false,
      consent: "denied",
      showSystemMessages: settings?.showSystemMessages ?? false,
    });
    setSettings(nextSettings);
  };

  const toggleAutoStart = (checked: boolean) => {
    if (!checked) {
      const nextSettings = saveRaceAiSettings({
        autoStart: false,
        consent: settings?.consent ?? "denied",
        showSystemMessages: settings?.showSystemMessages ?? false,
      });
      setSettings(nextSettings);
      return;
    }
    if (settings?.consent !== "granted") {
      enablePermission();
      return;
    }
    const nextSettings = saveRaceAiSettings({
      autoStart: true,
      consent: "granted",
      showSystemMessages: settings.showSystemMessages,
    });
    setSettings(nextSettings);
  };

  const runModelAction = (state: RaceAiModelState) => {
    if (isCachedModelDeleteAction(state)) {
      if (window.confirm(DELETE_MODEL_CONFIRM_MESSAGE)) {
        void deleteRaceAiModel(state.model);
      }
      return;
    }
    if (state.status === "downloading") {
      if (window.confirm(ABORT_MODEL_CONFIRM_MESSAGE)) {
        abortRaceAiModelDownload(state.model);
      }
      return;
    }
    if (window.confirm(RACE_AI_MODEL_DOWNLOAD_CONFIRM_MESSAGE)) {
      void downloadRaceAiModel(state.model).catch(() => {});
    }
  };

  const saveTonePrompt = () => {
    const nextTonePrompt = tonePromptDraft.trim();
    if (nextTonePrompt.length > TONE_PROMPT_MAX_LENGTH) {
      setTonePromptError("口調プロンプトは4000文字以内で入力してください。");
      return;
    }
    setTonePromptStatus("saving");
    setTonePromptError(null);
    void (async () => {
      try {
        const response = await fetch("/api/race-ai/tone-prompt", {
          body: JSON.stringify({ prompt: nextTonePrompt }),
          headers: { "content-type": "application/json" },
          method: "PUT",
        });
        if (!response.ok) {
          throw new Error("口調プロンプトを保存できませんでした。");
        }
        const payload: unknown = await response.json();
        const savedTonePrompt = readTonePromptFromPayload(payload);
        setTonePrompt(savedTonePrompt);
        setTonePromptDraft(savedTonePrompt);
        setTonePromptStatus("saved");
      } catch (caught) {
        setTonePromptError(caught instanceof Error ? caught.message : String(caught));
        setTonePromptStatus("idle");
      }
    })();
  };

  const tonePromptChanged = tonePromptDraft.trim() !== tonePrompt.trim();
  const canSaveTonePrompt = tonePromptStatus !== "loading" && tonePromptStatus !== "saving";

  return (
    <details
      className="mypage-ai-panel"
      open={settingsOpen}
      onToggle={(event) => {
        setSettingsOpen(event.currentTarget.open);
      }}
    >
      <summary className="section-heading compact">
        <h2>AI利用設定</h2>
        <span>
          {supported ? "WebGPU対応" : "WebGPU非対応"} / {settingsOpen ? "閉じる" : "表示する"}
        </span>
      </summary>
      {!supported ? (
        <p className="empty-state">このブラウザではWebGPU AI予想は利用できません。</p>
      ) : (
        <>
          <div className="mypage-ai-settings-grid">
            <label>
              <input
                type="checkbox"
                checked={settings?.consent === "granted"}
                onChange={(event) => {
                  if (event.currentTarget.checked) {
                    enablePermission();
                  } else {
                    disablePermission();
                  }
                }}
              />
              <span>ブラウザでのAI利用を許可</span>
            </label>
            <label>
              <input
                type="checkbox"
                checked={settings?.consent === "granted" && settings.autoStart}
                onChange={(event) => toggleAutoStart(event.currentTarget.checked)}
              />
              <span>レース詳細でAIを自動開始</span>
            </label>
          </div>
        </>
      )}
      <section className="mypage-ai-tone-panel" aria-label="AI口調プロンプト">
        <div>
          <h3>AI口調プロンプト</h3>
          <span>
            {tonePromptStatus === "loading"
              ? "読み込み中"
              : tonePromptStatus === "saving"
                ? "保存中"
                : tonePromptStatus === "saved"
                  ? "保存済み"
                  : tonePromptChanged
                    ? "未保存"
                    : "保存済み"}
          </span>
        </div>
        <textarea
          value={tonePromptDraft}
          maxLength={TONE_PROMPT_MAX_LENGTH}
          rows={6}
          onChange={(event) => {
            setTonePromptDraft(event.currentTarget.value);
            setTonePromptStatus("idle");
          }}
        />
        <div className="mypage-ai-tone-actions">
          <button type="button" disabled={!canSaveTonePrompt} onClick={saveTonePrompt}>
            保存
          </button>
          <button
            type="button"
            disabled={!canSaveTonePrompt || !tonePromptDraft}
            onClick={() => {
              setTonePromptDraft("");
              setTonePromptStatus("idle");
            }}
          >
            空にする
          </button>
          <span>
            {tonePromptDraft.length.toLocaleString("ja-JP")} /{" "}
            {TONE_PROMPT_MAX_LENGTH.toLocaleString("ja-JP")}
          </span>
        </div>
        {tonePromptError ? <p>{tonePromptError}</p> : null}
      </section>
      {supported ? (
        <div className="mypage-ai-model-list" aria-label="AIモデル一覧">
          {modelStates.length === 0
            ? RACE_AI_MODELS.map((model) => (
                <div className="mypage-ai-model-row" key={model.id}>
                  <strong>{model.name}</strong>
                  <span data-label="バージョン">{model.version}</span>
                  <span data-label="状態">確認中</span>
                </div>
              ))
            : modelStates.map((state) => (
                <div className="mypage-ai-model-row" key={state.model.id}>
                  <span className="mypage-ai-model-name">
                    <strong>{state.model.name}</strong>
                    {state.model.isLatest ? <small>最新</small> : null}
                  </span>
                  <span data-label="バージョン">{state.model.version}</span>
                  <span data-label="サイズ">{formatRaceAiModelSize(state.totalBytes)}</span>
                  <span data-label="状態">{statusLabel(state)}</span>
                  <span className="mypage-ai-model-progress" data-label="進捗">
                    <strong>{progressLabel(state)}</strong>
                    <progress
                      value={state.status === "downloaded" ? 1 : (state.progress ?? undefined)}
                      max={1}
                    />
                    <small>
                      {modelDownloadedLabel(state)}
                      {attemptLabel(state) ? ` / ${attemptLabel(state)}` : ""}
                    </small>
                  </span>
                  <button type="button" onClick={() => runModelAction(state)}>
                    {isCachedModelDeleteAction(state)
                      ? "削除"
                      : state.status === "downloading"
                        ? "中止"
                        : "ダウンロード"}
                  </button>
                  {state.error ? <p className="mypage-ai-model-error">{state.error}</p> : null}
                </div>
              ))}
        </div>
      ) : null}
    </details>
  );
}
