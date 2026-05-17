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

const statusLabel = (status: RaceAiModelState["status"]): string => {
  if (status === "downloaded") {
    return "ダウンロード済み";
  }
  if (status === "downloading") {
    return "ダウンロード中";
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

const isWebGpuSupported = (): boolean => typeof navigator !== "undefined" && "gpu" in navigator;

export function RaceAiSettingsPanel() {
  const [settings, setSettings] = useState<RaceAiSettings | null>(null);
  const [modelStates, setModelStates] = useState<RaceAiModelState[]>([]);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [supported, setSupported] = useState(false);

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

  const enablePermission = () => {
    const nextSettings = requestRaceAiConsent();
    setSettings(nextSettings);
    if (nextSettings.consent === "granted") {
      void downloadRaceAiModel().catch(() => {});
    }
  };

  const disablePermission = () => {
    const nextSettings = saveRaceAiSettings({ autoStart: false, consent: "denied" });
    setSettings(nextSettings);
  };

  const toggleAutoStart = (checked: boolean) => {
    if (!checked) {
      const nextSettings = saveRaceAiSettings({
        autoStart: false,
        consent: settings?.consent ?? "denied",
      });
      setSettings(nextSettings);
      return;
    }
    if (settings?.consent !== "granted") {
      enablePermission();
      return;
    }
    const nextSettings = saveRaceAiSettings({ autoStart: true, consent: "granted" });
    setSettings(nextSettings);
  };

  const runModelAction = (state: RaceAiModelState) => {
    if (state.status === "downloaded") {
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
        <span>{supported ? "WebGPU対応" : "WebGPU非対応"}</span>
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
              <span>AI利用を許可</span>
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
          <div className="mypage-ai-model-list" aria-label="AIモデル一覧">
            {modelStates.length === 0
              ? RACE_AI_MODELS.map((model) => (
                  <div className="mypage-ai-model-row" key={model.id}>
                    <strong>{model.name}</strong>
                    <span>{model.version}</span>
                    <span>確認中</span>
                  </div>
                ))
              : modelStates.map((state) => (
                  <div className="mypage-ai-model-row" key={state.model.id}>
                    <span>
                      <strong>{state.model.name}</strong>
                      {state.model.isLatest ? <small>最新</small> : null}
                    </span>
                    <span>{state.model.version}</span>
                    <span>{formatRaceAiModelSize(state.totalBytes)}</span>
                    <span>{statusLabel(state.status)}</span>
                    <span>{progressLabel(state)}</span>
                    <button type="button" onClick={() => runModelAction(state)}>
                      {state.status === "downloaded"
                        ? "削除"
                        : state.status === "downloading"
                          ? "中止"
                          : "ダウンロード"}
                    </button>
                  </div>
                ))}
          </div>
        </>
      )}
    </details>
  );
}
