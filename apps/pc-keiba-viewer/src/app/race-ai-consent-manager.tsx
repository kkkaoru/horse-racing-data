"use client";

import { useEffect, useRef, useState } from "react";

import {
  downloadRaceAiModel,
  getRaceAiModelState,
  LATEST_RACE_AI_MODEL,
  subscribeRaceAiModelDownloads,
  type RaceAiModelState,
} from "./races/detail/race-ai-model-manager";
import {
  getRaceAiSettings,
  requestRaceAiConsent,
  subscribeRaceAiSettings,
  type RaceAiSettings,
} from "./races/detail/race-ai-storage";

const isWebGpuSupported = (): boolean => typeof navigator !== "undefined" && "gpu" in navigator;

export function RaceAiConsentManager() {
  const [settings, setSettings] = useState<RaceAiSettings | null>(null);
  const [modelState, setModelState] = useState<RaceAiModelState | null>(null);
  const [supported, setSupported] = useState(false);
  const [dismissedError, setDismissedError] = useState(false);
  const promptedRef = useRef(false);

  useEffect(() => {
    const nextSupported = isWebGpuSupported();
    setSupported(nextSupported);
    if (!nextSupported) {
      return undefined;
    }

    const refreshModelState = () => {
      void getRaceAiModelState(LATEST_RACE_AI_MODEL).then(setModelState);
    };
    setSettings(getRaceAiSettings());
    refreshModelState();
    const unsubscribeSettings = subscribeRaceAiSettings(setSettings);
    const unsubscribeDownloads = subscribeRaceAiModelDownloads(refreshModelState);
    return () => {
      unsubscribeSettings();
      unsubscribeDownloads();
    };
  }, []);

  useEffect(() => {
    if (!supported || !settings || promptedRef.current || settings.consent !== "unanswered") {
      return;
    }
    promptedRef.current = true;
    const nextSettings = requestRaceAiConsent();
    setSettings(nextSettings);
    if (nextSettings.consent === "granted") {
      setDismissedError(false);
      void downloadRaceAiModel(LATEST_RACE_AI_MODEL).catch(() => {});
    }
  }, [settings, supported]);

  const downloadError = modelState?.status === "not-downloaded" ? modelState.error : null;

  if (!supported) {
    return null;
  }

  if (downloadError && !dismissedError) {
    return (
      <dialog className="race-ai-download-dialog" open>
        <div>
          <span>AIモデルのダウンロードに失敗しました</span>
        </div>
        <small>{downloadError}</small>
        <div className="race-ai-download-dialog-actions">
          <button
            type="button"
            onClick={() => {
              setDismissedError(false);
              void downloadRaceAiModel(LATEST_RACE_AI_MODEL).catch(() => {});
            }}
          >
            再試行
          </button>
          <button type="button" onClick={() => setDismissedError(true)}>
            閉じる
          </button>
        </div>
      </dialog>
    );
  }

  if (modelState?.status !== "downloading") {
    return null;
  }

  const progressLabel =
    modelState.progress === null ? "取得中" : `${Math.round(modelState.progress * 100)}%`;

  return (
    <dialog className="race-ai-download-dialog" open>
      <div>
        <span>AIモデルをダウンロード中</span>
        <strong>{progressLabel}</strong>
      </div>
      <progress value={modelState.progress ?? undefined} max={1} />
      <small>完了後、このブラウザ内に保存されます。サイト内を移動しても中止されません。</small>
    </dialog>
  );
}
