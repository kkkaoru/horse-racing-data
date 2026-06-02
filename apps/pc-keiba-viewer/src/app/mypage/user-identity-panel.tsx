"use client";

// Run with: bunx vitest run src/app/mypage/user-identity-panel.test.tsx

import { useEffect, useState } from "react";

import { getOrCreateUserId, setUserId as persistUserId } from "../../lib/user-identity-indexeddb";

const USER_ID_MIN_LENGTH = 1;
const USER_ID_MAX_LENGTH = 128;
const USER_ID_PATTERN = /^[A-Za-z0-9_-]+$/u;
const SAVED_RESET_MS = 1800;

type PanelStatus = "idle" | "loading" | "saving" | "saved" | "error";

interface ValidationResult {
  ok: boolean;
  message: string | null;
}

const validateUserId = (raw: string): ValidationResult => {
  const trimmed = raw.trim();
  if (trimmed.length < USER_ID_MIN_LENGTH) {
    return { ok: false, message: "1文字以上で入力してください。" };
  }
  if (trimmed.length > USER_ID_MAX_LENGTH) {
    return { ok: false, message: "128文字以内で入力してください。" };
  }
  if (!USER_ID_PATTERN.test(trimmed)) {
    return {
      ok: false,
      message: "英数字、 ハイフン、 アンダースコアのみ使用できます。",
    };
  }
  return { ok: true, message: null };
};

const statusLabel = (status: PanelStatus): string => {
  if (status === "loading") {
    return "読み込み中";
  }
  if (status === "saving") {
    return "保存中";
  }
  if (status === "saved") {
    return "保存済み";
  }
  if (status === "error") {
    return "エラー";
  }
  return "未保存";
};

export function UserIdentityPanel() {
  const [currentUserId, setCurrentUserId] = useState<string>("");
  const [draft, setDraft] = useState<string>("");
  const [status, setStatus] = useState<PanelStatus>("loading");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const userId = await getOrCreateUserId();
        if (cancelled) {
          return;
        }
        setCurrentUserId(userId);
        setDraft(userId);
        setStatus("idle");
      } catch (caught) {
        if (cancelled) {
          return;
        }
        setErrorMessage(caught instanceof Error ? caught.message : String(caught));
        setStatus("error");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const persistDraft = async (nextValue: string): Promise<void> => {
    setStatus("saving");
    setErrorMessage(null);
    try {
      await persistUserId(nextValue);
      setCurrentUserId(nextValue);
      setDraft(nextValue);
      setStatus("saved");
      window.setTimeout(() => {
        setStatus("idle");
      }, SAVED_RESET_MS);
    } catch (caught) {
      setErrorMessage(caught instanceof Error ? caught.message : String(caught));
      setStatus("error");
    }
  };

  const handleSave = (): void => {
    const validation = validateUserId(draft);
    if (!validation.ok) {
      setErrorMessage(validation.message);
      setStatus("error");
      return;
    }
    void persistDraft(draft.trim());
  };

  const handleRegenerate = (): void => {
    const next = crypto.randomUUID();
    void persistDraft(next);
  };

  const disabled = status === "loading" || status === "saving";

  return (
    <section className="mypage-user-identity-panel" aria-label="ユーザー識別子">
      <div className="section-heading compact">
        <h2>ユーザー識別子</h2>
        <span>{statusLabel(status)}</span>
      </div>
      <p className="mypage-user-identity-note">
        IDは localStorage や IndexedDB のような自分のブラウザ内のみに保存され、
        サーバには送信されません (paddock 評価更新時を除く)。
      </p>
      <dl className="mypage-user-identity-current">
        <dt>現在の ID</dt>
        <dd>
          <code>{currentUserId || "未設定"}</code>
        </dd>
      </dl>
      <label className="mypage-user-identity-field">
        <span>新しい ID</span>
        <input
          type="text"
          value={draft}
          disabled={disabled}
          maxLength={USER_ID_MAX_LENGTH}
          onChange={(event) => {
            setDraft(event.currentTarget.value);
            setStatus("idle");
            setErrorMessage(null);
          }}
        />
      </label>
      <div className="mypage-user-identity-actions">
        <button type="button" disabled={disabled} onClick={handleSave}>
          保存
        </button>
        <button type="button" disabled={disabled} onClick={handleRegenerate}>
          再生成
        </button>
      </div>
      {errorMessage ? <p className="mypage-user-identity-error">{errorMessage}</p> : null}
    </section>
  );
}
