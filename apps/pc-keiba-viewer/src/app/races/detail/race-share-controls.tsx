"use client";

import { useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";
import { createPortal } from "react-dom";

const RESET_DELAY_MS = 1800;
const PRODUCTION_HOST = "pc-keiba-viewer.kkk4oru.com";

function CopyUrlButton({
  copied,
  label,
  onCopy,
  shareUrl,
}: {
  copied: boolean;
  label: string;
  onCopy: () => void;
  shareUrl: string;
}) {
  return (
    <button
      aria-label="共有URLをコピー"
      className="race-share-button"
      title={shareUrl}
      type="button"
      onClick={onCopy}
    >
      <span className="race-share-button-icon" aria-hidden="true" />
      <span>{copied ? "コピー済み" : label}</span>
    </button>
  );
}

export function RaceShareControls({ path }: { path: string }) {
  const searchParams = useSearchParams();
  const [copied, setCopied] = useState(false);
  const [hiddenFloating, setHiddenFloating] = useState(false);
  const [isProductionHost] = useState(
    () => typeof window !== "undefined" && window.location.hostname === PRODUCTION_HOST,
  );
  const [headerNav, setHeaderNav] = useState<Element | null>(null);
  const queryString = searchParams.toString();
  const shareUrl = `https://pc-keiba-viewer.kkk4oru.com${path}${queryString ? `?${queryString}` : ""}`;

  useEffect(() => {
    if (isProductionHost) {
      return;
    }

    setHeaderNav(document.querySelector(".header-menu nav"));
  }, [isProductionHost]);

  if (isProductionHost) {
    return null;
  }

  const copyUrl = () => {
    void (async () => {
      try {
        await navigator.clipboard.writeText(shareUrl);
        setCopied(true);
        window.setTimeout(() => {
          setCopied(false);
        }, RESET_DELAY_MS);
      } catch {
        setCopied(false);
      }
    })();
  };

  return (
    <>
      {headerNav
        ? createPortal(
            <CopyUrlButton
              copied={copied}
              label="共有URLコピー"
              shareUrl={shareUrl}
              onCopy={copyUrl}
            />,
            headerNav,
          )
        : null}
      {hiddenFloating ? null : (
        <aside className="race-share-floating" aria-label="race share url">
          <button
            aria-label="共有URLコピーを閉じる"
            className="race-share-floating-close"
            type="button"
            onClick={() => {
              setHiddenFloating(true);
            }}
          >
            <span aria-hidden="true" />
          </button>
          <span className="race-share-floating-label">共有</span>
          <CopyUrlButton copied={copied} label="URLコピー" shareUrl={shareUrl} onCopy={copyUrl} />
        </aside>
      )}
    </>
  );
}
