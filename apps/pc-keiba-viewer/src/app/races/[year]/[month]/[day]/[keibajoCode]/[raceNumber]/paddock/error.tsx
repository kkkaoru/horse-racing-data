"use client";

// Graceful fallback for paddock-edit SSR failures (e.g. Hyperdrive timeout
// during a cold cache hit). Without this file Next.js bubbles up the
// global error boundary, which renders an unstyled error page. Showing a
// retry control keeps the user inside the race context.

import Link from "next/link";
import { useEffect } from "react";

interface PaddockErrorProps {
  error: Error & { digest?: string };
  reset: () => void;
}

export default function PaddockError({ error, reset }: PaddockErrorProps) {
  useEffect(() => {
    // biome-ignore lint/suspicious/noConsole: surface paddock SSR failures in client logs
    console.error("paddock-edit page failed to render", error);
  }, [error]);

  return (
    <main className="page-shell paddock-edit-error" aria-live="assertive">
      <section className="paddock-edit-error-card">
        <h1>パドック編集ページを表示できませんでした</h1>
        <p>
          一時的な通信エラーが起きた可能性があります。再読み込みすると表示されることがあります。
        </p>
        <div className="paddock-edit-error-actions">
          <button className="paddock-edit-link" onClick={reset} type="button">
            再読み込み
          </button>
          <Link className="paddock-edit-link" href="..">
            レース詳細へ戻る
          </Link>
        </div>
        {error.digest ? (
          <p className="paddock-edit-error-digest">エラーID: {error.digest}</p>
        ) : null}
      </section>
    </main>
  );
}
