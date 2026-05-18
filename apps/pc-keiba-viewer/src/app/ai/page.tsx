import type { Metadata } from "next";

import { AiPlayground } from "./ai-playground";

export const metadata: Metadata = {
  title: "AI動作確認",
};

export default function AiPage() {
  return (
    <div className="page-shell ai-playground-page">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">WebGPU AI</p>
          <h1>AI動作確認</h1>
        </div>
      </div>
      <AiPlayground />
    </div>
  );
}
