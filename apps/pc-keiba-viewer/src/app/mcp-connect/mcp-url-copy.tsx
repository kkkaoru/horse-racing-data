"use client";

import { useEffect, useState } from "react";

import { displayedMcpUrl } from "../../lib/mcp-oauth-origin";

interface McpUrlCopyProps {
  mcpUrl: string;
}

export function McpUrlCopy({ mcpUrl }: McpUrlCopyProps) {
  const [url, setUrl] = useState(mcpUrl);
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    setUrl(displayedMcpUrl(mcpUrl, window.location.origin));
  }, [mcpUrl]);
  const label = copied ? "コピー済み" : "コピー";
  return (
    <div className="mcp-connect-url-row">
      <code>{url}</code>
      <button
        type="button"
        onClick={() => {
          void navigator.clipboard.writeText(url).then(() => {
            setCopied(true);
            return undefined;
          });
        }}
      >
        {label}
      </button>
    </div>
  );
}
