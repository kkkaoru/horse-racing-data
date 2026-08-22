"use client";

import { useState } from "react";

interface McpCopyRowProps {
  value: string;
}

export function McpCopyRow({ value }: McpCopyRowProps) {
  const [copied, setCopied] = useState(false);
  const label = copied ? "コピー済み" : "コピー";
  return (
    <div className="mcp-connect-url-row">
      <code>{value}</code>
      <button
        type="button"
        onClick={() => {
          void navigator.clipboard.writeText(value).then(() => {
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
