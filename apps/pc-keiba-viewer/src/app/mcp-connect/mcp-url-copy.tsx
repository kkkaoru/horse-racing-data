"use client";

import { useEffect, useState } from "react";

import { displayedMcpUrl } from "../../lib/mcp-oauth-origin";
import { McpCopyRow } from "./mcp-copy-row";

interface McpUrlCopyProps {
  mcpUrl: string;
}

export function McpUrlCopy({ mcpUrl }: McpUrlCopyProps) {
  const [url, setUrl] = useState(mcpUrl);
  useEffect(() => {
    setUrl(displayedMcpUrl(mcpUrl, window.location.origin));
  }, [mcpUrl]);
  return <McpCopyRow value={url} />;
}
