// Run with bun.

export interface McpProxyConfig {
  accessClientId: string;
  accessClientSecret: string;
  mcpAuthToken: string;
  mcpUrl: string;
}

export interface McpProxyEnv {
  MCP_AUTH_TOKEN?: string;
  PC_KEIBA_ACCESS_CLIENT_ID?: string;
  PC_KEIBA_ACCESS_CLIENT_SECRET?: string;
  PC_KEIBA_VIEWER_MCP_URL?: string;
}

const HTTPS_PROTOCOL: string = "https:";
const USER_AGENT: string = "pc-keiba-viewer-plugin/1.0";
const CONTENT_LENGTH_HEADER: string = "Content-Length:";

export const loadMcpProxyConfig = (env: McpProxyEnv): McpProxyConfig | string => {
  const mcpUrl = env.PC_KEIBA_VIEWER_MCP_URL?.trim() ?? "";
  const mcpAuthToken = env.MCP_AUTH_TOKEN?.trim() ?? "";
  const accessClientId = env.PC_KEIBA_ACCESS_CLIENT_ID?.trim() ?? "";
  const accessClientSecret = env.PC_KEIBA_ACCESS_CLIENT_SECRET?.trim() ?? "";
  if (mcpUrl.length === 0) {
    return "PC_KEIBA_VIEWER_MCP_URL is required (absolute https URL ending with /mcp)";
  }
  if (mcpAuthToken.length === 0) {
    return "MCP_AUTH_TOKEN is required";
  }
  if (accessClientId.length === 0) {
    return "PC_KEIBA_ACCESS_CLIENT_ID is required";
  }
  if (accessClientSecret.length === 0) {
    return "PC_KEIBA_ACCESS_CLIENT_SECRET is required";
  }
  try {
    const url = new URL(mcpUrl);
    if (url.protocol !== HTTPS_PROTOCOL) {
      return "PC_KEIBA_VIEWER_MCP_URL must be https";
    }
    if (url.username.length > 0 || url.password.length > 0) {
      return "PC_KEIBA_VIEWER_MCP_URL must not include userinfo";
    }
    if (url.hash.length > 0) {
      return "PC_KEIBA_VIEWER_MCP_URL must not include a fragment";
    }
    if (!url.pathname.endsWith("/mcp")) {
      return "PC_KEIBA_VIEWER_MCP_URL path must end with /mcp";
    }
  } catch {
    return "PC_KEIBA_VIEWER_MCP_URL is not a valid URL";
  }
  return {
    accessClientId,
    accessClientSecret,
    mcpAuthToken,
    mcpUrl,
  };
};

export const extractStdioMessages = (buffer: string): { messages: string[]; rest: string } => {
  const messages: string[] = [];
  let rest = buffer;
  while (rest.length > 0) {
    const trimmed = rest.trimStart();
    if (trimmed.length !== rest.length) {
      rest = trimmed;
      continue;
    }
    if (rest.startsWith("{")) {
      const newlineIndex = rest.indexOf("\n");
      if (newlineIndex < 0) {
        return { messages, rest };
      }
      const line = rest.slice(0, newlineIndex).trim();
      rest = rest.slice(newlineIndex + 1);
      messages.push(line);
      continue;
    }
    const headerIndex = rest.indexOf(CONTENT_LENGTH_HEADER);
    if (headerIndex < 0) {
      return { messages, rest };
    }
    const afterHeader = rest.slice(headerIndex + CONTENT_LENGTH_HEADER.length);
    const headerEnd = afterHeader.indexOf("\r\n\r\n");
    if (headerEnd < 0) {
      return { messages, rest };
    }
    const lengthText = afterHeader.slice(0, afterHeader.indexOf("\r\n")).trim();
    const bodyLength = Number(lengthText);
    if (!Number.isInteger(bodyLength) || bodyLength < 0) {
      return { messages, rest: afterHeader.slice(headerEnd + 4) };
    }
    const bodyStart = headerIndex + CONTENT_LENGTH_HEADER.length + headerEnd + 4;
    if (rest.length < bodyStart + bodyLength) {
      return { messages, rest };
    }
    messages.push(rest.slice(bodyStart, bodyStart + bodyLength));
    rest = rest.slice(bodyStart + bodyLength);
  }
  return { messages, rest };
};

export const encodeStdioMessage = (body: string): string =>
  `${CONTENT_LENGTH_HEADER} ${Buffer.byteLength(body, "utf8")}\r\n\r\n${body}`;

export const forwardMcpJsonRpc = async (
  config: McpProxyConfig,
  body: string,
  fetchImpl: typeof fetch,
): Promise<string | null> => {
  const response = await fetchImpl(config.mcpUrl, {
    body,
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${config.mcpAuthToken}`,
      "CF-Access-Client-Id": config.accessClientId,
      "CF-Access-Client-Secret": config.accessClientSecret,
      "Content-Type": "application/json",
      "User-Agent": USER_AGENT,
    },
    method: "POST",
  });
  if (response.status === 202) {
    return null;
  }
  const text = await response.text();
  if (!response.ok) {
    return JSON.stringify({
      error: {
        code: -32000,
        message: `Remote MCP HTTP ${response.status}`,
      },
      id: null,
      jsonrpc: "2.0",
    });
  }
  if (text.trim().length === 0) {
    return null;
  }
  return text;
};
