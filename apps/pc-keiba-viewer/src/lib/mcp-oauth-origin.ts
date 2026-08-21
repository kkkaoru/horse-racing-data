// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

export const originFromRequestUrl = (requestUrl: string): string => new URL(requestUrl).origin;

export const originFromForwardedHeaders = (headerMap: Headers): string | null => {
  const host = headerMap.get("x-forwarded-host") ?? headerMap.get("host");
  if (host === null || host.trim().length === 0) {
    return null;
  }
  const protoHeader = headerMap.get("x-forwarded-proto");
  const proto = protoHeader === "http" ? "http" : "https";
  return `${proto}://${host.trim()}`;
};

export const mcpResourceUrl = (origin: string): string => `${origin}/mcp`;

export const displayedMcpUrl = (serverMcpUrl: string, browserOrigin: string | null): string => {
  if (serverMcpUrl.startsWith("https://") || serverMcpUrl.startsWith("http://")) {
    return serverMcpUrl;
  }
  if (browserOrigin === null || browserOrigin.trim().length === 0) {
    return "/mcp";
  }
  const origin = browserOrigin.trim();
  const withoutSlash = origin.endsWith("/") ? origin.slice(0, -1) : origin;
  return mcpResourceUrl(withoutSlash);
};

export const isCanonicalMcpResource = (origin: string, resource: string): boolean => {
  const expected = mcpResourceUrl(origin);
  return resource === expected || resource === origin;
};

export const normalizeMcpResource = (origin: string, resource: string): string | null =>
  isCanonicalMcpResource(origin, resource) ? mcpResourceUrl(origin) : null;
