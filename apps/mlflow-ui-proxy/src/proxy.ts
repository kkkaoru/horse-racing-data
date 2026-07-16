// Run with bun.
import type { MlflowContainerNamespace } from "./types";

export interface ProxyEnv {
  MLFLOW_CONTAINER: MlflowContainerNamespace;
}

// `RequestInit` in @cloudflare/workers-types has no `duplex` field because
// the Workers runtime (workerd) never required it, but the Node/undici fetch
// used by this package's own vitest run enforces the newer Fetch spec rule
// that a streaming (ReadableStream) request body requires `duplex: "half"`.
// This extends RequestInit locally so the option can be passed through
// without an `as`/`any` cast, keeping the client body streamed rather than
// buffered into memory (relevant for large MLflow artifact uploads).
interface UpstreamRequestInit extends RequestInit {
  duplex?: "half";
}

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "host",
  "authorization",
]);

// MLflow 3.x's server rejects mutating requests whose `Origin` (and, by the
// same check, `Referer`) header doesn't match its own host with a 403
// "Cross-origin request blocked" response. The browser always sends this
// Worker's own origin, which never matches the MLflow origin, so these
// headers must be stripped for every proxied request to reach MLflow at all.
const BROWSER_CONTEXT_HEADERS = new Set(["origin", "referer"]);

const METHODS_WITHOUT_BODY = new Set(["GET", "HEAD"]);

const BAD_GATEWAY_STATUS = 502;
const BAD_GATEWAY_BODY = "Bad Gateway: unable to reach MLflow origin";
const PROXIED_BY_HEADER_NAME = "X-Proxied-By";
const PROXIED_BY_HEADER_VALUE = "mlflow-ui-proxy";
const MLFLOW_CONTAINER_NAME = "primary";
const CONTAINER_ORIGIN = "http://mlflow-container";

const shouldForwardHeader = (name: string): boolean => {
  const lowerName = name.toLowerCase();
  return !HOP_BY_HOP_HEADERS.has(lowerName) && !BROWSER_CONTEXT_HEADERS.has(lowerName);
};

const copyForwardableHeaders = (source: Headers): Headers => {
  const entries = Array.from(source.entries()).filter(([name]) => shouldForwardHeader(name));
  return new Headers(entries);
};

const buildUpstreamUrl = (requestUrl: string): string => {
  const parsed = new URL(requestUrl);
  return new URL(parsed.pathname + parsed.search, CONTAINER_ORIGIN).toString();
};

const buildUpstreamInit = (request: Request, headers: Headers): UpstreamRequestInit => {
  if (METHODS_WITHOUT_BODY.has(request.method)) {
    return { method: request.method, headers };
  }
  // Streamed through unbuffered (not read into memory) — see the
  // UpstreamRequestInit comment above for why `duplex: "half"` is needed.
  return { method: request.method, headers, body: request.body, duplex: "half" };
};

export const buildUpstreamRequest = (request: Request): Request => {
  const url = buildUpstreamUrl(request.url);
  const headers = copyForwardableHeaders(request.headers);
  return new Request(url, buildUpstreamInit(request, headers));
};

const buildUpstreamResponse = (upstreamResponse: Response): Response => {
  const headers = copyForwardableHeaders(upstreamResponse.headers);
  headers.set(PROXIED_BY_HEADER_NAME, PROXIED_BY_HEADER_VALUE);
  return new Response(upstreamResponse.body, {
    status: upstreamResponse.status,
    statusText: upstreamResponse.statusText,
    headers,
  });
};

const buildBadGatewayResponse = (): Response =>
  new Response(BAD_GATEWAY_BODY, { status: BAD_GATEWAY_STATUS });

export const proxyRequest = async (request: Request, env: ProxyEnv): Promise<Response> => {
  const upstreamRequest = buildUpstreamRequest(request);
  const container = env.MLFLOW_CONTAINER.getByName(MLFLOW_CONTAINER_NAME);
  try {
    const upstreamResponse = await container.fetch(upstreamRequest);
    return buildUpstreamResponse(upstreamResponse);
  } catch (error) {
    console.error(`[mlflow-proxy] container fetch failed: ${String(error)}`);
    return buildBadGatewayResponse();
  }
};
