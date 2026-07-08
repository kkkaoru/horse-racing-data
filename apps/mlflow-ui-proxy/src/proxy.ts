// Run with bun.
import type { Env } from "./types";

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

const METHODS_WITHOUT_BODY = new Set(["GET", "HEAD"]);

const BAD_GATEWAY_STATUS = 502;
const BAD_GATEWAY_BODY = "Bad Gateway: unable to reach MLflow origin";
const PROXIED_BY_HEADER_NAME = "X-Proxied-By";
const PROXIED_BY_HEADER_VALUE = "mlflow-ui-proxy";

const shouldForwardHeader = (name: string): boolean => !HOP_BY_HOP_HEADERS.has(name.toLowerCase());

const copyForwardableHeaders = (source: Headers): Headers => {
  const entries = Array.from(source.entries()).filter(([name]) => shouldForwardHeader(name));
  return new Headers(entries);
};

const buildUpstreamUrl = (requestUrl: string, origin: string): string => {
  const parsed = new URL(requestUrl);
  return new URL(parsed.pathname + parsed.search, origin).toString();
};

const buildUpstreamInit = (request: Request, headers: Headers): UpstreamRequestInit => {
  if (METHODS_WITHOUT_BODY.has(request.method)) {
    return { method: request.method, headers };
  }
  // Streamed through unbuffered (not read into memory) — see the
  // UpstreamRequestInit comment above for why `duplex: "half"` is needed.
  return { method: request.method, headers, body: request.body, duplex: "half" };
};

export const buildUpstreamRequest = (request: Request, origin: string): Request => {
  const url = buildUpstreamUrl(request.url, origin);
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

export const proxyRequest = async (request: Request, env: Env): Promise<Response> => {
  const upstreamRequest = buildUpstreamRequest(request, env.MLFLOW_ORIGIN);
  try {
    const upstreamResponse = await fetch(upstreamRequest);
    return buildUpstreamResponse(upstreamResponse);
  } catch {
    return buildBadGatewayResponse();
  }
};
