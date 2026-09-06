// Run with bun. Cloudflare Worker and singleton Durable-Object-backed Linux Container.

import { Container, switchPort } from "@cloudflare/containers";
import { isAuthorized, unauthorizedResponse } from "./auth";

interface JvLinkSecrets {
  JRA_VAN_API_TOKEN: string;
  JRA_VAN_DATALAB_KEY: string;
  JRA_VAN_STATE_TOKEN: string;
}

interface JvLinkEnvironment extends Env, JvLinkSecrets {}

const DEFAULT_PORT: number = 8080;
const SLEEP_AFTER: string = "10m";
const CONTAINER_NAME: string = "authenticated-jvlink-terminal";
const STATE_PATH: string = "/internal/state";
const STATE_OBJECT_KEY: string = "terminal/prefix.tar.gz";
const HEALTH_PATH: string = "/health";
const BOOTSTRAP_PATH: string = "/bootstrap";
const BOOTSTRAP_START_PATH: string = "/bootstrap/start";
const BOOTSTRAP_PORT: number = 6080;
const CONTENT_TYPE_GZIP: string = "application/gzip";

const stateResponse = async (request: Request, env: JvLinkEnvironment): Promise<Response> => {
  if (!(await isAuthorized(request, env.JRA_VAN_STATE_TOKEN))) return unauthorizedResponse();
  if (request.method === "GET") {
    const object = await env.JV_LINK_STATE.get(STATE_OBJECT_KEY);
    if (object === null) return new Response(null, { status: 404 });
    return new Response(object.body, {
      headers: {
        "Content-Type": CONTENT_TYPE_GZIP,
        ETag: object.httpEtag,
      },
    });
  }
  if (request.method !== "PUT") return new Response(null, { status: 405 });
  if (request.body === null)
    return Response.json({ error: "State archive body is required" }, { status: 400 });
  await env.JV_LINK_STATE.put(STATE_OBJECT_KEY, request.body, {
    httpMetadata: { contentType: CONTENT_TYPE_GZIP },
  });
  return Response.json({ ok: true });
};

const proxyRequest = (request: Request): Request => {
  const headers = new Headers(request.headers);
  headers.delete("authorization");
  return new Request(request, { headers });
};

export class JvLinkContainer extends Container<JvLinkEnvironment> {
  override defaultPort = DEFAULT_PORT;
  override sleepAfter = SLEEP_AFTER;
  override enableInternet = true;

  override async fetch(request: Request): Promise<Response> {
    this.envVars = {
      JRA_VAN_DATALAB_KEY: this.env.JRA_VAN_DATALAB_KEY,
      JRA_VAN_STATE_TOKEN: this.env.JRA_VAN_STATE_TOKEN,
      JRA_VAN_STATE_URL: this.env.JRA_VAN_STATE_URL,
    };
    const forwardedRequest = proxyRequest(request);
    const url = new URL(forwardedRequest.url);
    if (
      url.pathname !== BOOTSTRAP_START_PATH &&
      (url.pathname === BOOTSTRAP_PATH || url.pathname.startsWith(`${BOOTSTRAP_PATH}/`))
    ) {
      url.pathname = url.pathname.slice(BOOTSTRAP_PATH.length) || "/";
      return super.fetch(switchPort(new Request(url, forwardedRequest), BOOTSTRAP_PORT));
    }
    return this.containerFetch(forwardedRequest);
  }
}

export default {
  async fetch(request: Request, env: JvLinkEnvironment): Promise<Response> {
    const pathname = new URL(request.url).pathname;
    if (pathname === HEALTH_PATH)
      return Response.json({ ok: true, runtime: "cloudflare-containers" });
    if (pathname === STATE_PATH) return stateResponse(request, env);
    if (!(await isAuthorized(request, env.JRA_VAN_API_TOKEN))) return unauthorizedResponse();
    return env.JV_LINK_CONTAINER.getByName(CONTAINER_NAME).fetch(request);
  },
} satisfies ExportedHandler<JvLinkEnvironment>;
