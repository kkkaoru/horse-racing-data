// Run with bun. Credential-protected Cloudflare Worker for native UmaConn stored-data acquisition.

import { isAuthorized, unauthorizedResponse } from "./auth";
import { assertCompatibilityAttestation, type ArtifactBucket } from "./compatibility-attestation";
import { createDataStream } from "./streaming";
import { AcquisitionError, type AcquisitionQuery } from "./acquisition";

export interface Env {
  NV_CORE_CONFIG_V1: string;
  NVLINK_COMPATIBILITY_ARTIFACTS: ArtifactBucket;
  UMMACON_WORKER_API_TOKEN: string;
}

const isQuery = (value: unknown): value is AcquisitionQuery => {
  if (typeof value !== "object" || value === null) return false;
  return (
    "dataSpec" in value &&
    "fromTime" in value &&
    "option" in value &&
    typeof value.dataSpec === "string" &&
    typeof value.fromTime === "string" &&
    typeof value.option === "number" &&
    Number.isInteger(value.option)
  );
};

const jsonError = (message: string, status: number): Response =>
  Response.json({ error: message }, { status });

const compatibilityResponse = async (env: Env): Promise<Response> => {
  const attestation = await assertCompatibilityAttestation(env.NVLINK_COMPATIBILITY_ARTIFACTS);
  return Response.json({
    attestation,
    deploymentCompatibility: true,
    deployedMethods: [
      "NVInit",
      "NVOpen",
      "NVRead",
      "NVGets",
      "NVStatus",
      "NVSkip",
      "NVCancel",
      "NVClose",
    ],
    fullCompatibility: false,
    provider: "UmaConn NV-Link",
  });
};

const acquire = async (request: Request, env: Env): Promise<Response> => {
  let value: unknown;
  try {
    value = await request.json();
  } catch {
    return jsonError("Invalid JSON request", 400);
  }
  if (!isQuery(value)) return jsonError("Invalid acquisition request", 400);
  await assertCompatibilityAttestation(env.NVLINK_COMPATIBILITY_ARTIFACTS);
  const stream: ReadableStream<Uint8Array> = await createDataStream(env.NV_CORE_CONFIG_V1, value);
  return new Response(stream, {
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/x-ndjson; charset=utf-8",
    },
  });
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url: URL = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/health")
      return Response.json({ ok: true, runtime: "cloudflare-workers-native-nvdata" });
    if (!(await isAuthorized(request, env.UMMACON_WORKER_API_TOKEN))) return unauthorizedResponse();
    try {
      if (request.method === "GET" && url.pathname === "/compatibility")
        return await compatibilityResponse(env);
      if (request.method === "POST" && url.pathname === "/acquire/stream")
        return await acquire(request, env);
      return jsonError("Not found", 404);
    } catch (error: unknown) {
      const headers: Headers = new Headers();
      if (error instanceof AcquisitionError) {
        headers.set("X-NV-Link-Failure-Stage", error.stage);
        if (error.upstreamStatus !== undefined)
          headers.set("X-NV-Link-Upstream-Status", String(error.upstreamStatus));
        if (error.socketStage !== undefined)
          headers.set("X-NV-Link-Socket-Stage", error.socketStage);
        if (error.socketBytes !== undefined)
          headers.set("X-NV-Link-Socket-Bytes", String(error.socketBytes));
      }
      return Response.json({ error: "NV-Link acquisition failed" }, { headers, status: 502 });
    }
  },
};
