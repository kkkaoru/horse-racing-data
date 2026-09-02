// Run with bun. Remote proof that a Workers isolate cannot host the official JV-Link SDK.

import { spawnSync } from "node:child_process";
import { acquireJvData, type AcquisitionQuery } from "./acquisition";
import { assertCompatibilityAttestation } from "./compatibility-attestation";
import {
  JVLINK_DEPLOYED_METHODS,
  JVLINK_DEPLOYMENT_COMPATIBILITY,
  JVLINK_EVENTS,
  JVLINK_FULL_COMPATIBILITY,
  JVLINK_METHODS,
  JVLINK_PROPERTIES,
} from "./compatibility";
import { acquireCourse } from "./course";
import { probeShiftJis } from "./encoding";
import { decodeJvFile } from "./jvfile";
import { acquireMovieList, type MovieListQuery } from "./movie";
import { probeJvLinkNetwork } from "./network";
import { probeWorkerOnly } from "./probe";
import { acquireRealtimeData, type RealtimeQuery } from "./realtime";
import { RUST_CORE_VERSION } from "./rust-core";
import { probeRuntime } from "./runtime";
import { createJvDataStream, encodeBase64 } from "./streaming";

interface WorkerNativeSecrets {
  CORE_CONFIG_V1: string;
  JRA_VAN_WORKER_API_TOKEN: string;
}

interface WorkerNativeEnvironment extends Env, WorkerNativeSecrets {}

const ACQUIRE_PATH: string = "/acquire";
const ACQUIRE_STREAM_PATH: string = "/acquire/stream";
const HEALTH_PATH: string = "/health";
const COMPATIBILITY_PATH: string = "/compatibility";
const COURSE_PATH: string = "/course";
const ENCODING_PROBE_PATH: string = "/encoding";
const JV_FILE_PROBE_PATH: string = "/jvfile";
const NETWORK_PROBE_PATH: string = "/network";
const MOVIES_PATH: string = "/movies";
const RUNTIME_PROBE_PATH: string = "/runtime";
const REALTIME_PATH: string = "/realtime";
const WINE_COMMAND: string = "wine";
const WINE_VERSION_ARGUMENTS: string[] = ["--version"];
const MAX_REQUEST_BYTES: number = 4096;
const JV_FILE_VECTOR: Uint8Array = new Uint8Array([
  32, 32, 32, 32, 32, 32, 32, 32, 32, 53, 120, 156, 155, 228, 170, 250, 112, 26, 59, 0, 9, 53, 2,
  123,
]);

const isAuthorized = (request: Request, token: string): boolean => {
  const authorization = request.headers.get("Authorization");
  if (authorization === null || !authorization.startsWith("Bearer ")) return false;
  const supplied = authorization.slice(7);
  if (supplied.length !== token.length) return false;
  let difference = 0;
  for (let index = 0; index < token.length; index += 1)
    difference |= supplied.charCodeAt(index) ^ token.charCodeAt(index);
  return difference === 0;
};

const acquire = async (request: Request, env: WorkerNativeEnvironment): Promise<Response> => {
  if (request.method !== "POST") return new Response(null, { status: 405 });
  if (!isAuthorized(request, env.JRA_VAN_WORKER_API_TOKEN))
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  const contentLength = Number(request.headers.get("Content-Length"));
  if (!Number.isInteger(contentLength) || contentLength < 1 || contentLength > MAX_REQUEST_BYTES)
    return Response.json({ error: "A bounded Content-Length is required" }, { status: 400 });
  try {
    await assertCompatibilityAttestation(env.JVLINK_COMPATIBILITY_ARTIFACTS);
    const query = (await request.json()) as AcquisitionQuery;
    const result = await acquireJvData(env.CORE_CONFIG_V1, query);
    return Response.json({
      decodedBytes: result.decodedBytes,
      fileBytes: result.fileBytes,
      filename: result.filename,
      filesReturned: result.files.length,
      record: new TextDecoder("shift_jis").decode(result.record),
      recordsReturned: result.recordCount,
      transitions: result.transitions,
    });
  } catch (error) {
    return Response.json(
      {
        error: "JV-Data acquisition failed",
        stage:
          error instanceof Error && error.cause instanceof Error
            ? `${error.message}: ${error.cause.message}`
            : error instanceof Error
              ? error.message
              : "Unknown acquisition failure",
      },
      { status: 502 },
    );
  }
};

const acquireStream = async (request: Request, env: WorkerNativeEnvironment): Promise<Response> => {
  if (request.method !== "POST") return new Response(null, { status: 405 });
  if (!isAuthorized(request, env.JRA_VAN_WORKER_API_TOKEN))
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  const contentLength = Number(request.headers.get("Content-Length"));
  if (!Number.isInteger(contentLength) || contentLength < 1 || contentLength > MAX_REQUEST_BYTES)
    return Response.json({ error: "A bounded Content-Length is required" }, { status: 400 });
  try {
    await assertCompatibilityAttestation(env.JVLINK_COMPATIBILITY_ARTIFACTS);
    const query = (await request.json()) as AcquisitionQuery;
    const stream = await createJvDataStream(env.CORE_CONFIG_V1, query);
    return new Response(stream, {
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": "application/x-ndjson",
        "X-Content-Type-Options": "nosniff",
      },
    });
  } catch (error) {
    return Response.json(
      {
        error: "JV-Data stream could not be opened",
        stage: error instanceof Error ? error.message : "Unknown acquisition failure",
      },
      { status: 502 },
    );
  }
};

const acquireCourseFile = async (
  request: Request,
  env: WorkerNativeEnvironment,
): Promise<Response> => {
  if (request.method !== "POST") return new Response(null, { status: 405 });
  if (!isAuthorized(request, env.JRA_VAN_WORKER_API_TOKEN))
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  const contentLength = Number(request.headers.get("Content-Length"));
  if (!Number.isInteger(contentLength) || contentLength < 1 || contentLength > MAX_REQUEST_BYTES)
    return Response.json({ error: "A bounded Content-Length is required" }, { status: 400 });
  try {
    await assertCompatibilityAttestation(env.JVLINK_COMPATIBILITY_ARTIFACTS);
    const { key } = (await request.json()) as { key: string };
    const result = await acquireCourse(env.CORE_CONFIG_V1, key);
    return Response.json(
      {
        encoding: "base64",
        explanation: encodeBase64(result.explanation),
        image: encodeBase64(result.image),
        path: result.path,
      },
      { headers: { "Cache-Control": "no-store" } },
    );
  } catch (error) {
    return Response.json(
      {
        error: "JV course acquisition failed",
        stage: error instanceof Error ? error.message : "Unknown acquisition failure",
      },
      { status: 502 },
    );
  }
};

const acquireMovies = async (request: Request, env: WorkerNativeEnvironment): Promise<Response> => {
  if (request.method !== "POST") return new Response(null, { status: 405 });
  if (!isAuthorized(request, env.JRA_VAN_WORKER_API_TOKEN))
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  const contentLength = Number(request.headers.get("Content-Length"));
  if (!Number.isInteger(contentLength) || contentLength < 1 || contentLength > MAX_REQUEST_BYTES)
    return Response.json({ error: "A bounded Content-Length is required" }, { status: 400 });
  try {
    await assertCompatibilityAttestation(env.JVLINK_COMPATIBILITY_ARTIFACTS);
    const query = (await request.json()) as MovieListQuery;
    const result = await acquireMovieList(env.CORE_CONFIG_V1, query);
    return Response.json(
      {
        encoding: "base64",
        keys: result.keys.map(encodeBase64),
        status: result.status,
      },
      { headers: { "Cache-Control": "no-store" } },
    );
  } catch (error) {
    return Response.json(
      {
        error: "JV movie-list acquisition failed",
        stage: error instanceof Error ? error.message : "Unknown acquisition failure",
      },
      { status: 502 },
    );
  }
};

const acquireRealtime = async (
  request: Request,
  env: WorkerNativeEnvironment,
): Promise<Response> => {
  if (request.method !== "POST") return new Response(null, { status: 405 });
  if (!isAuthorized(request, env.JRA_VAN_WORKER_API_TOKEN))
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  const contentLength = Number(request.headers.get("Content-Length"));
  if (!Number.isInteger(contentLength) || contentLength < 1 || contentLength > MAX_REQUEST_BYTES)
    return Response.json({ error: "A bounded Content-Length is required" }, { status: 400 });
  try {
    await assertCompatibilityAttestation(env.JVLINK_COMPATIBILITY_ARTIFACTS);
    const query = (await request.json()) as RealtimeQuery;
    const result = await acquireRealtimeData(env.CORE_CONFIG_V1, query);
    return Response.json(
      {
        encoding: "base64",
        records: result.records.map(encodeBase64),
        status: result.status,
        transitions: result.transitions,
      },
      { headers: { "Cache-Control": "no-store" } },
    );
  } catch (error) {
    return Response.json(
      {
        error: "JV realtime acquisition failed",
        stage: error instanceof Error ? error.message : "Unknown acquisition failure",
      },
      { status: 502 },
    );
  }
};

const compatibility = async (env: WorkerNativeEnvironment): Promise<Response> => {
  try {
    const attestation = await assertCompatibilityAttestation(env.JVLINK_COMPATIBILITY_ARTIFACTS);
    return Response.json({
      attestation,
      deployedMethods: JVLINK_DEPLOYED_METHODS,
      deploymentCompatibility: JVLINK_DEPLOYMENT_COMPATIBILITY,
      events: JVLINK_EVENTS,
      fullCompatibility: JVLINK_FULL_COMPATIBILITY,
      implementation: "rust-wasm-static-module",
      methods: JVLINK_METHODS,
      properties: JVLINK_PROPERTIES,
      rustCoreVersion: RUST_CORE_VERSION,
      specification: "JV-Link 4.9.0.1 / runtime oracle 5.0.0",
    });
  } catch (error) {
    return Response.json(
      {
        deploymentCompatibility: false,
        error: "JV-Link compatibility attestation failed",
        stage: error instanceof Error ? error.message : "Unknown attestation failure",
      },
      { status: 503 },
    );
  }
};

export default {
  async fetch(request: Request, env: WorkerNativeEnvironment): Promise<Response> {
    const pathname = new URL(request.url).pathname;
    if (pathname === HEALTH_PATH)
      return Response.json({ ok: true, runtime: "cloudflare-workers-native-jvdata" });
    if (pathname === COMPATIBILITY_PATH) return compatibility(env);
    if (pathname === COURSE_PATH) return acquireCourseFile(request, env);
    if (pathname === ACQUIRE_PATH) return acquire(request, env);
    if (pathname === ACQUIRE_STREAM_PATH) return acquireStream(request, env);
    if (pathname === ENCODING_PROBE_PATH) return Response.json(probeShiftJis());
    if (pathname === JV_FILE_PROBE_PATH) {
      const decoded = await decodeJvFile(JV_FILE_VECTOR);
      return Response.json({ decoded: new TextDecoder().decode(decoded), supported: true });
    }
    if (pathname === NETWORK_PROBE_PATH) return Response.json(await probeJvLinkNetwork());
    if (pathname === MOVIES_PATH) return acquireMovies(request, env);
    if (pathname === RUNTIME_PROBE_PATH) return Response.json(await probeRuntime());
    if (pathname === REALTIME_PATH) return acquireRealtime(request, env);
    return Response.json(
      probeWorkerOnly(() => {
        const result = spawnSync(WINE_COMMAND, WINE_VERSION_ARGUMENTS, { encoding: "utf8" });
        return {
          status: result.status,
          stderr: result.stderr,
          stdout: result.stdout,
        };
      }),
    );
  },
} satisfies ExportedHandler<WorkerNativeEnvironment>;
