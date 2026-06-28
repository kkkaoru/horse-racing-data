// Run with bun. Durable-Object-backed Container class for the predictor image.
// Held-fetch design: the queue consumer calls stub.fetch("/predict?...") which
// the DO proxies via containerFetch — the in-flight containerFetch keeps the
// container alive without any keepalive loop. sleepAfter resets automatically
// per CF docs while the HTTP request is in-flight. container-class.ts is
// excluded from the coverage gate (see vitest.config.ts).

import { Container } from "@cloudflare/containers";
import type { PerRaceParquetEntry, PredictResultLine } from "./ndjson-stream";
import type { Env } from "./types";

const DEFAULT_PORT = 8080;
const SLEEP_AFTER = "15m";
const MODELS_DIR_DEFAULT = "/models";
const EMPTY_ENV_VALUE = "";
const RESULT_LINE_TYPE = "result";
const NDJSON_CONTENT_TYPE = "application/x-ndjson";
const TEXT_DECODER = new TextDecoder();

// Proxy the feature parquet bytes embedded in the NDJSON result line to R2 via
// the Worker's FEATURES_CACHE binding. The Container's S3 token is read-only so
// the Python side embeds the parquet as base64 in the result line, and the Worker
// DO does the R2 PUT here — no S3 credentials needed in the Container env.
const proxyParquetToR2 = async (result: PredictResultLine, env: Env): Promise<void> => {
  const { parquetBase64, parquetKey } = result;
  if (!parquetBase64 || !parquetKey) return;
  try {
    const bytes = Uint8Array.from(atob(parquetBase64), (c) => c.charCodeAt(0));
    await env.FEATURES_CACHE.put(parquetKey, bytes.buffer, {
      httpMetadata: { contentType: "application/octet-stream" },
    });
    console.log(`[container-class] R2 proxy ok key=${parquetKey} bytes=${bytes.length}`);
  } catch (err) {
    // R2 proxy failure must never block predictions — log and continue.
    console.error(`[container-class] R2 proxy failed key=${parquetKey}: ${String(err)}`);
  }
};

// Proxy each per-race feature parquet embedded in the NDJSON result line to R2
// via the Worker's FEATURES_CACHE binding, for the same read-only-S3-token reason
// as proxyParquetToR2 above. Failures are logged and never block the response.
const proxyPerRaceParquetsToR2 = async (
  parquets: ReadonlyArray<PerRaceParquetEntry>,
  env: Env,
): Promise<void> => {
  await Promise.all(
    parquets.map(async (entry) => {
      try {
        const bytes = Uint8Array.from(atob(entry.parquetBase64), (c) => c.charCodeAt(0));
        await env.FEATURES_CACHE.put(entry.parquetKey, bytes.buffer, {
          httpMetadata: { contentType: "application/octet-stream" },
        });
        console.log(
          `[container-class] R2 per-race proxy ok key=${entry.parquetKey} bytes=${bytes.length}`,
        );
      } catch (err) {
        console.error(
          `[container-class] R2 per-race proxy failed key=${entry.parquetKey}: ${String(err)}`,
        );
      }
    }),
  );
};

// Parse the NDJSON body to find the result line and proxy parquet if present,
// then return a reconstructed Response with the same body for the caller.
const proxyParquetFromNdjson = async (response: Response, env: Env): Promise<Response> => {
  if (!response.body) return response;
  const contentType = response.headers.get("Content-Type") ?? "";
  if (!contentType.includes(NDJSON_CONTENT_TYPE)) return response;

  // Buffer the full response body to extract the result line, then re-stream it.
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) chunks.push(value);
  }
  const combined = new Uint8Array(chunks.reduce((acc, c) => acc + c.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.length;
  }
  const text = TEXT_DECODER.decode(combined);

  // Find and proxy the result line asynchronously (non-blocking).
  const lastLine = text
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0)
    .at(-1);
  if (lastLine) {
    try {
      const parsed = JSON.parse(lastLine) as { type: string };
      if (parsed.type === RESULT_LINE_TYPE) {
        const resultLine = parsed as PredictResultLine;
        void proxyParquetToR2(resultLine, env);
        if (resultLine.perRaceParquets && resultLine.perRaceParquets.length > 0) {
          void proxyPerRaceParquetsToR2(resultLine.perRaceParquets, env);
        }
      }
    } catch {
      // Malformed JSON — ignore; parseNdjsonStream will surface the error.
    }
  }

  // Reconstruct the response with the original headers and buffered body.
  return new Response(combined, {
    status: response.status,
    headers: response.headers,
  });
};

export class FinishPositionPredictContainer extends Container<Env> {
  override defaultPort = DEFAULT_PORT;
  override sleepAfter = SLEEP_AFTER;
  override enableInternet = true;

  override async fetch(request: Request): Promise<Response> {
    this.envVars = {
      MODELS_DIR: MODELS_DIR_DEFAULT,
      NEON_DATABASE_URL: this.env.NEON_DATABASE_URL,
      PREDICT_DAYS_AHEAD: this.env.PREDICT_DAYS_AHEAD,
      R2_ACCOUNT_ID: this.env.R2_ACCOUNT_ID ?? EMPTY_ENV_VALUE,
      R2_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID ?? EMPTY_ENV_VALUE,
      R2_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY ?? EMPTY_ENV_VALUE,
      R2_BUCKET: this.env.R2_BUCKET ?? EMPTY_ENV_VALUE,
      VENUE_WEATHER_URL: this.env.VENUE_WEATHER_URL ?? EMPTY_ENV_VALUE,
    };
    try {
      const response = await this.containerFetch(request);
      return proxyParquetFromNdjson(response, this.env);
    } catch (err) {
      console.error(`[container-class] containerFetch failed: ${String(err)}`);
      return Response.json(
        { error: "Container start failed", detail: String(err) },
        { status: 502 },
      );
    }
  }
}
