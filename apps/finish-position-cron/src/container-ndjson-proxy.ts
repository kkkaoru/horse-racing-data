// Run with bun. Streaming NDJSON response proxy for the Container Durable Object.

import type { DaybaseWatermark, PredictResultLine } from "./ndjson-stream";
import type { Env } from "./types";

const RESULT_LINE_TYPE = "result";
const NDJSON_CONTENT_TYPE = "application/x-ndjson";
const SINGLE_PARQUET_KIND = "single";
const PER_RACE_PARQUET_KIND = "per-race";
const PARQUET_CONTENT_TYPE = "application/octet-stream";
const IDLE_TERMINAL_STATUS_PATTERN = /"status"\s*:\s*"(?:busy|accepted|already-complete)"/;
// R2 custom metadata key names (no "x-amz-meta-" prefix -- R2's S3-compatible
// API adds that automatically on GET/HEAD). Must stay in sync with
// predict_lib.r2_client's own `_WATERMARK_META_*` header name constants on
// the Container side.
const WATERMARK_META_MAX_UPDATED = "max-data-sakusei-nengappi";
const WATERMARK_META_ROW_COUNT = "row-count";
const WATERMARK_META_RS_PREDICTED_AT_MAX = "rs-predicted-at-max";
const WATERMARK_META_RS_ROW_COUNT = "rs-row-count";

type R2ProxyEnv = Pick<Env, "FEATURES_CACHE">;
type WaitUntil = (promise: Promise<void>) => void;
type RenewActivityTimeout = () => void;
type ParquetProxyKind = typeof SINGLE_PARQUET_KIND | typeof PER_RACE_PARQUET_KIND;

interface ParquetProxyEntry {
  base64: string;
  key: string;
  kind: ParquetProxyKind;
  customMetadata?: Record<string, string>;
}

interface LastLineTracker {
  acceptChunk(chunk: Uint8Array): void;
  finish(): string | undefined;
}

const logLabel = (kind: ParquetProxyKind): string =>
  kind === SINGLE_PARQUET_KIND ? "R2 proxy" : "R2 per-race proxy";

// Translates the day-base watermark (task #32) into R2 customMetadata keys.
// Only the single day-base parquet ever carries a watermark -- per-race
// feature parquets have no day-base freshness concept of their own.
const buildWatermarkCustomMetadata = (
  watermark: DaybaseWatermark | undefined,
): Record<string, string> | undefined => {
  if (watermark === undefined) return undefined;
  return {
    [WATERMARK_META_MAX_UPDATED]: watermark.maxDataSakuseiNengappi,
    [WATERMARK_META_ROW_COUNT]: String(watermark.rowCount),
    [WATERMARK_META_RS_PREDICTED_AT_MAX]: watermark.rsPredictedAtMax,
    [WATERMARK_META_RS_ROW_COUNT]: String(watermark.rsRowCount),
  };
};

const buildR2PutOptions = (customMetadata: Record<string, string> | undefined): R2PutOptions => {
  if (customMetadata === undefined) return { httpMetadata: { contentType: PARQUET_CONTENT_TYPE } };
  return { httpMetadata: { contentType: PARQUET_CONTENT_TYPE }, customMetadata };
};

const putParquetToR2 = async (
  entry: ParquetProxyEntry,
  env: R2ProxyEnv,
  debug: boolean,
): Promise<void> => {
  try {
    const bytes = Uint8Array.from(atob(entry.base64), (c) => c.charCodeAt(0));
    await env.FEATURES_CACHE.put(entry.key, bytes.buffer, buildR2PutOptions(entry.customMetadata));
    if (debug) {
      console.log(
        `[container-class] ${logLabel(entry.kind)} ok key=${entry.key} bytes=${bytes.length}`,
      );
    }
  } catch (err) {
    console.error(
      `[container-class] ${logLabel(entry.kind)} failed key=${entry.key}: ${String(err)}`,
    );
  }
};

const buildParquetProxyEntries = (result: PredictResultLine): ParquetProxyEntry[] => {
  const entries: ParquetProxyEntry[] = [];
  const { parquetBase64, parquetKey } = result;
  if (parquetBase64 && parquetKey) {
    entries.push({
      base64: parquetBase64,
      key: parquetKey,
      kind: SINGLE_PARQUET_KIND,
      customMetadata: buildWatermarkCustomMetadata(result.daybaseWatermark),
    });
  }
  for (const entry of result.perRaceParquets ?? []) {
    entries.push({
      base64: entry.parquetBase64,
      key: entry.parquetKey,
      kind: PER_RACE_PARQUET_KIND,
    });
  }
  return entries;
};

// Exported for reuse by focused-full-cache-pickup.ts, which proxies a
// synthetic PredictResultLine-shaped payload fetched from the container's
// GET /focused-full-cache endpoint through the exact same R2-write path --
// see that module for why a second, delayed proxy channel exists at all.
export const proxyResultParquetsToR2 = async (
  result: PredictResultLine,
  env: R2ProxyEnv,
  debug: boolean,
): Promise<void> => {
  await Promise.all(
    buildParquetProxyEntries(result).map((entry) => putParquetToR2(entry, env, debug)),
  );
};

const isIdleTerminalNdjson = (text: string): boolean => IDLE_TERMINAL_STATUS_PATTERN.test(text);

const proxyResultLineParquetsToR2 = async (
  line: string,
  env: R2ProxyEnv,
  debug: boolean,
): Promise<void> => {
  try {
    const parsed = JSON.parse(line) as { type?: unknown };
    if (parsed.type !== RESULT_LINE_TYPE) return;
    await proxyResultParquetsToR2(parsed as PredictResultLine, env, debug);
  } catch {
    // Malformed JSON is left for parseNdjsonStream to surface to the queue consumer.
  }
};

const scheduleResultLineProxy = (
  line: string,
  env: R2ProxyEnv,
  waitUntil: WaitUntil | undefined,
  debug: boolean,
): void => {
  if (isIdleTerminalNdjson(line)) return;
  const task = Promise.resolve().then(() => proxyResultLineParquetsToR2(line, env, debug));
  if (waitUntil) {
    waitUntil(task);
    return;
  }
  void task;
};

const scheduleActivityRenew = (
  renewActivityTimeout: RenewActivityTimeout | undefined,
  text: string,
): void => {
  if (renewActivityTimeout === undefined) return;
  if (isIdleTerminalNdjson(text)) return;
  try {
    renewActivityTimeout();
  } catch (err) {
    console.error(`[container-class] activity renew failed: ${String(err)}`);
  }
};

const createLastLineTracker = (): LastLineTracker => {
  const decoder = new TextDecoder();
  let pendingLine = "";
  let lastNonEmptyLine: string | undefined;

  const rememberLine = (line: string): void => {
    const trimmed = line.trim();
    if (trimmed.length > 0) lastNonEmptyLine = trimmed;
  };

  const acceptText = (text: string): void => {
    pendingLine += text;
    const lines = pendingLine.split("\n");
    pendingLine = lines.pop() as string;
    for (const line of lines) rememberLine(line);
  };

  return {
    acceptChunk(chunk: Uint8Array): void {
      acceptText(decoder.decode(chunk, { stream: true }));
    },
    finish(): string | undefined {
      acceptText(decoder.decode());
      rememberLine(pendingLine);
      pendingLine = "";
      return lastNonEmptyLine;
    },
  };
};

const createProxyingNdjsonStream = (
  body: ReadableStream<Uint8Array>,
  env: R2ProxyEnv,
  waitUntil: WaitUntil | undefined,
  renewActivityTimeout: RenewActivityTimeout | undefined,
  debug: boolean,
): ReadableStream<Uint8Array> => {
  const tracker = createLastLineTracker();
  const decoder = new TextDecoder();
  return body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller): void {
        scheduleActivityRenew(renewActivityTimeout, decoder.decode(chunk, { stream: true }));
        controller.enqueue(chunk);
        tracker.acceptChunk(chunk);
      },
      flush(): void {
        const lastLine = tracker.finish();
        if (lastLine !== undefined) scheduleResultLineProxy(lastLine, env, waitUntil, debug);
      },
    }),
  );
};

export const proxyParquetFromNdjson = (
  response: Response,
  env: R2ProxyEnv,
  waitUntil?: WaitUntil,
  renewActivityTimeout?: RenewActivityTimeout,
  debug = false,
): Response => {
  if (!response.body) return response;
  const contentType = response.headers.get("Content-Type") ?? "";
  if (!contentType.includes(NDJSON_CONTENT_TYPE)) return response;
  return new Response(
    createProxyingNdjsonStream(response.body, env, waitUntil, renewActivityTimeout, debug),
    {
      headers: response.headers,
      status: response.status,
      statusText: response.statusText,
    },
  );
};

export type { R2ProxyEnv, RenewActivityTimeout, WaitUntil };
