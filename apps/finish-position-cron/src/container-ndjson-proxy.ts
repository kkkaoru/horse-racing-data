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

interface ParquetPutContext {
  debug: boolean;
  env: R2ProxyEnv;
  logCommit: boolean;
}

interface StrictDayBaseProxyOptions {
  debug?: boolean;
  env: R2ProxyEnv;
  renewActivityTimeout?: RenewActivityTimeout;
  response: Response;
  waitUntil?: WaitUntil;
}

interface ProxyingStreamOptions {
  body: ReadableStream<Uint8Array>;
  debug: boolean;
  env: R2ProxyEnv;
  renewActivityTimeout: RenewActivityTimeout | undefined;
  strictResultProxy: boolean;
  waitUntil: WaitUntil | undefined;
}

interface LastLineTracker {
  acceptChunk(chunk: Uint8Array): void;
  finish(): string | undefined;
}

type LineObserver = (line: string) => void;

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
  context: ParquetPutContext,
): Promise<void> => {
  const bytes = Uint8Array.from(atob(entry.base64), (c) => c.charCodeAt(0));
  const startedAt = Date.now();
  try {
    await context.env.FEATURES_CACHE.put(
      entry.key,
      bytes.buffer,
      buildR2PutOptions(entry.customMetadata),
    );
  } catch (error) {
    if (context.logCommit) {
      console.error(
        `[daybase-r2-commit] failed key=${entry.key} durationMs=${Date.now() - startedAt} error=${String(error)}`,
      );
    }
    throw error;
  }
  if (context.logCommit) {
    console.log(
      `[daybase-r2-commit] key=${entry.key} bytes=${bytes.length} durationMs=${Date.now() - startedAt}`,
    );
  } else if (context.debug) {
    console.log(
      `[container-class] ${logLabel(entry.kind)} ok key=${entry.key} bytes=${bytes.length}`,
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
    buildParquetProxyEntries(result).map((entry) =>
      putParquetToR2(entry, { debug, env, logCommit: false }),
    ),
  );
};

const commitDayBaseResultParquetsToR2 = async (
  result: PredictResultLine,
  env: R2ProxyEnv,
  debug: boolean,
): Promise<void> => {
  const startedAt = Date.now();
  const entries = buildParquetProxyEntries(result);
  console.log(`[daybase-r2-commit] terminal result received entries=${entries.length}`);
  await Promise.all(entries.map((entry) => putParquetToR2(entry, { debug, env, logCommit: true })));
  console.log(
    `[daybase-r2-commit] terminal barrier complete entries=${entries.length} durationMs=${Date.now() - startedAt}`,
  );
};

const isIdleTerminalNdjson = (text: string): boolean => IDLE_TERMINAL_STATUS_PATTERN.test(text);

const proxyResultLineParquetsToR2 = async (
  line: string,
  env: R2ProxyEnv,
  debug: boolean,
): Promise<void> => {
  let parsed: { type?: unknown };
  try {
    parsed = JSON.parse(line) as { type?: unknown };
  } catch {
    // Malformed JSON is left for parseNdjsonStream to surface to the queue consumer.
    return;
  }
  if (parsed.type !== RESULT_LINE_TYPE) return;
  try {
    await proxyResultParquetsToR2(parsed as PredictResultLine, env, debug);
  } catch (error) {
    // Live NDJSON proxying is intentionally best effort so an R2 outage does
    // not truncate the Container response. Explicit pickup callers invoke the
    // strict export directly and therefore cannot report a false success.
    console.error(`[container-class] live R2 proxy failed: ${String(error)}`);
  }
};

const commitResultLineParquetsToR2 = async (
  line: string,
  env: R2ProxyEnv,
  debug: boolean,
): Promise<void> => {
  let parsed: { type?: unknown };
  try {
    parsed = JSON.parse(line) as { type?: unknown };
  } catch {
    return;
  }
  if (parsed.type !== RESULT_LINE_TYPE) return;
  await commitDayBaseResultParquetsToR2(parsed as PredictResultLine, env, debug);
};

const scheduleResultLineProxy = (
  line: string,
  env: R2ProxyEnv,
  waitUntil: WaitUntil | undefined,
  debug: boolean,
  strict: boolean,
): Promise<void> | undefined => {
  if (isIdleTerminalNdjson(line)) return undefined;
  const task = Promise.resolve().then(() =>
    strict
      ? commitResultLineParquetsToR2(line, env, debug)
      : proxyResultLineParquetsToR2(line, env, debug),
  );
  waitUntil?.(task);
  return task;
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

const logDaybasePipelineTiming = (line: string): void => {
  let parsed: { type?: unknown; stage?: unknown };
  try {
    parsed = JSON.parse(line) as { type?: unknown; stage?: unknown };
  } catch {
    return;
  }
  if (
    parsed.type !== "progress" ||
    typeof parsed.stage !== "string" ||
    !parsed.stage.startsWith("step=daybase-")
  ) {
    return;
  }
  console.log(`[daybase-pipeline-timing] ${parsed.stage}`);
};

const createLastLineTracker = (observeLine?: LineObserver): LastLineTracker => {
  const decoder = new TextDecoder();
  let pendingLine = "";
  let lastNonEmptyLine: string | undefined;

  const rememberLine = (line: string): void => {
    const trimmed = line.trim();
    if (trimmed.length > 0) {
      lastNonEmptyLine = trimmed;
      observeLine?.(trimmed);
    }
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

const createProxyingNdjsonStream = (options: ProxyingStreamOptions): ReadableStream<Uint8Array> => {
  const tracker = createLastLineTracker(logDaybasePipelineTiming);
  const decoder = new TextDecoder();
  return options.body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller): void {
        scheduleActivityRenew(
          options.renewActivityTimeout,
          decoder.decode(chunk, { stream: true }),
        );
        controller.enqueue(chunk);
        tracker.acceptChunk(chunk);
      },
      async flush(): Promise<void> {
        const lastLine = tracker.finish();
        if (lastLine === undefined) return;
        const task = scheduleResultLineProxy(
          lastLine,
          options.env,
          options.waitUntil,
          options.debug,
          options.strictResultProxy,
        );
        if (options.strictResultProxy) await task;
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
    createProxyingNdjsonStream({
      body: response.body,
      debug,
      env,
      renewActivityTimeout,
      strictResultProxy: false,
      waitUntil,
    }),
    {
      headers: response.headers,
      status: response.status,
      statusText: response.statusText,
    },
  );
};

export const proxyDayBaseParquetFromNdjson = (options: StrictDayBaseProxyOptions): Response => {
  if (!options.response.body) return options.response;
  const contentType = options.response.headers.get("Content-Type") ?? "";
  if (!contentType.includes(NDJSON_CONTENT_TYPE)) return options.response;
  return new Response(
    createProxyingNdjsonStream({
      body: options.response.body,
      debug: options.debug ?? false,
      env: options.env,
      renewActivityTimeout: options.renewActivityTimeout,
      strictResultProxy: true,
      waitUntil: options.waitUntil,
    }),
    {
      headers: options.response.headers,
      status: options.response.status,
      statusText: options.response.statusText,
    },
  );
};

export type { R2ProxyEnv, RenewActivityTimeout, WaitUntil };
