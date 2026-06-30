// Run with bun. Streaming NDJSON response proxy for the Container Durable Object.

import type { PredictResultLine } from "./ndjson-stream";
import type { Env } from "./types";

const RESULT_LINE_TYPE = "result";
const NDJSON_CONTENT_TYPE = "application/x-ndjson";
const SINGLE_PARQUET_KIND = "single";
const PER_RACE_PARQUET_KIND = "per-race";

type R2ProxyEnv = Pick<Env, "FEATURES_CACHE">;
type WaitUntil = (promise: Promise<void>) => void;
type RenewActivityTimeout = () => void;
type ParquetProxyKind = typeof SINGLE_PARQUET_KIND | typeof PER_RACE_PARQUET_KIND;

interface ParquetProxyEntry {
  base64: string;
  key: string;
  kind: ParquetProxyKind;
}

interface LastLineTracker {
  acceptChunk(chunk: Uint8Array): void;
  finish(): string | undefined;
}

const logLabel = (kind: ParquetProxyKind): string =>
  kind === SINGLE_PARQUET_KIND ? "R2 proxy" : "R2 per-race proxy";

const putParquetToR2 = async (entry: ParquetProxyEntry, env: R2ProxyEnv): Promise<void> => {
  try {
    const bytes = Uint8Array.from(atob(entry.base64), (c) => c.charCodeAt(0));
    await env.FEATURES_CACHE.put(entry.key, bytes.buffer, {
      httpMetadata: { contentType: "application/octet-stream" },
    });
    console.log(
      `[container-class] ${logLabel(entry.kind)} ok key=${entry.key} bytes=${bytes.length}`,
    );
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
    entries.push({ base64: parquetBase64, key: parquetKey, kind: SINGLE_PARQUET_KIND });
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

const proxyResultParquetsToR2 = async (
  result: PredictResultLine,
  env: R2ProxyEnv,
): Promise<void> => {
  await Promise.all(buildParquetProxyEntries(result).map((entry) => putParquetToR2(entry, env)));
};

const proxyResultLineParquetsToR2 = async (line: string, env: R2ProxyEnv): Promise<void> => {
  try {
    const parsed = JSON.parse(line) as { type?: unknown };
    if (parsed.type !== RESULT_LINE_TYPE) return;
    await proxyResultParquetsToR2(parsed as PredictResultLine, env);
  } catch {
    // Malformed JSON is left for parseNdjsonStream to surface to the queue consumer.
  }
};

const scheduleResultLineProxy = (
  line: string,
  env: R2ProxyEnv,
  waitUntil: WaitUntil | undefined,
): void => {
  const task = Promise.resolve().then(() => proxyResultLineParquetsToR2(line, env));
  if (waitUntil) {
    waitUntil(task);
    return;
  }
  void task;
};

const scheduleActivityRenew = (renewActivityTimeout: RenewActivityTimeout | undefined): void => {
  if (renewActivityTimeout === undefined) return;
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
): ReadableStream<Uint8Array> => {
  const tracker = createLastLineTracker();
  return body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller): void {
        scheduleActivityRenew(renewActivityTimeout);
        controller.enqueue(chunk);
        tracker.acceptChunk(chunk);
      },
      flush(): void {
        const lastLine = tracker.finish();
        if (lastLine !== undefined) scheduleResultLineProxy(lastLine, env, waitUntil);
      },
    }),
  );
};

export const proxyParquetFromNdjson = (
  response: Response,
  env: R2ProxyEnv,
  waitUntil?: WaitUntil,
  renewActivityTimeout?: RenewActivityTimeout,
): Response => {
  if (!response.body) return response;
  const contentType = response.headers.get("Content-Type") ?? "";
  if (!contentType.includes(NDJSON_CONTENT_TYPE)) return response;
  return new Response(
    createProxyingNdjsonStream(response.body, env, waitUntil, renewActivityTimeout),
    {
      headers: response.headers,
      status: response.status,
      statusText: response.statusText,
    },
  );
};

export type { R2ProxyEnv, RenewActivityTimeout, WaitUntil };
