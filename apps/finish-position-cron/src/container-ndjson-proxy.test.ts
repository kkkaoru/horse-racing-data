// Run with bun. Tests for the Container NDJSON response proxy.

import { expect, test, vi } from "vitest";
import {
  proxyParquetFromNdjson,
  type R2ProxyEnv,
  type RenewActivityTimeout,
  type WaitUntil,
} from "./container-ndjson-proxy";

const encoder = new TextEncoder();

interface R2Mock {
  env: R2ProxyEnv;
  put: ReturnType<typeof vi.fn>;
}

interface ControlledStream {
  controller: ReadableStreamDefaultController<Uint8Array>;
  stream: ReadableStream<Uint8Array>;
}

const makeR2Mock = (putImpl: () => Promise<void> = async () => undefined): R2Mock => {
  const put = vi.fn(putImpl);
  return {
    env: { FEATURES_CACHE: { put } as unknown as R2Bucket },
    put,
  };
};

const makeWaitUntil = (): { tasks: Promise<void>[]; waitUntil: WaitUntil } => {
  const tasks: Promise<void>[] = [];
  return {
    tasks,
    waitUntil(task): void {
      tasks.push(task);
    },
  };
};

const makeControlledStream = (): ControlledStream => {
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  const stream = new ReadableStream<Uint8Array>({
    start(c): void {
      controller = c;
    },
  });
  if (controller === null) throw new Error("stream controller was not initialized");
  return { controller, stream };
};

const ndjsonResponse = (
  body: ReadableStream<Uint8Array>,
  headers: HeadersInit = { "Content-Type": "application/x-ndjson; charset=utf-8" },
): Response => new Response(body, { headers, status: 202, statusText: "Accepted" });

const enqueueText = (
  controller: ReadableStreamDefaultController<Uint8Array>,
  text: string,
): void => {
  controller.enqueue(encoder.encode(text));
};

test("proxyParquetFromNdjson returns body-less responses unchanged", () => {
  const { env } = makeR2Mock();
  const response = new Response(null, { status: 204 });
  expect(proxyParquetFromNdjson(response, env)).toBe(response);
});

test("proxyParquetFromNdjson returns non-NDJSON responses unchanged", () => {
  const { env, put } = makeR2Mock();
  const response = new Response("ok", { headers: { "Content-Type": "text/plain" } });
  expect(proxyParquetFromNdjson(response, env)).toBe(response);
  expect(put).not.toHaveBeenCalled();
});

test("proxyParquetFromNdjson streams chunks before upstream closes and proxies result parquets", async () => {
  const { controller, stream } = makeControlledStream();
  const { env, put } = makeR2Mock();
  const { tasks, waitUntil } = makeWaitUntil();
  const proxied = proxyParquetFromNdjson(ndjsonResponse(stream), env, waitUntil);
  const reader = proxied.body?.getReader();
  if (!reader) throw new Error("proxied response did not have a body");

  const progress = `${JSON.stringify({ type: "progress", message: "started" })}\n`;
  enqueueText(controller, progress);
  await expect(reader.read()).resolves.toStrictEqual({
    done: false,
    value: encoder.encode(progress),
  });
  expect(put).not.toHaveBeenCalled();

  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 2,
    category: "nar",
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-cache/nar/20260629/features.parquet",
    perRaceParquets: [
      { parquetBase64: "cmFjZTE=", parquetKey: "feat-cache/nar/20260629/01.parquet" },
      { parquetBase64: "cmFjZTI=", parquetKey: "feat-cache/nar/20260629/02.parquet" },
    ],
  });
  enqueueText(controller, `\n${resultLine}`);
  controller.close();

  await expect(reader.read()).resolves.toStrictEqual({
    done: false,
    value: encoder.encode(`\n${resultLine}`),
  });
  await expect(reader.read()).resolves.toStrictEqual({ done: true, value: undefined });
  await Promise.all(tasks);

  expect(proxied.status).toBe(202);
  expect(proxied.statusText).toBe("Accepted");
  expect(proxied.headers.get("Content-Type")).toBe("application/x-ndjson; charset=utf-8");
  expect(put).toHaveBeenCalledTimes(3);
  expect(put).toHaveBeenNthCalledWith(
    1,
    "feat-cache/nar/20260629/features.parquet",
    encoder.encode("main").buffer,
    { httpMetadata: { contentType: "application/octet-stream" } },
  );
  expect(put).toHaveBeenNthCalledWith(
    2,
    "feat-cache/nar/20260629/01.parquet",
    encoder.encode("race1").buffer,
    { httpMetadata: { contentType: "application/octet-stream" } },
  );
  expect(put).toHaveBeenNthCalledWith(
    3,
    "feat-cache/nar/20260629/02.parquet",
    encoder.encode("race2").buffer,
    { httpMetadata: { contentType: "application/octet-stream" } },
  );
});

test("proxyParquetFromNdjson renews container activity for each streamed chunk", async () => {
  const { controller, stream } = makeControlledStream();
  const { env } = makeR2Mock();
  const renewActivityTimeout: RenewActivityTimeout = vi.fn(() => undefined);
  const proxied = proxyParquetFromNdjson(
    ndjsonResponse(stream),
    env,
    undefined,
    renewActivityTimeout,
  );
  const reader = proxied.body?.getReader();
  if (!reader) throw new Error("proxied response did not have a body");

  const first = `${JSON.stringify({ type: "progress", message: "started" })}\n`;
  const second = `${JSON.stringify({ type: "progress", message: "predict" })}\n`;
  enqueueText(controller, first);
  enqueueText(controller, second);
  controller.close();

  await expect(reader.read()).resolves.toStrictEqual({
    done: false,
    value: encoder.encode(first),
  });
  await expect(reader.read()).resolves.toStrictEqual({
    done: false,
    value: encoder.encode(second),
  });
  await expect(reader.read()).resolves.toStrictEqual({ done: true, value: undefined });
  expect(renewActivityTimeout).toHaveBeenCalledTimes(2);
});

test("proxyParquetFromNdjson keeps streaming when activity renew throws", async () => {
  const { controller, stream } = makeControlledStream();
  const { env } = makeR2Mock();
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const renewActivityTimeout: RenewActivityTimeout = vi.fn(() => {
    throw new Error("renew failed");
  });
  const proxied = proxyParquetFromNdjson(
    ndjsonResponse(stream),
    env,
    undefined,
    renewActivityTimeout,
  );
  const reader = proxied.body?.getReader();
  if (!reader) throw new Error("proxied response did not have a body");

  const chunk = `${JSON.stringify({ type: "progress", message: "started" })}\n`;
  enqueueText(controller, chunk);
  controller.close();

  await expect(reader.read()).resolves.toStrictEqual({
    done: false,
    value: encoder.encode(chunk),
  });
  await expect(reader.read()).resolves.toStrictEqual({ done: true, value: undefined });

  expect(renewActivityTimeout).toHaveBeenCalledTimes(1);
  expect(consoleError).toHaveBeenCalledWith(
    "[container-class] activity renew failed: Error: renew failed",
  );
  consoleError.mockRestore();
});

test("proxyParquetFromNdjson tracks split lines and ignores non-result last lines", async () => {
  const { env, put } = makeR2Mock();
  const { tasks, waitUntil } = makeWaitUntil();
  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 1,
    category: "jra",
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-cache/jra/20260629/features.parquet",
  });
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(`${resultLine.slice(0, 20)}`));
        controller.enqueue(encoder.encode(`${resultLine.slice(20)}\n\n`));
        controller.enqueue(encoder.encode(JSON.stringify({ type: "progress", message: "after" })));
        controller.close();
      },
    }),
  );

  await expect(proxyParquetFromNdjson(response, env, waitUntil).text()).resolves.toContain("after");
  await Promise.all(tasks);
  expect(put).not.toHaveBeenCalled();
});

test("proxyParquetFromNdjson ignores malformed last lines", async () => {
  const { env, put } = makeR2Mock();
  const { tasks, waitUntil } = makeWaitUntil();
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode("{not-json"));
        controller.close();
      },
    }),
  );

  await expect(proxyParquetFromNdjson(response, env, waitUntil).text()).resolves.toBe("{not-json");
  await Promise.all(tasks);
  expect(put).not.toHaveBeenCalled();
});

test("proxyParquetFromNdjson does not block stream completion when R2 put fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const { env, put } = makeR2Mock(async () => {
    throw new Error("r2 down");
  });
  const { tasks, waitUntil } = makeWaitUntil();
  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 1,
    category: "nar",
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-cache/nar/20260629/features.parquet",
  });
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(resultLine));
        controller.close();
      },
    }),
  );

  await expect(proxyParquetFromNdjson(response, env, waitUntil).text()).resolves.toBe(resultLine);
  await expect(Promise.all(tasks)).resolves.toStrictEqual([undefined]);
  expect(put).toHaveBeenCalledTimes(1);
  expect(errorSpy).toHaveBeenCalledOnce();
  errorSpy.mockRestore();
});

test("proxyParquetFromNdjson can run without waitUntil when no parquet fields are present", async () => {
  const { env, put } = makeR2Mock();
  const resultLine = JSON.stringify({ type: "result", racesPredicted: 0, category: "ban-ei" });
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(resultLine));
        controller.close();
      },
    }),
  );

  await expect(proxyParquetFromNdjson(response, env).text()).resolves.toBe(resultLine);
  await Promise.resolve();
  expect(put).not.toHaveBeenCalled();
});
