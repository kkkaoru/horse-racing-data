// Run with bun. Tests for the Container NDJSON response proxy.

import { expect, test, vi } from "vitest";
import {
  proxyDayBaseParquetFromNdjson,
  proxyParquetFromNdjson,
  proxyResultParquetsToR2,
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

test("proxyParquetFromNdjson returns responses without content type unchanged", () => {
  const { env, put } = makeR2Mock();
  const response = new Response("ok");
  expect(proxyParquetFromNdjson(response, env)).toBe(response);
  expect(put).not.toHaveBeenCalled();
});

test("proxyParquetFromNdjson returns stream responses without content type unchanged", () => {
  const { env, put } = makeR2Mock();
  const response = new Response(new Blob(["ok"]).stream());
  expect(proxyParquetFromNdjson(response, env)).toBe(response);
  expect(put).not.toHaveBeenCalled();
});

test("proxyDayBaseParquetFromNdjson returns body-less responses unchanged", () => {
  const { env } = makeR2Mock();
  const response = new Response(null, { status: 204 });
  expect(proxyDayBaseParquetFromNdjson({ env, response })).toBe(response);
});

test("proxyDayBaseParquetFromNdjson returns stream responses without content type unchanged", () => {
  const { env, put } = makeR2Mock();
  const response = new Response(new Blob(["ok"]).stream());
  expect(proxyDayBaseParquetFromNdjson({ env, response })).toBe(response);
  expect(put).not.toHaveBeenCalled();
});

test("proxyDayBaseParquetFromNdjson returns non-NDJSON responses unchanged", () => {
  const { env, put } = makeR2Mock();
  const response = new Response("ok", { headers: { "Content-Type": "text/plain" } });
  expect(proxyDayBaseParquetFromNdjson({ env, response })).toBe(response);
  expect(put).not.toHaveBeenCalled();
});

test("proxyParquetFromNdjson does not schedule R2 proxy when stream has no result line", async () => {
  const { env, put } = makeR2Mock();
  const waitUntil = vi.fn();
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.close();
      },
    }),
  );
  await expect(proxyParquetFromNdjson(response, env, waitUntil).text()).resolves.toBe("");
  expect(waitUntil).not.toHaveBeenCalled();
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

test("proxyDayBaseParquetFromNdjson waits for terminal result R2 writes before stream completion", async () => {
  const { controller, stream } = makeControlledStream();
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const putBarrier = Promise.withResolvers<void>();
  const { env, put } = makeR2Mock(() => putBarrier.promise);
  const { tasks, waitUntil } = makeWaitUntil();
  const proxied = proxyDayBaseParquetFromNdjson({
    env,
    response: ndjsonResponse(stream),
    waitUntil,
  });
  const reader = proxied.body?.getReader();
  if (!reader) throw new Error("proxied response did not have a body");

  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 1,
    category: "nar",
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-daybase/catalog-v1/nar/20260826/features.parquet",
  });
  enqueueText(controller, resultLine);
  controller.close();

  await expect(reader.read()).resolves.toStrictEqual({
    done: false,
    value: encoder.encode(resultLine),
  });
  expect(put).toHaveBeenCalledTimes(1);
  expect(tasks).toHaveLength(1);

  const terminalRead = reader.read();
  const terminalState = vi.fn();
  void terminalRead.then(terminalState);
  await Promise.resolve();
  expect(terminalState).not.toHaveBeenCalled();

  putBarrier.resolve();
  await expect(terminalRead).resolves.toStrictEqual({ done: true, value: undefined });
  await expect(Promise.all(tasks)).resolves.toStrictEqual([undefined]);
  expect(consoleLog).toHaveBeenCalledWith("[daybase-r2-commit] terminal result received entries=1");
  expect(consoleLog).toHaveBeenCalledWith(
    expect.stringMatching(
      /^\[daybase-r2-commit\] key=feat-daybase\/catalog-v1\/nar\/20260826\/features\.parquet bytes=4 durationMs=\d+$/,
    ),
  );
  expect(consoleLog).toHaveBeenCalledWith(
    expect.stringMatching(
      /^\[daybase-r2-commit\] terminal barrier complete entries=1 durationMs=\d+$/,
    ),
  );
  consoleLog.mockRestore();
});

test("proxyParquetFromNdjson keeps ordinary prediction stream completion non-blocking", async () => {
  const { controller, stream } = makeControlledStream();
  const putBarrier = Promise.withResolvers<void>();
  const { env } = makeR2Mock(() => putBarrier.promise);
  const { tasks, waitUntil } = makeWaitUntil();
  const proxied = proxyParquetFromNdjson(ndjsonResponse(stream), env, waitUntil);
  const reader = proxied.body?.getReader();
  if (!reader) throw new Error("proxied response did not have a body");

  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 1,
    category: "nar",
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-cache/nar/20260826/43/01/features.parquet",
  });
  enqueueText(controller, resultLine);
  controller.close();

  await expect(reader.read()).resolves.toStrictEqual({
    done: false,
    value: encoder.encode(resultLine),
  });
  await expect(reader.read()).resolves.toStrictEqual({ done: true, value: undefined });
  putBarrier.resolve();
  await expect(Promise.all(tasks)).resolves.toStrictEqual([undefined]);
});

test("proxyParquetFromNdjson attaches the daybase watermark as R2 customMetadata on the single parquet only", async () => {
  const { env, put } = makeR2Mock();
  const { tasks, waitUntil } = makeWaitUntil();
  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 1,
    category: "jra",
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-daybase/catalog-v1/jra/20260712/features.parquet",
    daybaseWatermark: {
      maxDataSakuseiNengappi: "20260712",
      rowCount: 946,
      rsPredictedAtMax: "2026-07-18T09:00:00",
      rsRowCount: 12,
    },
    perRaceParquets: [
      { parquetBase64: "cmFjZTE=", parquetKey: "feat-cache/jra/20260712/01.parquet" },
    ],
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
  await Promise.all(tasks);

  expect(put).toHaveBeenCalledTimes(2);
  expect(put).toHaveBeenNthCalledWith(
    1,
    "feat-daybase/catalog-v1/jra/20260712/features.parquet",
    encoder.encode("main").buffer,
    {
      httpMetadata: { contentType: "application/octet-stream" },
      customMetadata: {
        "max-data-sakusei-nengappi": "20260712",
        "row-count": "946",
        "rs-predicted-at-max": "2026-07-18T09:00:00",
        "rs-row-count": "12",
      },
    },
  );
  expect(put).toHaveBeenNthCalledWith(
    2,
    "feat-cache/jra/20260712/01.parquet",
    encoder.encode("race1").buffer,
    { httpMetadata: { contentType: "application/octet-stream" } },
  );
});

test("proxyParquetFromNdjson logs successful R2 proxy only when debug is enabled", async () => {
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { env } = makeR2Mock();
  const { tasks, waitUntil } = makeWaitUntil();
  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 1,
    category: "jra",
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-cache/jra/20260629/features.parquet",
    perRaceParquets: [
      { parquetBase64: "cmFjZQ==", parquetKey: "feat-cache/jra/20260629/05/01/features.parquet" },
    ],
  });
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(resultLine));
        controller.close();
      },
    }),
  );

  await expect(
    proxyParquetFromNdjson(response, env, waitUntil, undefined, true).text(),
  ).resolves.toBe(resultLine);
  await Promise.all(tasks);
  expect(consoleLog).toHaveBeenCalledWith(
    "[container-class] R2 proxy ok key=feat-cache/jra/20260629/features.parquet bytes=4",
  );
  expect(consoleLog).toHaveBeenCalledWith(
    "[container-class] R2 per-race proxy ok key=feat-cache/jra/20260629/05/01/features.parquet bytes=4",
  );
  consoleLog.mockRestore();
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

test("proxyParquetFromNdjson logs split day-base timings without debug", async () => {
  const { env } = makeR2Mock();
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const timingLine = `${JSON.stringify({
    type: "progress",
    stage:
      "step=daybase-layer index=3/8 status=done category=nar script=add-head-to-head-features.py elapsed_seconds=12.345",
    elapsed_s: 13,
  })}\n`;
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(timingLine.slice(0, 35)));
        controller.enqueue(encoder.encode(timingLine.slice(35)));
        controller.close();
      },
    }),
  );

  await expect(proxyParquetFromNdjson(response, env).text()).resolves.toBe(timingLine);
  expect(consoleLog).toHaveBeenCalledWith(
    "[daybase-pipeline-timing] step=daybase-layer index=3/8 status=done category=nar script=add-head-to-head-features.py elapsed_seconds=12.345",
  );
  consoleLog.mockRestore();
});

test("proxyParquetFromNdjson ignores non-daybase progress for operational logging", async () => {
  const { env } = makeR2Mock();
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const progressLine = `${JSON.stringify({
    type: "progress",
    stage: "predict",
    elapsed_s: 10,
  })}\n`;

  await expect(
    proxyParquetFromNdjson(ndjsonResponse(new Blob([progressLine]).stream()), env).text(),
  ).resolves.toBe(progressLine);
  expect(consoleLog).not.toHaveBeenCalled();
  consoleLog.mockRestore();
});

test("proxyParquetFromNdjson does not renew activity for a busy result chunk", async () => {
  const { env } = makeR2Mock();
  const renewActivityTimeout: RenewActivityTimeout = vi.fn(() => undefined);
  const { tasks, waitUntil } = makeWaitUntil();
  const resultLine = `${JSON.stringify({
    type: "result",
    category: "nar",
    runDate: "20260820",
    racesPredicted: 0,
    status: "busy",
  })}\n`;
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(resultLine));
        controller.close();
      },
    }),
  );
  await expect(
    proxyParquetFromNdjson(response, env, waitUntil, renewActivityTimeout).text(),
  ).resolves.toBe(resultLine);
  expect(renewActivityTimeout).not.toHaveBeenCalled();
  expect(tasks).toStrictEqual([]);
});

test("proxyParquetFromNdjson does not schedule waitUntil for an already-complete result", async () => {
  const { env } = makeR2Mock();
  const renewActivityTimeout: RenewActivityTimeout = vi.fn(() => undefined);
  const { tasks, waitUntil } = makeWaitUntil();
  const resultLine = `${JSON.stringify({
    type: "result",
    category: "jra",
    runDate: "20260820",
    racesPredicted: 0,
    status: "already-complete",
  })}\n`;
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(resultLine));
        controller.close();
      },
    }),
  );
  await expect(
    proxyParquetFromNdjson(response, env, waitUntil, renewActivityTimeout).text(),
  ).resolves.toBe(resultLine);
  expect(renewActivityTimeout).not.toHaveBeenCalled();
  expect(tasks).toStrictEqual([]);
});

test("proxyParquetFromNdjson does not renew activity for an accepted result chunk", async () => {
  const { env } = makeR2Mock();
  const renewActivityTimeout: RenewActivityTimeout = vi.fn(() => undefined);
  const { tasks, waitUntil } = makeWaitUntil();
  const resultLine = `${JSON.stringify({
    type: "result",
    category: "nar",
    runDate: "20260820",
    racesPredicted: 0,
    status: "accepted",
  })}\n`;
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(resultLine));
        controller.close();
      },
    }),
  );
  await expect(
    proxyParquetFromNdjson(response, env, waitUntil, renewActivityTimeout).text(),
  ).resolves.toBe(resultLine);
  expect(renewActivityTimeout).not.toHaveBeenCalled();
  expect(tasks).toStrictEqual([]);
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
  expect(errorSpy).toHaveBeenCalledWith("[container-class] live R2 proxy failed: Error: r2 down");
  errorSpy.mockRestore();
});

test("proxyDayBaseParquetFromNdjson rejects stream completion when an R2 put fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const { env, put } = makeR2Mock(async () => {
    throw new Error("day-base r2 down");
  });
  const { tasks, waitUntil } = makeWaitUntil();
  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 1,
    category: "nar",
    parquetBase64: "bWFpbg==",
    parquetKey: "feat-daybase/catalog-v1/nar/20260826/features.parquet",
  });
  const response = ndjsonResponse(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        controller.enqueue(encoder.encode(resultLine));
        controller.close();
      },
    }),
  );

  await expect(proxyDayBaseParquetFromNdjson({ env, response, waitUntil }).text()).rejects.toThrow(
    "day-base r2 down",
  );
  await expect(Promise.all(tasks)).rejects.toThrow("day-base r2 down");
  expect(put).toHaveBeenCalledTimes(1);
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringMatching(
      /^\[daybase-r2-commit\] failed key=feat-daybase\/catalog-v1\/nar\/20260826\/features\.parquet durationMs=\d+ error=Error: day-base r2 down$/,
    ),
  );
  errorSpy.mockRestore();
});

test("proxyDayBaseParquetFromNdjson preserves an existing-key success without embedded parquet", async () => {
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { env, put } = makeR2Mock();
  const resultLine = JSON.stringify({
    type: "result",
    status: "success",
    racesPredicted: 0,
    category: "nar",
    parquetKey: "feat-daybase/catalog-v1/nar/20260826/features.parquet",
  });
  const response = ndjsonResponse(new Blob([resultLine]).stream());

  await expect(proxyDayBaseParquetFromNdjson({ env, response }).text()).resolves.toBe(resultLine);
  expect(put).not.toHaveBeenCalled();
  expect(consoleLog).toHaveBeenCalledWith("[daybase-r2-commit] terminal result received entries=0");
  expect(consoleLog).toHaveBeenCalledWith(
    expect.stringMatching(
      /^\[daybase-r2-commit\] terminal barrier complete entries=0 durationMs=\d+$/,
    ),
  );
  consoleLog.mockRestore();
});

test("proxyDayBaseParquetFromNdjson preserves an empty terminal result", async () => {
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { env, put } = makeR2Mock();
  const resultLine = JSON.stringify({
    type: "result",
    status: "empty",
    racesPredicted: 0,
    category: "nar",
  });
  const response = ndjsonResponse(new Blob([resultLine]).stream());

  await expect(proxyDayBaseParquetFromNdjson({ env, response }).text()).resolves.toBe(resultLine);
  expect(put).not.toHaveBeenCalled();
  consoleLog.mockRestore();
});

test("proxyDayBaseParquetFromNdjson preserves an error terminal result", async () => {
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { env, put } = makeR2Mock();
  const resultLine = JSON.stringify({
    type: "result",
    status: "error",
    racesPredicted: 0,
    category: "nar",
    error: "build failed",
  });
  const response = ndjsonResponse(new Blob([resultLine]).stream());

  await expect(proxyDayBaseParquetFromNdjson({ env, response }).text()).resolves.toBe(resultLine);
  expect(put).not.toHaveBeenCalled();
  consoleLog.mockRestore();
});

test("proxyDayBaseParquetFromNdjson preserves a non-result terminal line", async () => {
  const { env, put } = makeR2Mock();
  const progressLine = JSON.stringify({ type: "progress", stage: "complete" });
  const response = ndjsonResponse(new Blob([progressLine]).stream());

  await expect(proxyDayBaseParquetFromNdjson({ env, response }).text()).resolves.toBe(progressLine);
  expect(put).not.toHaveBeenCalled();
});

test("proxyResultParquetsToR2 strictly rejects an R2 PUT failure for pickup callers", async () => {
  const { env } = makeR2Mock(async () => {
    throw new Error("canonical put failed");
  });

  await expect(
    proxyResultParquetsToR2(
      {
        type: "result",
        category: "jra",
        racesPredicted: 0,
        parquetBase64: "bWFpbg==",
        parquetKey: "feat-daybase/catalog-v1/jra/20260823/features.parquet",
      },
      env,
      false,
    ),
  ).rejects.toThrow("canonical put failed");
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
