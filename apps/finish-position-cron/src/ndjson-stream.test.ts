// Run with bun. Tests for the NDJSON stream parser.

import { expect, test } from "vitest";
import { parseNdjsonStream } from "./ndjson-stream";
import type { PerRaceParquetEntry } from "./ndjson-stream";

const makeStream = (text: string): ReadableStream<Uint8Array> => {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(text));
      controller.close();
    },
  });
};

const makeChunkedStream = (chunks: string[]): ReadableStream<Uint8Array> => {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
};

test("parseNdjsonStream returns the result line from a single-line stream", async () => {
  const line = JSON.stringify({ type: "result", racesPredicted: 5, category: "jra" });
  const result = await parseNdjsonStream(makeStream(line));
  expect(result.type).toBe("result");
  expect(result.racesPredicted).toBe(5);
  expect(result.category).toBe("jra");
});

test("parseNdjsonStream returns the result line from a multi-line stream with progress lines", async () => {
  const progress1 = JSON.stringify({ type: "progress", message: "starting" });
  const progress2 = JSON.stringify({ type: "progress", message: "halfway" });
  const resultLine = JSON.stringify({ type: "result", racesPredicted: 12, category: "nar" });
  const stream = makeStream(`${progress1}\n${progress2}\n${resultLine}`);
  const result = await parseNdjsonStream(stream);
  expect(result.type).toBe("result");
  expect(result.racesPredicted).toBe(12);
  expect(result.category).toBe("nar");
});

test("parseNdjsonStream processes complete lines while chunks are still arriving", async () => {
  const progress1 = JSON.stringify({ type: "progress", stage: "starting", elapsed_s: 0 });
  const progress2 = JSON.stringify({ type: "progress", message: "halfway", elapsed: 12 });
  const resultLine = JSON.stringify({ type: "result", racesPredicted: 9, category: "jra" });
  const progressLines: unknown[] = [];
  const result = await parseNdjsonStream(
    makeChunkedStream([
      progress1.slice(0, 10),
      `${progress1.slice(10)}\n${progress2.slice(0, 8)}`,
      `${progress2.slice(8)}\n`,
      resultLine,
    ]),
    {
      onProgress(line) {
        progressLines.push(line);
      },
    },
  );
  expect(progressLines).toStrictEqual([
    { type: "progress", stage: "starting", elapsed_s: 0 },
    { type: "progress", message: "halfway", elapsed: 12 },
  ]);
  expect(result.racesPredicted).toBe(9);
});

test("parseNdjsonStream ignores blank lines without replacing the last result line", async () => {
  const resultLine = JSON.stringify({ type: "result", racesPredicted: 4, category: "jra" });
  const result = await parseNdjsonStream(makeStream(`\n ${resultLine} \n\n`));
  expect(result.racesPredicted).toBe(4);
});

test("parseNdjsonStream throws on empty stream", async () => {
  await expect(parseNdjsonStream(makeStream(""))).rejects.toThrow("Empty NDJSON stream");
});

test("parseNdjsonStream throws on malformed JSON lines", async () => {
  const resultLine = JSON.stringify({ type: "result", racesPredicted: 5, category: "jra" });
  await expect(parseNdjsonStream(makeStream(`{"type":"progress"\n${resultLine}`))).rejects.toThrow(
    SyntaxError,
  );
});

test("parseNdjsonStream throws when last line is not type result", async () => {
  const progress = JSON.stringify({ type: "progress", message: "started" });
  await expect(parseNdjsonStream(makeStream(progress))).rejects.toThrow("Expected result line");
});

test("parseNdjsonStream returns parquetBase64 and parquetKey when present in result line", async () => {
  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 8,
    category: "nar",
    parquetBase64: "dGVzdA==",
    parquetKey: "feat-cache/nar/20260619/features.parquet",
  });
  const result = await parseNdjsonStream(makeStream(resultLine));
  expect(result.parquetBase64).toBe("dGVzdA==");
  expect(result.parquetKey).toBe("feat-cache/nar/20260619/features.parquet");
});

test("parseNdjsonStream returns undefined parquetBase64/parquetKey when absent", async () => {
  const resultLine = JSON.stringify({ type: "result", racesPredicted: 5, category: "jra" });
  const result = await parseNdjsonStream(makeStream(resultLine));
  expect(result.parquetBase64).toBeUndefined();
  expect(result.parquetKey).toBeUndefined();
});

test("parseNdjsonStream returns perRaceParquets array when present in result line", async () => {
  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 3,
    category: "jra",
    perRaceParquets: [
      { parquetBase64: "YWFh", parquetKey: "feat-cache/jra/20260620/01.parquet" },
      { parquetBase64: "YmJi", parquetKey: "feat-cache/jra/20260620/02.parquet" },
    ],
  });
  const result = await parseNdjsonStream(makeStream(resultLine));
  expect(result.perRaceParquets).toStrictEqual([
    { parquetBase64: "YWFh", parquetKey: "feat-cache/jra/20260620/01.parquet" },
    { parquetBase64: "YmJi", parquetKey: "feat-cache/jra/20260620/02.parquet" },
  ]);
});

test("parseNdjsonStream returns undefined perRaceParquets when absent", async () => {
  const resultLine = JSON.stringify({ type: "result", racesPredicted: 5, category: "nar" });
  const result = await parseNdjsonStream(makeStream(resultLine));
  expect(result.perRaceParquets).toBeUndefined();
});

test("parseNdjsonStream returns status, error, and runDate when present", async () => {
  const resultLine = JSON.stringify({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    runDate: "20260619",
    status: "error",
    error: "RuntimeError: prediction failed",
  });
  const result = await parseNdjsonStream(makeStream(resultLine));
  expect(result.status).toBe("error");
  expect(result.error).toBe("RuntimeError: prediction failed");
  expect(result.runDate).toBe("20260619");
});

test("PerRaceParquetEntry type shape holds the expected fields", () => {
  const entry: PerRaceParquetEntry = {
    parquetBase64: "Y2Nj",
    parquetKey: "feat-cache/nar/20260620/03.parquet",
  };
  expect(entry).toStrictEqual({
    parquetBase64: "Y2Nj",
    parquetKey: "feat-cache/nar/20260620/03.parquet",
  });
});
