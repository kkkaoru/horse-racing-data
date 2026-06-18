// Run with bun. Tests for the NDJSON stream parser.

import { expect, test } from "vitest";
import { parseNdjsonStream } from "./ndjson-stream";

const makeStream = (text: string): ReadableStream<Uint8Array> => {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(text));
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

test("parseNdjsonStream throws on empty stream", async () => {
  await expect(parseNdjsonStream(makeStream(""))).rejects.toThrow("Empty NDJSON stream");
});

test("parseNdjsonStream throws when last line is not type result", async () => {
  const progress = JSON.stringify({ type: "progress", message: "started" });
  await expect(parseNdjsonStream(makeStream(progress))).rejects.toThrow("Expected result line");
});
