// Run with bun; all response streams are local fixtures.
import { expect, it, vi } from "vitest";

import { readBoundedCatalogBody } from "./catalog-read-body";

it("decodes multibyte UTF-8 across stream chunks", async () => {
  const bytes: Uint8Array = new TextEncoder().encode('{"name":"競走"}');
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes.slice(0, 10));
      controller.enqueue(bytes.slice(10));
      controller.close();
    },
  });
  await expect(readBoundedCatalogBody(new Response(stream))).resolves.toStrictEqual({
    name: "競走",
  });
});
it("accepts a payload exactly at the byte limit", async () => {
  await expect(
    readBoundedCatalogBody(new Response("{}".padEnd(65536, " "))),
  ).resolves.toStrictEqual({});
});
it("accepts exactly 1 MiB only for an enriched day list", async () => {
  await expect(
    readBoundedCatalogBody(new Response("{}".padEnd(1048576, " ")), "race-day-list-with-jockeys"),
  ).resolves.toStrictEqual({});
});
it("cancels an enriched day list exceeding 1 MiB across chunks", async () => {
  const cancel = vi.fn<() => void>();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array(524288));
      controller.enqueue(new Uint8Array(524289));
    },
    cancel,
  });
  await expect(
    readBoundedCatalogBody(new Response(stream), "race-day-list-with-jockeys"),
  ).rejects.toThrow("Invalid Catalog response body");
  expect(cancel).toHaveBeenCalledTimes(1);
});
it("rejects an absent response body", async () => {
  await expect(readBoundedCatalogBody(new Response(null))).rejects.toThrow(
    "Invalid Catalog response body",
  );
});
it("cancels the stream when its byte limit is exceeded", async () => {
  const cancel = vi.fn<() => void>();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array(65537));
    },
    cancel,
  });
  await expect(readBoundedCatalogBody(new Response(stream))).rejects.toThrow(
    "Invalid Catalog response body",
  );
  expect(cancel).toHaveBeenCalledTimes(1);
});
it.each([new Uint8Array([255]), new Uint8Array([227, 129])])(
  "rejects invalid or truncated UTF-8",
  async (bytes) => {
    await expect(readBoundedCatalogBody(new Response(bytes))).rejects.toThrow(TypeError);
  },
);
it("rejects malformed JSON rather than inventing an empty payload", async () => {
  await expect(readBoundedCatalogBody(new Response("{"))).rejects.toThrow(SyntaxError);
});
it("propagates a failed response stream", async () => {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.error(new Error("stream unavailable"));
    },
  });
  await expect(readBoundedCatalogBody(new Response(stream))).rejects.toThrow("stream unavailable");
});
