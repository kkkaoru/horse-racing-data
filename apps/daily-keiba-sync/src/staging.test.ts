import { Miniflare } from "miniflare";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { putStream } from "./staging";

let miniflare: Miniflare;
let bucket: R2Bucket;

beforeAll(async () => {
  miniflare = new Miniflare({
    compatibilityDate: "2026-06-18",
    modules: true,
    r2Buckets: { BUCKET: "stream-test" },
    script: "export default {}",
  });
  bucket = (await miniflare.getBindings<{ BUCKET: R2Bucket }>()).BUCKET;
});

afterAll(async () => {
  await miniflare.dispose();
});

describe("R2 multipart staging", () => {
  test("uploads a stream without buffering the complete source", async () => {
    const first = new Uint8Array(5 * 1024 * 1024);
    first.fill(1);
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(first.slice(0, 3 * 1024 * 1024));
        const remainderWithTail = new Uint8Array(2 * 1024 * 1024 + 3);
        remainderWithTail.set(first.slice(3 * 1024 * 1024));
        remainderWithTail.set([2, 3, 4], 2 * 1024 * 1024);
        controller.enqueue(remainderWithTail);
        controller.close();
      },
    });
    await putStream(bucket, "large.bin", stream, { customMetadata: { safe: "yes" } });
    const object = await bucket.get("large.bin");
    expect(object?.size).toBe(first.length + 3);
    expect(object?.customMetadata).toEqual({ safe: "yes" });

    const exact = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(first);
        controller.close();
      },
    });
    await putStream(bucket, "exact.bin", exact, {});
    expect((await bucket.head("exact.bin"))?.size).toBe(first.length);
  });

  test("refuses an empty stream and validates part size", async () => {
    await expect(
      putStream(
        bucket,
        "empty.bin",
        new ReadableStream({ start: (controller) => controller.close() }),
        {},
      ),
    ).rejects.toThrow("empty provider stream");
    await expect(
      putStream(
        bucket,
        "bad.bin",
        new ReadableStream({ start: (controller) => controller.close() }),
        {},
        0,
      ),
    ).rejects.toThrow("part size");
    expect(await bucket.head("empty.bin")).toBeNull();
  });
});
