import { Miniflare } from "miniflare";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createR2Resolver } from "./r2-resolver";

let miniflare: Miniflare;
let bucket: R2Bucket;

beforeAll(async () => {
  miniflare = new Miniflare({
    compatibilityDate: "2026-06-18",
    modules: true,
    r2Buckets: { BUCKET: "resolver-test" },
    script: "export default {}",
  });
  bucket = (await miniflare.getBindings<{ BUCKET: R2Bucket }>()).BUCKET;
});

afterAll(async () => {
  await miniflare.dispose();
});

describe("R2 Iceberg resolver", () => {
  test("writes and range-reads an object in the bound bucket", async () => {
    const resolver = createR2Resolver(bucket, "catalog");
    const writer = resolver.writer("s3://catalog/data/example.bin");
    writer.appendBytes(new Uint8Array([1, 2, 3, 4]));
    await Promise.resolve(writer.finish());

    const reader = await resolver.reader("s3://catalog/data/example.bin");
    expect(reader.byteLength).toBe(4);
    expect(new Uint8Array(await reader.slice(1, 3))).toEqual(new Uint8Array([2, 3]));
    expect(new Uint8Array(await reader.slice(0))).toEqual(new Uint8Array([1, 2, 3, 4]));
  });

  test("supports a known length and deletes the object", async () => {
    await bucket.put("data/known.bin", new Uint8Array([7, 8]));
    const resolver = createR2Resolver(bucket, "catalog");
    const reader = await resolver.reader("s3://catalog/data/known.bin", 2);
    expect(new Uint8Array(await reader.slice(0, 2))).toEqual(new Uint8Array([7, 8]));
    await resolver.deleter("s3://catalog/data/known.bin");
    await expect(reader.slice(0, 1)).rejects.toThrow("not found");
    await expect(resolver.reader("s3://catalog/data/known.bin")).rejects.toThrow("not found");
  });

  test("enforces bucket paths and byte ranges", async () => {
    const resolver = createR2Resolver(bucket, "catalog");
    expect(() => resolver.writer("s3://other/data.bin")).toThrow("outside");
    expect(() => resolver.writer("s3://catalog/../escape.bin")).toThrow("Invalid");
    await bucket.put("data/range.bin", new Uint8Array([1]));
    const reader = await resolver.reader("s3://catalog/data/range.bin");
    await expect(reader.slice(-1, 1)).rejects.toThrow("byte range");
    await expect(reader.slice(1, 0)).rejects.toThrow("byte range");
  });

  test("writes a new object conditionally", async () => {
    const resolver = createR2Resolver(bucket, "catalog");
    const writer = resolver.writer("s3://catalog/data/new-conditional.bin", { ifNoneMatch: "*" });
    writer.appendUint8(1);
    await Promise.resolve(writer.finish());
    expect((await bucket.head("data/new-conditional.bin"))?.size).toBe(1);
  });

  test("fails a conditional write when the object exists", async () => {
    await bucket.put("data/conditional.bin", new Uint8Array([1]));
    const resolver = createR2Resolver(bucket, "catalog");
    const writer = resolver.writer("s3://catalog/data/conditional.bin", { ifNoneMatch: "*" });
    writer.appendUint8(2);
    await expect(Promise.resolve(writer.finish())).rejects.toThrow("Conditional");
  });
});
