import type { AsyncBuffer } from "hyparquet";
import { ByteWriter } from "hyparquet-writer";

const parseObjectKey = (path: string, bucketName: string): string => {
  const prefix = `s3://${bucketName}/`;
  if (!path.startsWith(prefix)) throw new Error("Iceberg object is outside the bound R2 bucket");
  const key = path.slice(prefix.length);
  if (key.length === 0 || key.includes("..")) throw new Error("Invalid Iceberg object key");
  return key;
};

const notFound = (path: string): Error => new Error(`Iceberg object not found: ${path}`);

export const createR2Resolver = (bucket: R2Bucket, bucketName: string) => ({
  async reader(path: string, byteLength?: number): Promise<AsyncBuffer> {
    const key = parseObjectKey(path, bucketName);
    let knownLength = byteLength;
    if (knownLength === undefined) {
      const object = await bucket.head(key);
      if (object === null) throw notFound(path);
      knownLength = object.size;
    }
    const objectLength = knownLength;
    return {
      byteLength: objectLength,
      async slice(start: number, end?: number): Promise<ArrayBuffer> {
        const rangeEnd = end ?? objectLength;
        if (
          !Number.isInteger(start) ||
          !Number.isInteger(rangeEnd) ||
          start < 0 ||
          rangeEnd < start
        )
          throw new Error("Invalid Iceberg R2 byte range");
        const object = await bucket.get(key, {
          range: { offset: start, length: rangeEnd - start },
        });
        if (object === null) throw notFound(path);
        return await object.arrayBuffer();
      },
    };
  },
  writer(path: string, options?: { ifNoneMatch?: "*" }): ByteWriter {
    const key = parseObjectKey(path, bucketName);
    const writer = new ByteWriter();
    Object.defineProperty(writer, "finish", {
      configurable: true,
      value: async (): Promise<void> => {
        const bytes = writer.getBuffer().slice(0);
        const result =
          options?.ifNoneMatch === "*"
            ? await bucket.put(key, bytes, { onlyIf: { etagDoesNotMatch: "*" } })
            : await bucket.put(key, bytes);
        if (result === null) {
          const error = new Error("Conditional Iceberg object write failed");
          Object.defineProperty(error, "status", { value: 412 });
          throw error;
        }
      },
    });
    return writer;
  },
  async deleter(path: string): Promise<void> {
    await bucket.delete(parseObjectKey(path, bucketName));
  },
});
