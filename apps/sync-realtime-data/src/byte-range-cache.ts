// Run with bun. Invocation-local cache of completed immutable byte ranges.
// No global request state or retained I/O promises. Callers must key immutable
// content (including its ETag) and keep current authorization/freshness checks.
const MAX_CACHE_BYTES: number = 2_097_152;
const MAX_CACHE_ENTRIES: number = 512;

export interface ByteRangeCache {
  read(key: string, load: () => Promise<ArrayBuffer>): Promise<ArrayBuffer>;
}

export const createByteRangeCache = (): ByteRangeCache => {
  const entries: Map<string, ArrayBuffer> = new Map();
  const state: { bytes: number } = { bytes: 0 };
  return {
    async read(key, load) {
      const cached: ArrayBuffer | undefined = entries.get(key);
      if (cached !== undefined) return cached.slice(0);
      const bytes: ArrayBuffer = await load();
      // Another read of the same immutable range may have completed meanwhile.
      if (entries.has(key) || bytes.byteLength > MAX_CACHE_BYTES) return bytes;
      if (entries.size >= MAX_CACHE_ENTRIES || state.bytes + bytes.byteLength > MAX_CACHE_BYTES) {
        entries.clear();
        state.bytes = 0;
      }
      // Neither the loader nor a decoder receives the cache's owned buffer.
      entries.set(key, bytes.slice(0));
      state.bytes += bytes.byteLength;
      return bytes;
    },
  };
};
