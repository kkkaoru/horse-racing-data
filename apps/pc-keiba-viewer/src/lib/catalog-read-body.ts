// Run with bun. Shared bounded JSON decoding for private Catalog reads.
import "server-only";

interface ReadState {
  bytes: number;
}

const MAX_BYTES: number = 64 * 1024;
// Enriched day lists exceed 64 KiB in verified historical fixtures (89 races).
// Keep a finite 1 MiB budget, opt-in only; all other readers retain 64 KiB.
const MAX_JOCKEY_DAY_BYTES: number = 1024 * 1024;
const FAILURE: string = "Invalid Catalog response body";

export const readBoundedCatalogBody = async (
  response: Response,
  policy?: "race-day-list-with-jockeys",
): Promise<unknown> => {
  if (response.body === null) throw new Error(FAILURE);
  const decoder: TextDecoder = new TextDecoder("utf-8", { fatal: true });
  const chunks: string[] = [];
  const state: ReadState = { bytes: 0 };
  await response.body.pipeTo(
    new WritableStream<Uint8Array>({
      write(chunk) {
        state.bytes += chunk.byteLength;
        if (
          state.bytes > (policy === "race-day-list-with-jockeys" ? MAX_JOCKEY_DAY_BYTES : MAX_BYTES)
        )
          throw new Error(FAILURE);
        chunks.push(decoder.decode(chunk, { stream: true }));
      },
    }),
  );
  chunks.push(decoder.decode());
  return JSON.parse(chunks.join(""));
};
