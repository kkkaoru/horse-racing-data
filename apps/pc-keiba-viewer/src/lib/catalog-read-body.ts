// Run with bun. Shared bounded JSON decoding for private Catalog reads.
import "server-only";

interface ReadState {
  bytes: number;
}

type CatalogBodyPolicy = "race-day-list-with-jockeys" | "race-history";

const MAX_BYTES: number = 64 * 1024;
// Large responses opt in explicitly. History readers also enforce 4000 rows;
// their valid 13-column payload can exceed 1 MiB. Other readers retain 64 KiB.
const MAX_BYTES_BY_POLICY: Readonly<Record<CatalogBodyPolicy, number>> = {
  "race-day-list-with-jockeys": 1024 * 1024,
  "race-history": 2 * 1024 * 1024,
};
const FAILURE: string = "Invalid Catalog response body";

export const readBoundedCatalogBody = async (
  response: Response,
  policy?: CatalogBodyPolicy,
): Promise<unknown> => {
  if (response.body === null) throw new Error(FAILURE);
  const decoder: TextDecoder = new TextDecoder("utf-8", { fatal: true });
  const chunks: string[] = [];
  const state: ReadState = { bytes: 0 };
  await response.body.pipeTo(
    new WritableStream<Uint8Array>({
      write(chunk) {
        state.bytes += chunk.byteLength;
        if (state.bytes > (policy === undefined ? MAX_BYTES : MAX_BYTES_BY_POLICY[policy]))
          throw new Error(FAILURE);
        chunks.push(decoder.decode(chunk, { stream: true }));
      },
    }),
  );
  chunks.push(decoder.decode());
  return JSON.parse(chunks.join(""));
};
