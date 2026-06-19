// Run with bun. Pure NDJSON stream reader for the /predict endpoint response.
// Each line is a JSON object; progress lines have type "progress", the last
// line has type "result" and carries racesPredicted + category.

const RESULT_TYPE = "result";
const TEXT_DECODER = new TextDecoder();

interface NdjsonLine {
  type: string;
}

export interface PredictResultLine extends NdjsonLine {
  type: "result";
  racesPredicted: number;
  category: string;
  // Optional Worker-R2-proxy fields: present only on mode=full success when the
  // Container embedded the feature parquet bytes in the NDJSON result line.
  // The Worker DO decodes these and proxies the bytes to FEATURES_CACHE (R2
  // binding) so rescore runs can reuse the parquet without a write-capable S3
  // token in the Container env.
  parquetBase64?: string;
  parquetKey?: string;
}

const isResultLine = (line: NdjsonLine): line is PredictResultLine => line.type === RESULT_TYPE;

const readAllChunks = async (
  reader: ReadableStreamDefaultReader<Uint8Array>,
  accumulated: string,
): Promise<string> => {
  const { done, value } = await reader.read();
  if (done) return accumulated;
  return readAllChunks(reader, accumulated + TEXT_DECODER.decode(value, { stream: true }));
};

export const parseNdjsonStream = async (
  body: ReadableStream<Uint8Array>,
): Promise<PredictResultLine> => {
  const reader = body.getReader();
  const text = await readAllChunks(reader, "");
  const lines = text
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0);
  const lastLine = lines.at(-1);
  if (!lastLine) throw new Error("Empty NDJSON stream");
  const parsed = JSON.parse(lastLine) as NdjsonLine;
  if (!isResultLine(parsed)) throw new Error(`Expected result line, got: ${lastLine}`);
  return parsed;
};
