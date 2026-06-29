// Run with bun. Pure NDJSON stream reader for the /predict endpoint response.
// Each line is a JSON object; progress lines have type "progress", the last
// line has type "result" and carries racesPredicted + category.

const RESULT_TYPE = "result";
const PROGRESS_TYPE = "progress";

interface NdjsonLine {
  type: string;
}

export interface PredictProgressLine extends NdjsonLine {
  type: "progress";
  elapsed?: number;
  elapsed_s?: number;
  message?: string;
  stage?: string;
}

export interface ParseNdjsonStreamOptions {
  onProgress?: (line: PredictProgressLine) => void;
}

export interface PerRaceParquetEntry {
  parquetBase64: string;
  parquetKey: string;
}

export type PredictResultStatus = "success" | "error";

export interface PredictResultLine extends NdjsonLine {
  type: "result";
  racesPredicted: number;
  category: string;
  status?: PredictResultStatus;
  error?: string;
  runDate?: string;
  // Optional Worker-R2-proxy fields: present only on mode=full success when the
  // Container embedded the feature parquet bytes in the NDJSON result line.
  // The Worker DO decodes these and proxies the bytes to FEATURES_CACHE (R2
  // binding) so rescore runs can reuse the parquet without a write-capable S3
  // token in the Container env.
  parquetBase64?: string;
  parquetKey?: string;
  // Per-race feature parquets embedded by the Container; the Worker DO PUTs each
  // to FEATURES_CACHE (R2) for the same reason as the single-parquet fields above.
  perRaceParquets?: PerRaceParquetEntry[];
}

const isResultLine = (line: NdjsonLine): line is PredictResultLine => line.type === RESULT_TYPE;

const isProgressLine = (line: NdjsonLine): line is PredictProgressLine =>
  line.type === PROGRESS_TYPE;

interface ParsedLineState {
  lastLine?: string;
  lastParsed?: NdjsonLine;
}

const processLine = (
  rawLine: string,
  options: ParseNdjsonStreamOptions,
  state: ParsedLineState,
): void => {
  const line = rawLine.trim();
  if (line.length === 0) return;
  const parsed = JSON.parse(line) as NdjsonLine;
  if (isProgressLine(parsed)) options.onProgress?.(parsed);
  state.lastLine = line;
  state.lastParsed = parsed;
};

export const parseNdjsonStream = async (
  body: ReadableStream<Uint8Array>,
  options: ParseNdjsonStreamOptions = {},
): Promise<PredictResultLine> => {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  const state: ParsedLineState = {};
  let buffer = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let newlineIndex = buffer.indexOf("\n");
    while (newlineIndex !== -1) {
      processLine(buffer.slice(0, newlineIndex), options, state);
      buffer = buffer.slice(newlineIndex + 1);
      newlineIndex = buffer.indexOf("\n");
    }
  }
  buffer += decoder.decode();
  processLine(buffer, options, state);
  if (!state.lastLine || !state.lastParsed) throw new Error("Empty NDJSON stream");
  if (!isResultLine(state.lastParsed))
    throw new Error(`Expected result line, got: ${state.lastLine}`);
  return state.lastParsed;
};
