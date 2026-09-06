import { optionalLayoutForRecord, parseFixedRecord, rowKey } from "./layouts";
import type { Provider, RecordLayout, RecordRow, RecordValue, TableStage } from "./types";

const MAX_NDJSON_LINE_BYTES = 28 * 1024 * 1024;

interface ParsedTable {
  layout: RecordLayout;
  records: readonly RecordRow[];
}

export interface ParsedSourceStream {
  files: number;
  records: number;
  tables: readonly ParsedTable[];
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const eventName = (value: unknown): string => {
  if (!isObject(value) || typeof value.event !== "string") throw new Error("Invalid source event");
  return value.event;
};

const recordBytes = (value: unknown): Uint8Array => {
  if (
    !isObject(value) ||
    value.encoding !== "base64" ||
    typeof value.data !== "string" ||
    typeof value.bytes !== "number"
  )
    throw new Error("Invalid source record event");
  const binary = atob(value.data);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  if (bytes.length !== value.bytes || value.data.length === 0)
    throw new Error("Invalid source record payload");
  return bytes;
};

class SourceAccumulator {
  readonly #byTable = new Map<string, { layout: RecordLayout; records: Map<string, RecordRow> }>();
  #closeFiles: number | undefined;
  #closeRecords: number | undefined;
  #fileEvents = 0;
  #openEvents = 0;
  #recordEvents = 0;

  constructor(readonly provider: Provider) {}

  push(line: string): void {
    if (line.length === 0) return;
    const value: unknown = JSON.parse(line);
    const event = eventName(value);
    if (event === "open") {
      this.#openEvents += 1;
      return;
    }
    if (event === "file") {
      this.#fileEvents += 1;
      return;
    }
    if (event === "record") {
      const bytes = recordBytes(value);
      this.#recordEvents += 1;
      const layout = optionalLayoutForRecord(this.provider, bytes);
      if (layout === undefined) return;
      const row = parseFixedRecord(layout, bytes);
      let table = this.#byTable.get(layout.tableName);
      if (table === undefined) {
        table = { layout, records: new Map<string, RecordRow>() };
        this.#byTable.set(layout.tableName, table);
      }
      table.records.set(rowKey(layout, row), row);
      return;
    }
    if (event === "close") {
      if (!isObject(value) || typeof value.files !== "number" || typeof value.records !== "number")
        throw new Error("Invalid source close event");
      this.#closeFiles = value.files;
      this.#closeRecords = value.records;
      return;
    }
    throw new Error("Unsupported source event");
  }

  finish(): ParsedSourceStream {
    if (
      this.#openEvents !== 1 ||
      this.#closeFiles === undefined ||
      this.#closeRecords === undefined ||
      this.#closeFiles !== this.#fileEvents ||
      this.#closeRecords !== this.#recordEvents
    )
      throw new Error("Incomplete source stream");

    return {
      files: this.#fileEvents,
      records: this.#recordEvents,
      tables: [...this.#byTable.values()].map((table) => ({
        layout: table.layout,
        records: [...table.records.values()],
      })),
    };
  }
}

export const parseSourceStream = (body: string, provider: Provider): ParsedSourceStream => {
  const accumulator = new SourceAccumulator(provider);
  for (const line of body.split("\n")) accumulator.push(line);
  return accumulator.finish();
};

export const parseSourceReadable = async (
  stream: ReadableStream<Uint8Array>,
  provider: Provider,
  maxLineBytes = MAX_NDJSON_LINE_BYTES,
): Promise<ParsedSourceStream> => {
  if (!Number.isInteger(maxLineBytes) || maxLineBytes < 1)
    throw new Error("Invalid NDJSON line limit");
  const accumulator = new SourceAccumulator(provider);
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const reader = stream.getReader();
  let buffer = "";
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) break;
      buffer += decoder.decode(result.value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (new TextEncoder().encode(line).length > maxLineBytes)
          throw new Error("Source NDJSON line exceeds limit");
        accumulator.push(line);
      }
      if (new TextEncoder().encode(buffer).length > maxLineBytes)
        throw new Error("Source NDJSON line exceeds limit");
    }
    buffer += decoder.decode();
    accumulator.push(buffer);
    return accumulator.finish();
  } finally {
    reader.releaseLock();
  }
};

export const tableStagingKey = (
  provider: Provider,
  runDate: string,
  runId: string,
  tableName: string,
): string => `source-staging/v1/${provider}/${runDate}/${runId}/tables/${tableName}.json`;

export const createTableStage = (
  provider: Provider,
  runId: string,
  tableName: string,
  records: readonly RecordRow[],
): TableStage => ({ formatVersion: 1, provider, records, runId, tableName });

export const parseTableStage = (value: unknown): TableStage => {
  if (
    !isObject(value) ||
    value.formatVersion !== 1 ||
    (value.provider !== "jv" && value.provider !== "nv") ||
    typeof value.runId !== "string" ||
    typeof value.tableName !== "string" ||
    !Array.isArray(value.records)
  )
    throw new Error("Invalid table staging object");
  const records: RecordRow[] = [];
  for (const record of value.records) {
    if (
      !isObject(record) ||
      Object.values(record).some(
        (field) => field !== null && typeof field !== "string" && typeof field !== "number",
      )
    )
      throw new Error("Invalid staged record");
    const fields: Record<string, RecordValue> = {};
    for (const [name, field] of Object.entries(record)) {
      if (field === null || typeof field === "string" || typeof field === "number")
        fields[name] = field;
    }
    records.push(fields);
  }
  return {
    formatVersion: 1,
    provider: value.provider,
    records,
    runId: value.runId,
    tableName: value.tableName,
  };
};
