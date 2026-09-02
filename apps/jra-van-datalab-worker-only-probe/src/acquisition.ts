// Run with bun. Bounded Worker-native JV-Data acquisition using the official SDK wire contract.

import { decodeJvFile } from "./jvfile";
import {
  buildBootstrapBody,
  buildFileListBody,
  randomCredentialSeeds,
  decodeBootstrapResponse,
  decodeFileListResponse,
  deriveDownloadPath,
  type CoreConfig,
  type FileListEntry,
} from "./protocol";

export interface AcquisitionQuery {
  dataSpec: string;
  from: string;
  option?: 1 | 2 | 3 | 4;
  to: string;
}

export interface AcquiredJvFile {
  decoded: Uint8Array;
  fileBytes: number;
  filename: string;
}

export interface JvOpenResult {
  entries: readonly FileListEntry[];
  lastFileTimestamp: string;
  readCount: number;
  transitions: TerminalTransition[];
}

export interface AcquisitionResult {
  decodedBytes: number;
  fileBytes: number;
  filename: string;
  files: readonly AcquiredJvFile[];
  record: Uint8Array;
  recordCount: number;
  transitions: readonly TerminalTransition[];
}

export type TerminalTransition =
  | "configured"
  | "session-encoded"
  | "bootstrapped"
  | "file-listed"
  | "payload-decoded";

export type JvFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

const AUTH_URL: string = "http://authlab.jra-van.ne.jp/Browsing/JVServlet/";
const DATA_ORIGIN: string = "http://datalab.cdn.jra-van.ne.jp";
const FORM_CONTENT_TYPE: string = "application/x-www-form-urlencoded";
const MAX_FILE_BYTES: number = 20 * 1024 * 1024;
const MAX_DATA_SPEC_BYTES: number = 64;
const DATE_TIME_PATTERN: RegExp = /^\d{14}$/;
const DATA_SPEC_PATTERN: RegExp = /^(?:[A-Z0-9]{4})+$/;

const postForm = async (body: string, fetcher: JvFetch): Promise<Uint8Array> => {
  const response = await fetcher(AUTH_URL, {
    body,
    headers: {
      "Content-Type": FORM_CONTENT_TYPE,
      "User-Agent": "UNKNOWN:",
    },
    method: "POST",
    redirect: "manual",
  });
  if (!response.ok) throw new Error("JV authentication endpoint returned an HTTP error");
  return new Uint8Array(await response.arrayBuffer());
};

export const validateAcquisitionQuery = (query: AcquisitionQuery): AcquisitionQuery => {
  if (!DATA_SPEC_PATTERN.test(query.dataSpec) || query.dataSpec.length > MAX_DATA_SPEC_BYTES)
    throw new Error("dataSpec must contain one to sixteen four-character ASCII codes");
  if (!DATE_TIME_PATTERN.test(query.from) || !DATE_TIME_PATTERN.test(query.to))
    throw new Error("from and to must be fourteen digits");
  if (query.from > query.to) throw new Error("from must not be later than to");
  if (query.option !== undefined && ![1, 2, 3, 4].includes(query.option))
    throw new Error("option must be an official JVOpen option");
  return query;
};

export const openJvData = async (
  config: CoreConfig,
  query: AcquisitionQuery,
  fetcher: JvFetch = fetch,
): Promise<JvOpenResult> => {
  const validated = validateAcquisitionQuery(query);
  const transitions: TerminalTransition[] = ["configured"];
  const seeds = randomCredentialSeeds();
  transitions.push("session-encoded");

  const bootstrapBytes = await postForm(buildBootstrapBody(config, seeds), fetcher);
  let bootstrap;
  try {
    bootstrap = await decodeBootstrapResponse(bootstrapBytes);
  } catch (error) {
    throw new Error(`JV bootstrap framing failed (${bootstrapBytes.length} bytes)`, {
      cause: error,
    });
  }
  if (bootstrap.status !== 0) throw new Error("JV bootstrap rejected the terminal state");
  transitions.push("bootstrapped");

  const fileListBytes = await postForm(buildFileListBody(config, validated, seeds), fetcher);
  let fileList;
  try {
    fileList = await decodeFileListResponse(fileListBytes);
  } catch (error) {
    throw new Error(`JV file-list framing failed (${fileListBytes.length} bytes)`, {
      cause: error,
    });
  }
  if (fileList.status !== 0) throw new Error("JV file-list request was rejected");
  if (fileList.files.length === 0) throw new Error("JV file-list returned no files");
  for (const entry of fileList.files)
    if (entry.bytes < 1 || entry.bytes > MAX_FILE_BYTES)
      throw new Error("JV file-list size is outside the bounded limit");
  transitions.push("file-listed");
  return {
    entries: fileList.files,
    lastFileTimestamp: fileList.it,
    readCount: fileList.files.length,
    transitions,
  };
};

export const downloadJvFile = async (
  entry: FileListEntry,
  fetcher: JvFetch = fetch,
): Promise<AcquiredJvFile> => {
  const path = deriveDownloadPath(entry.filename);
  const fileResponse = await fetcher(`${DATA_ORIGIN}${path}`, {
    method: "GET",
    redirect: "manual",
  });
  if (!fileResponse.ok) throw new Error("JV data endpoint returned an HTTP error");
  const file = new Uint8Array(await fileResponse.arrayBuffer());
  if (file.length !== entry.bytes)
    throw new Error("JV data size does not match its file-list entry");
  return {
    decoded: await decodeJvFile(file),
    fileBytes: file.length,
    filename: entry.filename,
  };
};

export const splitJvRecords = (decoded: Uint8Array): readonly Uint8Array[] => {
  const records: Uint8Array[] = [];
  let start = 0;
  for (let index = 0; index + 1 < decoded.length; index += 1) {
    if (decoded[index] !== 0x0d || decoded[index + 1] !== 0x0a) continue;
    records.push(decoded.slice(start, index + 2));
    start = index + 2;
    index += 1;
  }
  if (start !== decoded.length) throw new Error("JV file does not end on a CRLF record boundary");
  return records;
};

export const acquireJvData = async (
  config: CoreConfig,
  query: AcquisitionQuery,
  fetcher: JvFetch = fetch,
): Promise<AcquisitionResult> => {
  const opened = await openJvData(config, query, fetcher);
  const files: AcquiredJvFile[] = [];
  let firstRecord: Uint8Array | undefined;
  let recordCount = 0;
  for (const entry of opened.entries) {
    const file = await downloadJvFile(entry, fetcher);
    files.push(file);
    const records = splitJvRecords(file.decoded);
    firstRecord ??= records[0];
    recordCount += records.length;
  }
  if (firstRecord === undefined) throw new Error("JV files returned no records");
  opened.transitions.push("payload-decoded");
  return {
    decodedBytes: files.reduce((sum, file) => sum + file.decoded.length, 0),
    fileBytes: files.reduce((sum, file) => sum + file.fileBytes, 0),
    filename: files[0]!.filename,
    files,
    record: firstRecord,
    recordCount,
    transitions: opened.transitions,
  };
};
