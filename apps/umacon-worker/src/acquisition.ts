// Run with bun. Acquire official UmaConn stored data through private core request builders.

import {
  buildFileListRequestInRust,
  buildLicenseRequestInRust,
  decodeDataArchiveInRust,
  parseFileListInRust,
  verifyLicenseResponseInRust,
  type CoreFileListEntry,
  type CoreHttpRequest,
} from "./rust-core";
import { socketFetch, SocketFallbackError, type SocketFailureStage } from "./socket-fetch";

export interface AcquisitionQuery {
  dataSpec: string;
  fromTime: string;
  option: number;
}

export interface OpenedAcquisition {
  entries: CoreFileListEntry[];
  transitions: string[];
}

export interface DownloadedFile {
  archiveBytes: number;
  decoded: Uint8Array;
  filename: string;
}

export type AcquisitionStage =
  | "license-build"
  | "license-fetch"
  | "license-verify"
  | "file-list-build"
  | "file-list-fetch"
  | "file-list-parse"
  | "archive-fetch"
  | "archive-decode";

export class AcquisitionError extends Error {
  readonly socketBytes?: number;
  readonly socketStage?: SocketFailureStage;
  readonly stage: AcquisitionStage;
  readonly upstreamStatus?: number;

  constructor(
    stage: AcquisitionStage,
    upstreamStatus?: number,
    socketStage?: SocketFailureStage,
    socketBytes?: number,
  ) {
    super("NV-Link acquisition stage failed");
    this.name = "AcquisitionError";
    this.socketBytes = socketBytes;
    this.socketStage = socketStage;
    this.stage = stage;
    this.upstreamStatus = upstreamStatus;
  }
}

class UpstreamHttpError extends Error {
  readonly status: number;

  constructor(status: number) {
    super("NV-Link upstream returned an HTTP error");
    this.status = status;
  }
}

export interface FetchBytesOptions {
  fetcher: NvFetch;
  maxBytes: number;
  request: CoreHttpRequest;
}

export type NvFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

const MAX_LICENSE_BYTES: number = 64;
const MAX_FILE_LIST_BYTES: number = 1024 * 1024;
const MAX_ARCHIVE_BYTES: number = 16 * 1024 * 1024;
const CR: number = 13;
const LF: number = 10;

const atStage = async <T>(stage: AcquisitionStage, operation: () => T | Promise<T>): Promise<T> => {
  try {
    return await operation();
  } catch (error: unknown) {
    throw new AcquisitionError(
      stage,
      error instanceof UpstreamHttpError ? error.status : undefined,
      error instanceof SocketFallbackError ? error.stage : undefined,
      error instanceof SocketFallbackError ? error.bytesRead : undefined,
    );
  }
};

export const fetchBounded = async (options: FetchBytesOptions): Promise<Uint8Array> => {
  let response: Response;
  try {
    response = await options.fetcher(options.request.url, {
      headers: { "User-Agent": options.request.userAgent },
      redirect: "error",
    });
  } catch {
    response = await socketFetch(options.request, options.maxBytes);
  }
  if (!response.ok) throw new UpstreamHttpError(response.status);
  const declaredLength: number = Number(response.headers.get("Content-Length") ?? 0);
  if (declaredLength > options.maxBytes) throw new Error("UmaConn upstream response is too large");
  const bytes: Uint8Array = new Uint8Array(await response.arrayBuffer());
  if (bytes.length > options.maxBytes)
    throw new Error("UmaConn upstream response exceeded its limit");
  return bytes;
};

export const authorizeAcquisition = async (
  config: string,
  fetcher: NvFetch = fetch,
): Promise<void> => {
  const request: CoreHttpRequest = await atStage("license-build", () =>
    buildLicenseRequestInRust(config),
  );
  const response: Uint8Array = await atStage("license-fetch", () =>
    fetchBounded({ fetcher, maxBytes: MAX_LICENSE_BYTES, request }),
  );
  await atStage("license-verify", () => verifyLicenseResponseInRust(response));
};

export const openAcquisition = async (
  config: string,
  query: AcquisitionQuery,
  fetcher: NvFetch = fetch,
): Promise<OpenedAcquisition> => {
  await authorizeAcquisition(config, fetcher);
  const request: CoreHttpRequest = await atStage("file-list-build", () =>
    buildFileListRequestInRust(query.dataSpec, query.fromTime, query.option),
  );
  const response: Uint8Array = await atStage("file-list-fetch", () =>
    fetchBounded({ fetcher, maxBytes: MAX_FILE_LIST_BYTES, request }),
  );
  const entries: CoreFileListEntry[] = await atStage("file-list-parse", () =>
    parseFileListInRust(response),
  );
  return {
    entries,
    transitions: ["license-authorized", "file-list-received"],
  };
};

export const downloadFile = async (
  entry: CoreFileListEntry,
  fetcher: NvFetch = fetch,
): Promise<DownloadedFile> => {
  const archive: Uint8Array = await atStage("archive-fetch", () =>
    fetchBounded({
      fetcher,
      maxBytes: MAX_ARCHIVE_BYTES,
      request: { url: entry.url, userAgent: "UmaConn/3.5.4" },
    }),
  );
  const decoded: Uint8Array = await atStage("archive-decode", () =>
    decodeDataArchiveInRust(archive),
  );
  return {
    archiveBytes: archive.length,
    decoded,
    filename: entry.filename,
  };
};

export const splitRecords = (bytes: Uint8Array): Uint8Array[] => {
  const records: Uint8Array[] = [];
  let start: number = 0;
  for (let index: number = 0; index + 1 < bytes.length; index += 1) {
    if (bytes[index] !== CR || bytes[index + 1] !== LF) continue;
    records.push(bytes.slice(start, index + 2));
    start = index + 2;
    index += 1;
  }
  if (start !== bytes.length)
    throw new Error("NV-Link data does not end on a CRLF record boundary");
  return records;
};
