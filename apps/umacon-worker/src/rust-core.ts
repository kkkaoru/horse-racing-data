// Generated Wasm is bundled statically because Workers forbid runtime compilation of R2 bytes.

import wasmModule from "./generated/nvlink-compatible/nvlink_compatible_bg.wasm";
import {
  buildFileListRequest,
  buildLicenseRequest,
  decodeDataArchive,
  initSync,
  parseFileList,
  verifyLicenseResponse,
} from "./generated/nvlink-compatible/nvlink_compatible.js";

export interface CoreHttpRequest {
  url: string;
  userAgent: string;
}

export interface CoreFileListEntry {
  expandedBytes: number;
  filename: string;
  timestamp: string;
  url: string;
}

const CLIENT_VERSION: string = "3.5.4";
export const RUST_CORE_VERSION: string = "0354-private-core-v1.2";

const isHttpRequest = (value: unknown): value is CoreHttpRequest => {
  if (typeof value !== "object" || value === null) return false;
  return (
    "url" in value &&
    "userAgent" in value &&
    typeof value.url === "string" &&
    typeof value.userAgent === "string"
  );
};

const isFileListEntry = (value: unknown): value is CoreFileListEntry => {
  if (typeof value !== "object" || value === null) return false;
  return (
    "expandedBytes" in value &&
    "filename" in value &&
    "timestamp" in value &&
    "url" in value &&
    typeof value.expandedBytes === "number" &&
    typeof value.filename === "string" &&
    typeof value.timestamp === "string" &&
    typeof value.url === "string"
  );
};

export const parseCoreRequestJson = (json: string): CoreHttpRequest => {
  const value: unknown = JSON.parse(json);
  if (!isHttpRequest(value)) throw new Error("Private NV-Link core returned an invalid request");
  return value;
};

initSync({ module: wasmModule });

export const buildLicenseRequestInRust = (config: string): CoreHttpRequest =>
  parseCoreRequestJson(buildLicenseRequest(config, CLIENT_VERSION));

export const verifyLicenseResponseInRust = (response: Uint8Array): void =>
  verifyLicenseResponse(response);

export const buildFileListRequestInRust = (
  dataSpec: string,
  fromTime: string,
  option: number,
): CoreHttpRequest =>
  parseCoreRequestJson(buildFileListRequest(dataSpec, fromTime, option, CLIENT_VERSION));

export const parseCoreFileListJson = (json: string): CoreFileListEntry[] => {
  const value: unknown = JSON.parse(json);
  if (!Array.isArray(value) || !value.every(isFileListEntry))
    throw new Error("Private NV-Link core returned an invalid file list");
  return value;
};

export const parseFileListInRust = (response: Uint8Array): CoreFileListEntry[] =>
  parseCoreFileListJson(parseFileList(response));

export const decodeDataArchiveInRust = (archive: Uint8Array): Uint8Array =>
  decodeDataArchive(archive);
