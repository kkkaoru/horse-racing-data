// Generated Wasm is bundled statically because Workers forbid runtime compilation of R2 bytes.

import wasmModule from "./generated/jvlink-compatible/jvlink_compatible_bg.wasm";
import {
  buildBootstrapBody as buildBootstrapBodyWasm,
  buildFileListBody as buildFileListBodyWasm,
  buildGateBody as buildGateBodyWasm,
  buildRealtimeAuthorizationBody as buildRealtimeAuthorizationBodyWasm,
  compatibilityVersion,
  decodeBootstrapResponseJson,
  decodeFileListResponseJson,
  decodeJvFile as decodeJvFileWasm,
  deriveDownloadPath as deriveDownloadPathWasm,
  initSync,
} from "./generated/jvlink-compatible/jvlink_compatible.js";

export interface RustBootstrapResponse {
  payFlag: number;
  status: number;
}

export interface RustFileListEntry {
  bytes: number;
  filename: string;
}

export interface RustFileListResponse {
  cd: string;
  files: RustFileListEntry[];
  it: string;
  rm: number;
  rt: number;
  status: number;
  to: string;
}

initSync({ module: wasmModule });

export const RUST_CORE_VERSION: string = compatibilityVersion();

export const buildBootstrapBodyInRust = (
  config: string,
  seeds: readonly [number, number],
): string => buildBootstrapBodyWasm(config, seeds[0], seeds[1]);

interface RustFileListRequest {
  dataSpec: string;
  from: string;
  option: number;
  to: string;
}

export const buildFileListBodyInRust = (
  config: string,
  seeds: readonly [number, number],
  request: RustFileListRequest,
): string =>
  buildFileListBodyWasm(
    config,
    seeds[0],
    seeds[1],
    request.dataSpec,
    request.option,
    request.from,
    request.to,
  );

export const buildRealtimeAuthorizationBodyInRust = (
  config: string,
  seeds: readonly [number, number],
): string => buildRealtimeAuthorizationBodyWasm(config, seeds[0], seeds[1]);

export const buildGateBodyInRust = (config: string, data: string): string =>
  buildGateBodyWasm(config, data);

export const decodeBootstrapInRust = (response: Uint8Array): RustBootstrapResponse =>
  JSON.parse(decodeBootstrapResponseJson(response)) as RustBootstrapResponse;

export const decodeFileListInRust = (response: Uint8Array): RustFileListResponse =>
  JSON.parse(decodeFileListResponseJson(response)) as RustFileListResponse;

export const deriveDownloadPathInRust = (filename: string): string =>
  deriveDownloadPathWasm(filename);

export const decodeJvFileInRust = (file: Uint8Array): Uint8Array => decodeJvFileWasm(file);
