// Run with bun. Public host adapter for the private JV-Link compatibility core.

import {
  buildBootstrapBodyInRust,
  buildFileListBodyInRust,
  decodeBootstrapInRust,
  decodeFileListInRust,
  deriveDownloadPathInRust,
} from "./rust-core";

export type CoreConfig = string;

export interface FileListEntry {
  bytes: number;
  filename: string;
}

export interface FileListResponse {
  cd: string;
  files: FileListEntry[];
  it: string;
  rm: number;
  rt: number;
  status: number;
  to: string;
}

export interface BootstrapResponse {
  payFlag: number;
  status: number;
}

export const randomCredentialSeeds = (): readonly [number, number] => {
  const seeds = crypto.getRandomValues(new Uint8Array(2));
  return [seeds[0]!, seeds[1]!];
};

export const decodeBootstrapResponse = async (response: Uint8Array): Promise<BootstrapResponse> =>
  decodeBootstrapInRust(response);

export const decodeFileListResponse = async (response: Uint8Array): Promise<FileListResponse> =>
  decodeFileListInRust(response);

export const buildBootstrapBody = (config: CoreConfig, seeds: readonly [number, number]): string =>
  buildBootstrapBodyInRust(config, seeds);

export interface FileListRequest {
  dataSpec: string;
  from: string;
  option?: 1 | 2 | 3 | 4;
  to: string;
}

export const buildFileListBody = (
  config: CoreConfig,
  request: FileListRequest,
  seeds: readonly [number, number],
): string =>
  buildFileListBodyInRust(config, seeds, {
    dataSpec: request.dataSpec,
    from: request.from,
    option: request.option ?? 1,
    to: request.to,
  });

export const deriveDownloadPath = (filename: string): string => deriveDownloadPathInRust(filename);
