// Run with: bun
// Iceberg REST Catalog client for loading and committing table metadata

import type { LoadTableResponse, CommitTableRequest } from "./metadata-types.ts";

interface CatalogConfig {
  readonly catalogUri: string;
  readonly warehouse: string;
  readonly apiToken: string;
}

interface TableIdentifier {
  readonly namespace: string;
  readonly table: string;
}

interface CommitSuccess {
  readonly success: true;
}

interface CommitFailure {
  readonly success: false;
  readonly error: string;
  readonly status: number;
}

type CommitResult = CommitSuccess | CommitFailure;

interface CatalogConfigResponse {
  readonly overrides: { readonly prefix: string };
}

const CONFLICT_STATUS = 409;
const SNAPSHOT_ID_TO_STRING_PATTERN =
  /("(?:snapshot-id|current-snapshot-id|parent-snapshot-id)")\s*:\s*(-?\d+)/g;
const SNAPSHOT_ID_TO_NUMBER_PATTERN =
  /("(?:snapshot-id|current-snapshot-id|parent-snapshot-id)")\s*:\s*"(-?\d+)"/g;

const stringifySnapshotIds = (jsonText: string): string =>
  jsonText.replace(SNAPSHOT_ID_TO_STRING_PATTERN, '$1:"$2"');

const numberifySnapshotIds = (jsonText: string): string =>
  jsonText.replace(SNAPSHOT_ID_TO_NUMBER_PATTERN, "$1:$2");

const buildConfigUrl = (catalogUri: string, warehouse: string): string =>
  `${catalogUri}/v1/config?warehouse=${warehouse}`;

const buildTableUrl = (config: CatalogConfig, id: TableIdentifier): string =>
  `${config.catalogUri}/v1/${config.warehouse}/namespaces/${id.namespace}/tables/${id.table}`;

const fetchCatalogPrefix = async (
  catalogUri: string,
  warehouse: string,
  apiToken: string,
): Promise<string> => {
  const url = buildConfigUrl(catalogUri, warehouse);
  const response = await fetch(url, {
    method: "GET",
    headers: buildAuthHeaders(apiToken),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to fetch catalog config: ${String(response.status)} ${text}`);
  }

  const body = (await response.json()) as CatalogConfigResponse;
  return body.overrides.prefix;
};

const buildAuthHeaders = (apiToken: string): Record<string, string> => ({
  Authorization: `Bearer ${apiToken}`,
  "Content-Type": "application/json",
});

const loadTable = async (
  config: CatalogConfig,
  id: TableIdentifier,
): Promise<LoadTableResponse> => {
  const url = buildTableUrl(config, id);
  const response = await fetch(url, {
    method: "GET",
    headers: buildAuthHeaders(config.apiToken),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Failed to load table ${id.namespace}.${id.table}: ${String(response.status)} ${text}`,
    );
  }

  const text = await response.text();
  const safeText = stringifySnapshotIds(text);
  return JSON.parse(safeText) as LoadTableResponse;
};

const commitTable = async (
  config: CatalogConfig,
  id: TableIdentifier,
  request: CommitTableRequest,
): Promise<CommitResult> => {
  const url = buildTableUrl(config, id);
  const jsonBody = numberifySnapshotIds(JSON.stringify(request));
  const response = await fetch(url, {
    method: "POST",
    headers: buildAuthHeaders(config.apiToken),
    body: jsonBody,
  });

  if (response.status === CONFLICT_STATUS) {
    return {
      success: false,
      error: "Conflict: table was modified concurrently",
      status: CONFLICT_STATUS,
    };
  }

  if (!response.ok) {
    const text = await response.text();
    return {
      success: false,
      error: `Commit failed: ${String(response.status)} ${text}`,
      status: response.status,
    };
  }

  return { success: true };
};

export {
  loadTable,
  commitTable,
  fetchCatalogPrefix,
  buildTableUrl,
  buildConfigUrl,
  buildAuthHeaders,
  stringifySnapshotIds,
  numberifySnapshotIds,
  CONFLICT_STATUS,
};
export type {
  CatalogConfig,
  CatalogConfigResponse,
  TableIdentifier,
  CommitSuccess,
  CommitFailure,
  CommitResult,
};
