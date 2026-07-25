// Run with bun. Shared client for the pc-keiba-r2-catalog Service Binding.

import type { CatalogServiceBinding } from "./types";

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const fetchCatalogRows = async (
  catalog: CatalogServiceBinding,
  url: URL,
): Promise<unknown[]> => {
  const response = await catalog.fetch(new Request(url, { method: "GET" }));
  if (!response.ok) {
    throw new Error(`PC_KEIBA_R2_CATALOG ${url.pathname} failed with HTTP ${response.status}`);
  }
  const payload: unknown = await response.json();
  if (!isRecord(payload) || !Array.isArray(payload.rows)) {
    throw new Error(`PC_KEIBA_R2_CATALOG ${url.pathname} returned invalid rows`);
  }
  return payload.rows;
};
