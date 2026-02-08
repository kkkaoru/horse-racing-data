// Run with: bun
// R2 SQL REST API client for querying Iceberg tables

import type { R2SqlResponse } from "../types.ts";

interface R2SqlConfig {
  readonly endpoint: string;
  readonly accountId: string;
  readonly bucketName: string;
  readonly apiToken: string;
}

interface R2SqlQueryResult {
  readonly success: boolean;
  readonly data: ReadonlyArray<Record<string, unknown>>;
  readonly error?: string;
}

const buildR2SqlUrl = (config: R2SqlConfig): string =>
  `${config.endpoint}/${config.accountId}/r2-sql/query/${config.bucketName}`;

const executeR2SqlQuery = async (config: R2SqlConfig, query: string): Promise<R2SqlQueryResult> => {
  const url = buildR2SqlUrl(config);
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.apiToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
  });

  if (!response.ok) {
    const text = await response.text();
    return { success: false, data: [], error: `R2 SQL request failed: ${response.status} ${text}` };
  }

  const body = (await response.json()) as R2SqlResponse;

  if (!body.success) {
    const errorMessage = body.errors.map((e) => e.message).join(", ");
    return { success: false, data: [], error: `R2 SQL query error: ${errorMessage}` };
  }

  return { success: true, data: body.result.rows };
};

export { executeR2SqlQuery, buildR2SqlUrl };
export type { R2SqlConfig, R2SqlQueryResult };
