// Run with: bun
// Shared type definitions for horse-racing-records worker

type TableName = "horse_racing_records" | "horse_info" | "race_info";

type FilterOperator = "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in" | "like";

interface QueryFilter {
  readonly column: string;
  readonly op: FilterOperator;
  readonly value: string | number | ReadonlyArray<string | number>;
}

interface QueryRequest {
  readonly filters?: ReadonlyArray<QueryFilter>;
  readonly columns?: ReadonlyArray<string>;
  readonly limit?: number;
}

interface DeleteRequest {
  readonly filters?: ReadonlyArray<QueryFilter>;
  readonly ids?: ReadonlyArray<string>;
  readonly confirm: boolean;
}

interface CloudflareBindings {
  readonly R2_BUCKET: R2Bucket;
  readonly CLOUDFLARE_API_TOKEN: string;
  readonly CLOUDFLARE_ACCOUNT_ID: string;
  readonly R2_BUCKET_NAME: string;
  readonly ICEBERG_NAMESPACE: string;
  readonly CATALOG_URI: string;
  readonly R2_SQL_ENDPOINT: string;
  readonly R2_ACCESS_KEY_ID: string;
  readonly R2_SECRET_ACCESS_KEY: string;
  readonly SKIP_MTLS?: string;
}

interface AppEnv {
  readonly Bindings: CloudflareBindings;
}

interface R2SqlResult {
  readonly rows: ReadonlyArray<Record<string, unknown>>;
}

interface R2SqlResponse {
  readonly success: boolean;
  readonly result: R2SqlResult;
  readonly errors: ReadonlyArray<R2SqlError>;
}

interface R2SqlError {
  readonly code: number;
  readonly message: string;
}

interface ApiErrorResponse {
  readonly error: string;
  readonly status: number;
}

interface SchemaField {
  readonly name: string;
  readonly type: string;
  readonly required: boolean;
}

export type {
  TableName,
  FilterOperator,
  QueryFilter,
  QueryRequest,
  DeleteRequest,
  CloudflareBindings,
  AppEnv,
  R2SqlResult,
  R2SqlResponse,
  R2SqlError,
  ApiErrorResponse,
  SchemaField,
};
