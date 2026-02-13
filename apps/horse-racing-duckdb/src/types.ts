// Run with: bun
// Type definitions for horse-racing-duckdb worker

interface DuckDBBindings {
  readonly [key: string]: unknown;
}

interface DuckDBAppEnv {
  readonly Bindings: DuckDBBindings;
}

interface CreateDeleteParquetRequest {
  readonly deleteIds: ReadonlyArray<string>;
  readonly columnName: string;
}

export type { DuckDBBindings, DuckDBAppEnv, CreateDeleteParquetRequest };
