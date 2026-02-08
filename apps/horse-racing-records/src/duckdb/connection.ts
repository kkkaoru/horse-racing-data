// Run with: bun
// DuckDB WASM lazy initialization and connection management

import { init, DuckDB } from "@ducklings/workers";
import wasmModule from "./duckdb-workers.wasm";
import type { Connection } from "@ducklings/workers";

interface DuckDBInstanceWrapper {
  readonly connect: () => Connection;
  readonly close: () => void;
}

const createDuckDBInstance = async (): Promise<DuckDBInstanceWrapper> => {
  await init({ wasmModule });

  const db = new DuckDB();

  return {
    connect: () => db.connect(),
    close: () => {
      db.close();
    },
  };
};

export { createDuckDBInstance };
export type { DuckDBInstanceWrapper };
