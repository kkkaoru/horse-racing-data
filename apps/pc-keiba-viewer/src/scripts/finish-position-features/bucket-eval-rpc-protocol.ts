// Run with: imported from bucket-eval-rpc-client.ts and evaluate-bucket-21y.ts (bun runtime)
// JSONL RPC protocol shared between the TS driver and the Python loader
// (load_bucket_predictions.py). One JSON object per line on stdin (requests)
// and stdout (responses). stderr is reserved for Python logs and ignored by
// the driver.

export const RPC_ROWS_CHUNK_SIZE: number = 1000;

export const RPC_REQUEST_TYPE_SQL: string = "sql";
export const RPC_REQUEST_TYPE_COPY: string = "copy";
export const RPC_REQUEST_TYPE_EXIT: string = "exit";

export const RPC_RESPONSE_TYPE_READY: string = "ready";
export const RPC_RESPONSE_TYPE_ROWS: string = "rows";
export const RPC_RESPONSE_TYPE_OK: string = "ok";
export const RPC_RESPONSE_TYPE_ERROR: string = "error";
export const RPC_RESPONSE_TYPE_CLOSED: string = "closed";

export interface RpcSqlRequest {
  type: "sql";
  id: string;
  query: string;
  params: unknown[];
}

export interface RpcCopyRequest {
  type: "copy";
  id: string;
  query: string;
  csv: string;
}

export interface RpcExitRequest {
  type: "exit";
}

export type RpcRequest = RpcSqlRequest | RpcCopyRequest | RpcExitRequest;

export interface RpcReadyResponse {
  type: "ready";
  loadedRows: number;
}

export interface RpcRowsResponse {
  type: "rows";
  id: string;
  seq: number;
  rows: unknown[];
  done: boolean;
}

export interface RpcOkResponse {
  type: "ok";
  id: string;
  rowcount: number;
}

export interface RpcErrorResponse {
  type: "error";
  id: string | null;
  message: string;
  details: string;
}

export interface RpcClosedResponse {
  type: "closed";
}

export type RpcResponse =
  | RpcReadyResponse
  | RpcRowsResponse
  | RpcOkResponse
  | RpcErrorResponse
  | RpcClosedResponse;
