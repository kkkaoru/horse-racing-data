import { retry, type RetryOptions } from "../lib/retry";

const TRANSIENT_ERROR_CODES: ReadonlySet<string> = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
  "EAI_AGAIN",
  "ENOTFOUND",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "EPIPE",
]);

const TRANSIENT_SQLSTATE_PREFIX = "08";
const SERIALIZATION_FAILURE_CODE = "40001";
const DEADLOCK_DETECTED_CODE = "40P01";

interface ErrorWithCode {
  code: string;
}

const isErrorWithCode = (error: unknown): error is ErrorWithCode =>
  typeof error === "object" && error !== null && "code" in error && typeof error.code === "string";

const isTransientMessage = (message: string): boolean => {
  const lowered = message.toLowerCase();
  return (
    lowered.includes("connection terminated") ||
    lowered.includes("connect timeout") ||
    lowered.includes("client has encountered a connection error") ||
    lowered.includes("timed out while waiting for an open slot in the pool") ||
    lowered.includes("hyperdrive") ||
    lowered.includes("could not establish connection") ||
    lowered.includes("socket hang up") ||
    lowered.includes("read econnreset")
  );
};

const isTransientPostgresError = (error: unknown): boolean => {
  if (isErrorWithCode(error)) {
    const { code } = error;
    if (TRANSIENT_ERROR_CODES.has(code)) {
      return true;
    }
    if (code === SERIALIZATION_FAILURE_CODE || code === DEADLOCK_DETECTED_CODE) {
      return true;
    }
    if (code.startsWith(TRANSIENT_SQLSTATE_PREFIX)) {
      return true;
    }
  }
  return error instanceof Error && isTransientMessage(error.message);
};

export const isRetryableDbError = isTransientPostgresError;

export const withDbRetry = <T>(load: () => Promise<T>, options?: RetryOptions): Promise<T> =>
  retry(load, { ...options, shouldRetry: options?.shouldRetry ?? isTransientPostgresError });
