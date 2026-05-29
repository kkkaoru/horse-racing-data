// Run with bun. Convert unknown thrown values to readable string for logging.

export const formatError = (error: unknown): string => {
  if (error instanceof Error) {
    return error.stack ?? error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  return JSON.stringify(error);
};
