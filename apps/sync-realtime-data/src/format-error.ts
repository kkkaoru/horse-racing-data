// Run with bun.
export const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

export type ErrorLogFields = {
  message: string;
  name: string;
  stack: string;
};

export const errorLogFields = (error: unknown): ErrorLogFields => {
  if (!(error instanceof Error)) {
    return { message: String(error), name: "unknown", stack: "" };
  }
  return {
    message: error.message,
    name: error.name,
    stack: typeof error.stack === "string" ? error.stack : "",
  };
};
