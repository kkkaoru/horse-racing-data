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

// wrangler tail's `logs` array often drops structured second-arg objects on
// queue consumers, which made running-style failures look like empty
// `outcome=exception` events. Keep the operator-visible line as one string.
export const formatErrorLogLine = (
  prefix: string,
  extra: Readonly<Record<string, string>>,
  error: unknown,
): string => {
  const fields = errorLogFields(error);
  const extraParts = Object.entries(extra).map(([key, value]) => `${key}=${value}`);
  return [
    prefix,
    ...extraParts,
    `name=${fields.name}`,
    `message=${fields.message}`,
    `stack=${fields.stack}`,
  ].join(" ");
};
