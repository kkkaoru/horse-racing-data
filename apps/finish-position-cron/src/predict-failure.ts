// Run with bun. Normalise a predict-queue / container failure into the fields
// stored on finish_position_predict_retry_errors and
// finish_position_predict_dlq_events. Cloudflare Queues message.retry() cannot
// mutate the original body, so the primary consumer persists this snapshot to
// D1 and the DLQ consumer copies it onto the dead-letter audit row.

export const PREDICT_FAILURE_MESSAGE_MAX_CHARS = 2000;
export const PREDICT_FAILURE_STACK_MAX_CHARS = 4000;
export const PREDICT_FAILURE_BODY_EXCERPT_MAX_CHARS = 2000;

const CONTAINER_HTTP_ERROR_PATTERN = /^Container DO returned (\d{3}):\s*([\s\S]*)$/u;

export interface PredictFailureSnapshot {
  errorName: string | null;
  errorMessage: string | null;
  errorStack: string | null;
  httpStatus: number | null;
  httpBodyExcerpt: string | null;
}

export const truncateFailureText = (value: string, maxChars: number): string =>
  value.length <= maxChars ? value : value.slice(0, maxChars);

const parseContainerHttpFailure = (
  message: string,
): Pick<PredictFailureSnapshot, "httpStatus" | "httpBodyExcerpt"> => {
  const match = CONTAINER_HTTP_ERROR_PATTERN.exec(message);
  if (match === null) {
    return { httpBodyExcerpt: null, httpStatus: null };
  }
  return {
    httpBodyExcerpt: truncateFailureText(match[2] ?? "", PREDICT_FAILURE_BODY_EXCERPT_MAX_CHARS),
    httpStatus: Number(match[1]),
  };
};

export const parsePredictFailure = (err: unknown): PredictFailureSnapshot => {
  if (err instanceof Error) {
    const http = parseContainerHttpFailure(err.message);
    return {
      errorMessage: truncateFailureText(err.message, PREDICT_FAILURE_MESSAGE_MAX_CHARS),
      errorName: err.name,
      errorStack:
        typeof err.stack === "string"
          ? truncateFailureText(err.stack, PREDICT_FAILURE_STACK_MAX_CHARS)
          : null,
      httpBodyExcerpt: http.httpBodyExcerpt,
      httpStatus: http.httpStatus,
    };
  }
  return {
    errorMessage: truncateFailureText(String(err), PREDICT_FAILURE_MESSAGE_MAX_CHARS),
    errorName: null,
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: null,
  };
};
