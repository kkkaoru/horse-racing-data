// Run with bun.
// 2026-06-28: handler timeout wrapper.
// Cloudflare Workers cancels a request after ~30s wall clock when the handler
// stops generating progress (the "your Worker's code had hung" error). When
// that happens inside the queue consumer the message is implicitly retried by
// the runtime without our queue handler ever seeing the error, so a stalled
// Browser-binding `page.goto` ties the same queue slot up indefinitely.
//
// Wrap heavy queue branches (Playwright fetch-results, NAR fetch-weights HTTP
// scrape) with this helper to time out a fraction below the 30s runtime cancel.
// On expiry we throw a typed Error so the catch in `handleJob` logs it through
// `logFetch` (observability) and the surrounding `message.retry({ delaySeconds })`
// can recycle the slot cleanly.
const ABORT_SAFETY_MARGIN_MS = 50;

export class HandlerTimeoutError extends Error {
  readonly label: string;
  constructor(label: string) {
    super(`handler timeout: ${label}`);
    this.name = "HandlerTimeoutError";
    this.label = label;
  }
}

export interface HandlerTimeoutInput<T> {
  label: string;
  ms: number;
  task: Promise<T>;
}

export const withHandlerTimeout = <T>(input: HandlerTimeoutInput<T>): Promise<T> => {
  const { label, ms, task } = input;
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new HandlerTimeoutError(label));
    }, ms);
    task.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
};

// Suggested wall-clock budget for queue handlers that wrap a Playwright /
// scrape path. The Workers runtime kills a hung handler at ~30s, so this
// keeps us a measurable margin below that so our logged error wins the race.
export const QUEUE_HANDLER_TIMEOUT_MS = 25_000 - ABORT_SAFETY_MARGIN_MS;
