export type FallbackProvider = "jv" | "nv";
export type WorkerFallbackFetch = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

export interface TriggerResult {
  action: "run";
  provider: FallbackProvider;
  runDate: string;
  runId: string;
}

const isTriggerResult = (value: unknown, provider: FallbackProvider): value is TriggerResult => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  return (
    "action" in value &&
    value.action === "run" &&
    "provider" in value &&
    value.provider === provider &&
    "runDate" in value &&
    typeof value.runDate === "string" &&
    /^20\d{6}$/.test(value.runDate) &&
    "runId" in value &&
    typeof value.runId === "string" &&
    value.runId.length > 0
  );
};

const isRunStatus = (
  value: unknown,
): value is { error_stage: string | null; run_id: string; status: string } =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  "error_stage" in value &&
  (value.error_stage === null || typeof value.error_stage === "string") &&
  "run_id" in value &&
  typeof value.run_id === "string" &&
  "status" in value &&
  typeof value.status === "string";

export const waitForWorkerFallback = async (
  result: TriggerResult,
  baseUrl: string,
  token: string,
  fetcher: WorkerFallbackFetch = fetch,
  delay: (milliseconds: number) => Promise<void> = async (milliseconds) =>
    await new Promise((resolve) => setTimeout(resolve, milliseconds)),
  maxAttempts = 240,
): Promise<void> => {
  const statusUrl = new URL("/admin/status", baseUrl);
  statusUrl.searchParams.set("provider", result.provider);
  statusUrl.searchParams.set("runDate", result.runDate);
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const response = await fetcher(statusUrl, {
      headers: { Authorization: `Bearer ${token}` },
      method: "GET",
    });
    if (response.ok) {
      const value: unknown = await response.json();
      if (!isRunStatus(value))
        throw new Error("daily-keiba-sync status returned an invalid response");
      if (value.run_id === result.runId) {
        if (value.status === "succeeded" || value.status === "succeeded_empty") return;
        if (value.status.endsWith("_failed"))
          throw new Error(`daily-keiba-sync run failed at ${value.error_stage ?? value.status}`);
      }
    } else if (response.status !== 404) {
      throw new Error(`daily-keiba-sync status failed with HTTP ${response.status}`);
    }
    if (attempt < maxAttempts) await delay(5_000);
  }
  throw new Error("daily-keiba-sync run did not complete before the fallback timeout");
};

export const triggerWorkerFallback = async (
  provider: FallbackProvider,
  baseUrl: string | undefined,
  token: string | undefined,
  fetcher: WorkerFallbackFetch = fetch,
): Promise<TriggerResult> => {
  if (baseUrl === undefined || baseUrl === "")
    throw new Error("DAILY_KEIBA_SYNC_BASE_URL is required");
  if (token === undefined || token === "")
    throw new Error("DAILY_KEIBA_SYNC_ADMIN_TOKEN is required");
  const response = await fetcher(new URL("/admin/trigger", baseUrl), {
    body: JSON.stringify({ action: "run", force: true, provider }),
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    method: "POST",
  });
  if (!response.ok) throw new Error(`daily-keiba-sync trigger failed with HTTP ${response.status}`);
  const value: unknown = await response.json();
  if (!isTriggerResult(value, provider))
    throw new Error("daily-keiba-sync trigger returned an invalid response");
  return value;
};
