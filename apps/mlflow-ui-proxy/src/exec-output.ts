// Run with bun. Bound cron work without stopping an interactive MLflow server.
export const SYNC_EXEC_TIMEOUT_MS: number = 10 * 60 * 1000;
const SIGKILL: number = 9;

export const readSyncOutput = async (
  process: Pick<ExecProcess, "output" | "kill">,
): Promise<ExecOutput> => {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => {
    try {
      process.kill(SIGKILL);
    } catch (error) {
      console.warn("[mlflow-sync] timed out exec could not be killed", String(error));
    }
    timeout.reject(new Error("MLflow sync exec exceeded ten minutes"));
  }, SYNC_EXEC_TIMEOUT_MS);
  try {
    return await Promise.race([process.output(), timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
};
