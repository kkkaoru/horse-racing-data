// Run with bun. Durable external-write barrier, independent of Container lifetime.
import type { Env } from "./types";

const COMPLETE_ATTEMPTS = 3;

export interface FocusedFullCommitIdentity {
  executionId?: string;
  workKey: string;
}

interface CommitParams<T> extends FocusedFullCommitIdentity {
  env: Pick<Env, "PREDICT_RUN_COORDINATOR">;
  commit: () => Promise<T>;
  enforce?: boolean;
}

export const withFocusedFullCommit = async <T>(params: CommitParams<T>): Promise<T> => {
  if (params.executionId === undefined && params.enforce !== true) return params.commit();
  const token: string = crypto.randomUUID();
  const namespace = params.env.PREDICT_RUN_COORDINATOR;
  const stub = namespace.get(namespace.idFromName("predict-run-coordinator"));
  const request = async (complete: boolean): Promise<void> => {
    const response = await stub.fetch(
      new Request("http://do/external-commit", {
        method: "POST",
        body: JSON.stringify({
          workKey: params.workKey,
          executionId: params.executionId,
          token,
          complete,
        }),
      }),
    );
    if (!response.ok) throw new Error("FOCUSED_FULL_COMMIT_UNRESOLVED");
  };
  // The write has landed once commit() resolves, so a lost completion
  // request (e.g. a coordinator reset on deploy) is safe to resend.
  const complete = async (attemptsLeft: number): Promise<void> =>
    request(true).catch(async (error: unknown) => {
      if (attemptsLeft <= 1) throw error;
      await complete(attemptsLeft - 1);
    });
  await request(false);
  // Never use finally: a rejected put may still land remotely. Retain the barrier.
  try {
    const result: T = await params.commit();
    await complete(COMPLETE_ATTEMPTS);
    return result;
  } catch (cause) {
    throw new Error("FOCUSED_FULL_COMMIT_UNRESOLVED", { cause });
  }
};
