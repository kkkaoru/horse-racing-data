// Run with bun. DO-backed run-state helpers for deduplication and progress tracking.
// All state operations route through the PredictRunCoordinator singleton DO,
// which provides strong-consistency (no eventual-consistency race window).

import type { Env } from "./types";

const DO_NAME = "predict-run-coordinator";
const CLAIM_PATH = "/claim";
const COMPLETE_PATH = "/complete";
const STATE_PATH = "/state";
const DO_HOST = "http://do";
const HTTP_OK = 200;

interface ClaimResult {
  proceed: boolean;
  state?: string;
}

interface CompleteParams {
  env: Env;
  runYmd: string;
  category: string;
  status: string;
  racesPredicted: number;
}

interface ClaimParams {
  env: Env;
  runYmd: string;
  category: string;
}

const getCoordinatorStub = (env: Env): DurableObjectStub => {
  const id = env.PREDICT_RUN_COORDINATOR.idFromName(DO_NAME);
  return env.PREDICT_RUN_COORDINATOR.get(id);
};

export const claimRun = async (params: ClaimParams): Promise<ClaimResult> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${CLAIM_PATH}`, {
      body: JSON.stringify({ runYmd: params.runYmd, category: params.category }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO claim failed: ${response.status}`);
  }
  return response.json() as Promise<ClaimResult>;
};

export const completeRun = async (params: CompleteParams): Promise<void> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${COMPLETE_PATH}`, {
      body: JSON.stringify({
        category: params.category,
        racesPredicted: params.racesPredicted,
        runYmd: params.runYmd,
        status: params.status,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO complete failed: ${response.status}`);
  }
};

export const getRunState = async (params: ClaimParams): Promise<unknown> => {
  const stub = getCoordinatorStub(params.env);
  const searchParams = new URLSearchParams({
    category: params.category,
    runYmd: params.runYmd,
  });
  const response = await stub.fetch(
    new Request(`${DO_HOST}${STATE_PATH}?${searchParams.toString()}`),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO getState failed: ${response.status}`);
  }
  return response.json();
};
