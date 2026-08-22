// Run with bun. DO-backed run-state helpers for deduplication and progress tracking.
// All state operations route through the PredictRunCoordinator singleton DO,
// which provides strong-consistency (no eventual-consistency race window).

import type { ContainerSlotKind } from "./container-slot-cap";
import { resolvePredictDoName } from "./predict-do-shard";
import type { Env } from "./types";

const DO_NAME = "predict-run-coordinator";
const CLAIM_PATH = "/claim";
const COMPLETE_PATH = "/complete";
const STATE_PATH = "/state";
const CLAIM_RACE_PATH = "/claim-race";
const CLAIM_RESCORE_EXECUTION_PATH = "/claim-rescore-execution";
const COMPLETE_RESCORE_RACE_PATH = "/complete-rescore-race";
const CLAIM_FOCUSED_FULL_RACE_PATH = "/claim-focused-full-race";
const RESERVE_FOCUSED_FULL_RACE_ENQUEUE_PATH = "/reserve-focused-full-race-enqueue";
const FAIL_FOCUSED_FULL_RACE_ENQUEUE_PATH = "/fail-focused-full-race-enqueue";
const COMPLETE_FOCUSED_FULL_RACE_PATH = "/complete-focused-full-race";
const CLAIM_CONTAINER_SLOT_PATH = "/claim-container-slot";
const RELEASE_CONTAINER_SLOT_PATH = "/release-container-slot";
const TOUCH_CONTAINER_SLOT_PATH = "/touch-container-slot";
const CLEAR_CONTAINER_SLOT_PATH = "/clear-container-slot";
const CHECK_CONTAINER_SLOT_STOP_PATH = "/check-container-slot-stop";
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

interface ClaimRaceParams {
  env: Env;
  runYmd: string;
  category: string;
  keibajoCode: string;
  raceBango: string;
}

interface ClaimFocusedFullRaceParams extends ClaimRaceParams {
  doName?: string;
  staleAfterMs: number;
  force?: boolean;
  raceStartAtJst?: string;
}

interface CompleteFocusedFullRaceParams extends ClaimRaceParams {
  status: string;
}

interface ReserveFocusedFullRaceEnqueueParams extends ClaimRaceParams {
  raceStartAtJst?: string;
  reservationId: string;
  staleAfterMs: number;
}

interface FailFocusedFullRaceEnqueueParams extends ClaimRaceParams {
  reservationId: string;
}

interface ClaimRescoreExecutionParams extends ClaimRaceParams {
  executionId: string;
  staleAfterMs: number;
}

interface CompleteRescoreRaceParams extends ClaimRaceParams {
  executionId: string;
  status: string;
}

interface ClaimContainerSlotParams {
  allowSameOwner?: boolean;
  category: string;
  doName: string;
  env: Env;
  kind: ContainerSlotKind;
  staleAfterMs: number;
  workKey?: string;
}

interface ReleaseContainerSlotParams {
  doName: string;
  env: Env;
  kind: ContainerSlotKind;
  workKey?: string;
}

interface TouchContainerSlotParams {
  doName: string;
  env: Env;
  staleAfterMs?: number;
  workKey?: string;
}

interface ClearContainerSlotParams {
  doName: string;
  env: Env;
  workKey?: string;
}

interface CheckContainerSlotStopParams {
  doName: string;
  env: Env;
  force?: boolean;
  requestedAt: string;
  workKey?: string;
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

// Per-race rescore claim. Returns proceed:true only for the first caller of a
// (runYmd, category, keibajo, race); later callers get proceed:false so the
// per-race coordinator enqueues each race for rescore at most once per day.
export const claimRescoreRace = async (params: ClaimRaceParams): Promise<ClaimResult> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${CLAIM_RACE_PATH}`, {
      body: JSON.stringify({
        category: params.category,
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        runYmd: params.runYmd,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO claim-race failed: ${response.status}`);
  }
  return response.json() as Promise<ClaimResult>;
};

export const claimRescoreExecution = async (
  params: ClaimRescoreExecutionParams,
): Promise<ClaimResult> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${CLAIM_RESCORE_EXECUTION_PATH}`, {
      body: JSON.stringify({
        category: params.category,
        executionId: params.executionId,
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        runYmd: params.runYmd,
        staleAfterMs: params.staleAfterMs,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO claim-rescore-execution failed: ${response.status}`);
  }
  return response.json() as Promise<ClaimResult>;
};

export const completeRescoreRace = async (params: CompleteRescoreRaceParams): Promise<void> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${COMPLETE_RESCORE_RACE_PATH}`, {
      body: JSON.stringify({
        category: params.category,
        executionId: params.executionId,
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        runYmd: params.runYmd,
        status: params.status,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO complete-rescore-race failed: ${response.status}`);
  }
};

export const checkContainerSlotStop = async (
  params: CheckContainerSlotStopParams,
): Promise<boolean> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${CHECK_CONTAINER_SLOT_STOP_PATH}`, {
      body: JSON.stringify({
        doName: params.doName,
        force: params.force,
        requestedAt: params.requestedAt,
        workKey: params.workKey,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO check-container-slot-stop failed: ${response.status}`);
  }
  const result: { allowed: boolean } = await response.json();
  return result.allowed;
};

export const claimFocusedFullRace = async (
  params: ClaimFocusedFullRaceParams,
): Promise<ClaimResult> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${CLAIM_FOCUSED_FULL_RACE_PATH}`, {
      body: JSON.stringify({
        category: params.category,
        doName:
          params.doName ??
          resolvePredictDoName({
            category: params.category,
            env: params.env,
            keibajoCode: params.keibajoCode,
            raceBango: params.raceBango,
          }),
        force: params.force === true,
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        raceStartAtJst: params.raceStartAtJst,
        runYmd: params.runYmd,
        staleAfterMs: params.staleAfterMs,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO claim-focused-full-race failed: ${response.status}`);
  }
  return response.json() as Promise<ClaimResult>;
};

export const reserveFocusedFullRaceEnqueue = async (
  params: ReserveFocusedFullRaceEnqueueParams,
): Promise<ClaimResult> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${RESERVE_FOCUSED_FULL_RACE_ENQUEUE_PATH}`, {
      body: JSON.stringify({
        category: params.category,
        doName: resolvePredictDoName({
          category: params.category,
          env: params.env,
          keibajoCode: params.keibajoCode,
          raceBango: params.raceBango,
        }),
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        raceStartAtJst: params.raceStartAtJst,
        reservationId: params.reservationId,
        runYmd: params.runYmd,
        staleAfterMs: params.staleAfterMs,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO reserve-focused-full-race-enqueue failed: ${response.status}`);
  }
  return response.json() as Promise<ClaimResult>;
};

export const failFocusedFullRaceEnqueue = async (
  params: FailFocusedFullRaceEnqueueParams,
): Promise<void> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${FAIL_FOCUSED_FULL_RACE_ENQUEUE_PATH}`, {
      body: JSON.stringify({
        category: params.category,
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        reservationId: params.reservationId,
        runYmd: params.runYmd,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO fail-focused-full-race-enqueue failed: ${response.status}`);
  }
};

export const completeFocusedFullRace = async (
  params: CompleteFocusedFullRaceParams,
): Promise<void> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${COMPLETE_FOCUSED_FULL_RACE_PATH}`, {
      body: JSON.stringify({
        category: params.category,
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        runYmd: params.runYmd,
        status: params.status,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO complete-focused-full-race failed: ${response.status}`);
  }
};

export const claimContainerSlot = async (
  params: ClaimContainerSlotParams,
): Promise<ClaimResult> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${CLAIM_CONTAINER_SLOT_PATH}`, {
      body: JSON.stringify({
        allowSameOwner: params.allowSameOwner,
        category: params.category,
        doName: params.doName,
        kind: params.kind,
        staleAfterMs: params.staleAfterMs,
        workKey: params.workKey,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO claim-container-slot failed: ${response.status}`);
  }
  return response.json() as Promise<ClaimResult>;
};

export const releaseContainerSlot = async (params: ReleaseContainerSlotParams): Promise<void> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${RELEASE_CONTAINER_SLOT_PATH}`, {
      body: JSON.stringify({ doName: params.doName, kind: params.kind, workKey: params.workKey }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO release-container-slot failed: ${response.status}`);
  }
};

export const touchContainerSlot = async (params: TouchContainerSlotParams): Promise<void> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${TOUCH_CONTAINER_SLOT_PATH}`, {
      body: JSON.stringify({
        doName: params.doName,
        staleAfterMs: params.staleAfterMs,
        workKey: params.workKey,
      }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO touch-container-slot failed: ${response.status}`);
  }
};

export const clearContainerSlot = async (params: ClearContainerSlotParams): Promise<void> => {
  const stub = getCoordinatorStub(params.env);
  const response = await stub.fetch(
    new Request(`${DO_HOST}${CLEAR_CONTAINER_SLOT_PATH}`, {
      body: JSON.stringify({ doName: params.doName, workKey: params.workKey }),
      headers: { "Content-Type": "application/json" },
      method: "POST",
    }),
  );
  if (response.status !== HTTP_OK) {
    throw new Error(`DO clear-container-slot failed: ${response.status}`);
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
