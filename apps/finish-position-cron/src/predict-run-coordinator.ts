// Run with bun. Durable Object coordinator for predict-run dedup and state tracking.
// Strong-consistency claim/complete: single DO instance serialises all calls via
// blockConcurrencyWhile.
// - Per-category run key:  run:{runYmd}:{category}
// - Per-race rescore key:  rescore:{runYmd}:{category}:{keibajo}:{race}
//   used by the per-race coordinator to avoid enqueueing the same race twice.
// - Focused full key: focused-full:{runYmd}:{category}:{keibajo}:{race}
//   used by the queue consumer to prevent redelivery from starting a duplicate
//   container pipeline while the original detached build is still in flight.

import { DurableObject } from "cloudflare:workers";
import {
  CONTAINER_SLOT_STALE_MS,
  clearContainerSlotLease,
  decideContainerSlotClaim,
  isContainerSlotStopAllowed,
  releaseContainerSlotLease,
  touchContainerSlotLease,
  type ContainerSlotKind,
  type ContainerSlotLease,
} from "./container-slot-cap";
import type { Env } from "./types";

const STORAGE_KEY_PREFIX = "run";
const RESCORE_KEY_PREFIX = "rescore";
const FOCUSED_FULL_KEY_PREFIX = "focused-full";
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
const CONTAINER_SLOTS_KEY = "container-slots";
const CONTAINER_STOP_FENCES_KEY = "container-stop-fences";
const HTTP_OK = 200;
const HTTP_METHOD_NOT_ALLOWED = 405;
const HTTP_NOT_FOUND = 404;

interface RunRecord {
  status: string;
  timestamp: number;
  doName?: string;
  priorityMs?: number;
  racesPredicted?: number;
  completedAt?: number;
  reservationId?: string;
  executionId?: string;
}

interface ClaimResult {
  proceed: boolean;
  state?: string;
}

interface CompleteParams {
  runYmd: string;
  category: string;
  status: string;
  racesPredicted: number;
}

interface ClaimRaceParams {
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

interface ReserveFocusedFullRaceEnqueueParams extends ClaimRaceParams {
  doName: string;
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

interface CompleteFocusedFullRaceParams extends ClaimRaceParams {
  status: string;
}

interface ContainerSlotsRecord {
  leases: ContainerSlotLease[];
}

interface ContainerStopFence {
  requestedAtMs: number;
  workKey?: string;
}

interface FocusedFullLaneRecord {
  activeRaceKey: string;
  startedAt: number;
  waiters: string[];
}

interface ClaimContainerSlotParams {
  allowSameOwner?: boolean;
  category: string;
  doName: string;
  kind: ContainerSlotKind;
  staleAfterMs?: number;
  workKey?: string;
}

interface ReleaseContainerSlotParams {
  doName: string;
  kind: ContainerSlotKind;
  workKey?: string;
}

interface TouchContainerSlotParams {
  doName: string;
  staleAfterMs?: number;
  workKey?: string;
}

interface ClearContainerSlotParams {
  doName: string;
  workKey?: string;
}

interface CheckContainerSlotStopParams {
  doName: string;
  force?: boolean;
  requestedAt: string;
  workKey?: string;
}

const buildKey = (runYmd: string, category: string): string =>
  `${STORAGE_KEY_PREFIX}:${runYmd}:${category}`;

const buildRaceKey = (params: ClaimRaceParams): string =>
  `${RESCORE_KEY_PREFIX}:${params.runYmd}:${params.category}:${params.keibajoCode}:${params.raceBango}`;

const buildFocusedFullRaceKey = (params: ClaimRaceParams): string =>
  `${FOCUSED_FULL_KEY_PREFIX}:${params.runYmd}:${params.category}:${params.keibajoCode}:${params.raceBango}`;

const buildFocusedFullLaneKey = (doName: string): string =>
  `${FOCUSED_FULL_KEY_PREFIX}-lane:${encodeURIComponent(doName)}`;

const TERMINAL_STATUSES = new Set(["success"]);
const resolveRacePriorityMs = (raceStartAtJst: string | undefined, fallback: number): number => {
  if (raceStartAtJst === undefined) return fallback;
  const parsed = Date.parse(raceStartAtJst);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const recordPriorityMs = (record: RunRecord | undefined): number =>
  record?.priorityMs ?? record?.timestamp ?? Number.MAX_SAFE_INTEGER;

export class PredictRunCoordinator extends DurableObject<Env> {
  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
  }

  async claim(runYmd: string, category: string): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const existing = await this.ctx.storage.get<RunRecord>(buildKey(runYmd, category));
      if (existing && TERMINAL_STATUSES.has(existing.status)) {
        return { proceed: false, state: existing.status };
      }
      await this.ctx.storage.put<RunRecord>(buildKey(runYmd, category), {
        status: "started",
        timestamp: Date.now(),
      });
      return { proceed: true };
    });
  }

  async complete(params: CompleteParams): Promise<void> {
    await this.ctx.storage.put<RunRecord>(buildKey(params.runYmd, params.category), {
      completedAt: Date.now(),
      racesPredicted: params.racesPredicted,
      status: params.status,
      timestamp: Date.now(),
    });
  }

  async getState(runYmd: string, category: string): Promise<RunRecord | undefined> {
    return this.ctx.storage.get<RunRecord>(buildKey(runYmd, category));
  }

  // Strong-consistency per-race claim used by the per-race coordinator. The
  // first caller for a (runYmd, category, keibajo, race) gets proceed:true and
  // the key is marked enqueued; subsequent callers get proceed:false so a race
  // is enqueued for rescore at most once per day. blockConcurrencyWhile
  // serialises the read-check-write so two cron ticks cannot both proceed.
  async claimRace(params: ClaimRaceParams): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const key = buildRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      if (existing !== undefined) {
        return { proceed: false, state: existing.status };
      }
      await this.ctx.storage.put<RunRecord>(key, {
        status: "enqueued",
        timestamp: Date.now(),
      });
      return { proceed: true };
    });
  }

  async claimRescoreExecution(params: ClaimRescoreExecutionParams): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const key = buildRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      const now = Date.now();
      if (existing?.status === "success") return { proceed: false, state: "success" };
      if (existing?.status === "started" && now - existing.timestamp < params.staleAfterMs) {
        return { proceed: false, state: "started" };
      }
      await this.ctx.storage.put<RunRecord>(key, {
        executionId: params.executionId,
        status: "started",
        timestamp: now,
      });
      return existing?.status === "started" ? { proceed: true, state: "stale" } : { proceed: true };
    });
  }

  async completeRescoreRace(params: CompleteRescoreRaceParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const key = buildRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      if (existing?.executionId !== params.executionId) return;
      if (existing.status === "success" && params.status !== "success") return;
      const now = Date.now();
      await this.ctx.storage.put<RunRecord>(key, {
        completedAt: now,
        executionId: params.executionId,
        status: params.status,
        timestamp: now,
      });
    });
  }

  // Redelivery is observation, not progress: a fresh same-race claim keeps its
  // original absolute deadline but proceeds to the downstream workKey-scoped
  // container-slot claim. That gate distinguishes a running execution from a
  // prior capped/stopping attempt that never acquired a slot. Once the lane's
  // deadline expires, the caller may clear and intentionally restart it.
  async claimFocusedFullRace(params: ClaimFocusedFullRaceParams): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const raceKey = buildFocusedFullRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(raceKey);
      const now = Date.now();
      const priorityMs = existing?.priorityMs ?? resolveRacePriorityMs(params.raceStartAtJst, now);
      if (
        existing !== undefined &&
        TERMINAL_STATUSES.has(existing.status) &&
        params.force !== true
      ) {
        return { proceed: false, state: existing.status };
      }
      const doName = params.doName ?? existing?.doName ?? `legacy-${params.category}`;
      const laneKey = buildFocusedFullLaneKey(doName);
      const storedLane = await this.ctx.storage.get<FocusedFullLaneRecord>(laneKey);
      const lane =
        storedLane ??
        (existing?.status === "started"
          ? { activeRaceKey: raceKey, startedAt: existing.timestamp, waiters: [] }
          : undefined);
      if (lane === undefined) {
        await this.ctx.storage.put<RunRecord>(raceKey, {
          doName,
          priorityMs,
          status: "started",
          timestamp: now,
        });
        await this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
          activeRaceKey: raceKey,
          startedAt: now,
          waiters: [],
        });
        return { proceed: true };
      }
      if (lane.activeRaceKey === raceKey) {
        if (existing?.status === "ready") {
          await this.ctx.storage.put<RunRecord>(raceKey, {
            doName,
            priorityMs,
            status: "started",
            timestamp: lane.startedAt,
          });
          return { proceed: true, state: "promoted" };
        }
        if (existing?.status === "started" && now - lane.startedAt < params.staleAfterMs) {
          return { proceed: true, state: "resumed" };
        }
        if (lane.waiters.length === 0) {
          await this.ctx.storage.put<RunRecord>(raceKey, {
            doName,
            priorityMs,
            status: "started",
            timestamp: now,
          });
          await this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
            activeRaceKey: raceKey,
            startedAt: now,
            waiters: [],
          });
          return { proceed: true, state: "stale" };
        }
        const [promotedRaceKey, ...remainingWaiters] = lane.waiters as [string, ...string[]];
        const promoted = await this.ctx.storage.get<RunRecord>(promotedRaceKey);
        await this.ctx.storage.put<RunRecord>(promotedRaceKey, {
          ...(promoted?.doName === undefined ? {} : { doName: promoted.doName }),
          ...(promoted?.priorityMs === undefined ? {} : { priorityMs: promoted.priorityMs }),
          status: "ready",
          timestamp: now,
        });
        await this.ctx.storage.put<RunRecord>(raceKey, {
          doName,
          priorityMs,
          status: "queued",
          timestamp: existing?.timestamp ?? now,
        });
        await this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
          activeRaceKey: promotedRaceKey,
          startedAt: now,
          waiters: [...remainingWaiters, raceKey],
        });
        return { proceed: false, state: "queued" };
      }
      if (existing?.status !== "queued" || existing.doName !== doName) {
        await this.ctx.storage.put<RunRecord>(raceKey, {
          doName,
          priorityMs,
          status: "queued",
          timestamp: existing?.timestamp ?? now,
        });
      }
      const waiters = lane.waiters.includes(raceKey) ? lane.waiters : [...lane.waiters, raceKey];
      const waiterRecords = await Promise.all(
        waiters.map(
          async (waiter) => [waiter, await this.ctx.storage.get<RunRecord>(waiter)] as const,
        ),
      );
      waiterRecords.sort((left, right) => {
        const priorityDelta = recordPriorityMs(left[1]) - recordPriorityMs(right[1]);
        return priorityDelta === 0 ? left[0].localeCompare(right[0]) : priorityDelta;
      });
      if (now - lane.startedAt >= params.staleAfterMs) {
        const promotedRaceKey = waiterRecords[0]?.[0] ?? raceKey;
        const promoted = await this.ctx.storage.get<RunRecord>(promotedRaceKey);
        const staleActive = await this.ctx.storage.get<RunRecord>(lane.activeRaceKey);
        if (!TERMINAL_STATUSES.has(staleActive?.status ?? "")) {
          await this.ctx.storage.put<RunRecord>(lane.activeRaceKey, {
            ...(staleActive?.doName === undefined ? {} : { doName: staleActive.doName }),
            ...(staleActive?.priorityMs === undefined
              ? {}
              : { priorityMs: staleActive.priorityMs }),
            status: "error",
            timestamp: now,
          });
        }
        const remainingWaiters = waiterRecords
          .filter(([waiter]) => waiter !== promotedRaceKey)
          .map(([waiter]) => waiter);
        if (promotedRaceKey === raceKey) {
          await this.ctx.storage.put<RunRecord>(raceKey, {
            doName,
            priorityMs,
            status: "started",
            timestamp: now,
          });
          await this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
            activeRaceKey: raceKey,
            startedAt: now,
            waiters: remainingWaiters,
          });
          return { proceed: true, state: "stale" };
        }
        await this.ctx.storage.put<RunRecord>(promotedRaceKey, {
          ...(promoted?.doName === undefined ? {} : { doName: promoted.doName }),
          ...(promoted?.priorityMs === undefined ? {} : { priorityMs: promoted.priorityMs }),
          status: "ready",
          timestamp: now,
        });
        await this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
          activeRaceKey: promotedRaceKey,
          startedAt: now,
          waiters: remainingWaiters,
        });
        return { proceed: false, state: "queued" };
      }
      await this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
        ...lane,
        waiters: waiterRecords.map(([waiter]) => waiter),
      });
      return { proceed: false, state: "queued" };
    });
  }

  async reserveFocusedFullRaceEnqueue(
    params: ReserveFocusedFullRaceEnqueueParams,
  ): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const raceKey = buildFocusedFullRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(raceKey);
      const now = Date.now();
      const freshReservation =
        existing?.status === "enqueued" && now - existing.timestamp < params.staleAfterMs;
      if (
        freshReservation ||
        (existing !== undefined && existing.status !== "error" && existing.status !== "enqueued")
      ) {
        return { proceed: false, state: existing.status };
      }
      await this.ctx.storage.put<RunRecord>(raceKey, {
        doName: params.doName,
        priorityMs: resolveRacePriorityMs(params.raceStartAtJst, now),
        reservationId: params.reservationId,
        status: "enqueued",
        timestamp: now,
      });
      return { proceed: true };
    });
  }

  async failFocusedFullRaceEnqueue(params: FailFocusedFullRaceEnqueueParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const raceKey = buildFocusedFullRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(raceKey);
      if (existing?.status !== "enqueued" || existing.reservationId !== params.reservationId)
        return;
      await this.ctx.storage.put<RunRecord>(raceKey, {
        ...(existing.doName === undefined ? {} : { doName: existing.doName }),
        ...(existing.priorityMs === undefined ? {} : { priorityMs: existing.priorityMs }),
        status: "error",
        timestamp: Date.now(),
      });
    });
  }

  async completeFocusedFullRace(params: CompleteFocusedFullRaceParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const raceKey = buildFocusedFullRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(raceKey);
      const now = Date.now();
      await this.ctx.storage.put<RunRecord>(raceKey, {
        ...(existing?.doName === undefined ? {} : { doName: existing.doName }),
        ...(existing?.priorityMs === undefined ? {} : { priorityMs: existing.priorityMs }),
        completedAt: now,
        status: params.status,
        timestamp: now,
      });
      if (existing?.doName === undefined) return;
      const laneKey = buildFocusedFullLaneKey(existing.doName);
      const lane = await this.ctx.storage.get<FocusedFullLaneRecord>(laneKey);
      if (lane === undefined) return;
      if (lane.activeRaceKey !== raceKey) {
        if (!lane.waiters.includes(raceKey)) return;
        await this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
          ...lane,
          waiters: lane.waiters.filter((waiter) => waiter !== raceKey),
        });
        return;
      }
      const [promotedRaceKey, ...remainingWaiters] = lane.waiters;
      if (promotedRaceKey === undefined) {
        await this.ctx.storage.delete(laneKey);
        return;
      }
      const promoted = await this.ctx.storage.get<RunRecord>(promotedRaceKey);
      await this.ctx.storage.put<RunRecord>(promotedRaceKey, {
        ...(promoted?.doName === undefined ? {} : { doName: promoted.doName }),
        ...(promoted?.priorityMs === undefined ? {} : { priorityMs: promoted.priorityMs }),
        status: "ready",
        timestamp: now,
      });
      await this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
        activeRaceKey: promotedRaceKey,
        startedAt: now,
        waiters: remainingWaiters,
      });
    });
  }

  async claimContainerSlot(params: ClaimContainerSlotParams): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const record = await this.ctx.storage.get<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY);
      const now = Date.now();
      const stopFences =
        (await this.ctx.storage.get<Record<string, ContainerStopFence>>(
          CONTAINER_STOP_FENCES_KEY,
        )) ?? {};
      if (stopFences[params.doName] !== undefined) {
        await this.ctx.storage.put<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY, {
          leases: record?.leases ?? [],
        });
        return { proceed: false, state: "stopping" };
      }
      const staleAfterMs =
        params.staleAfterMs === undefined ? CONTAINER_SLOT_STALE_MS : params.staleAfterMs;
      const decision = decideContainerSlotClaim(record?.leases ?? [], {
        allowSameOwner: params.allowSameOwner,
        category: params.category,
        doName: params.doName,
        kind: params.kind,
        now,
        staleAfterMs,
        workKey: params.workKey,
      });
      await this.ctx.storage.put<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY, {
        leases: decision.leases,
      });
      return decision.proceed ? { proceed: true } : { proceed: false, state: decision.state };
    });
  }

  async releaseContainerSlot(params: ReleaseContainerSlotParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const record = await this.ctx.storage.get<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY);
      const leases = releaseContainerSlotLease(
        record?.leases ?? [],
        params.doName,
        params.kind,
        Date.now(),
        params.workKey,
      );
      await this.ctx.storage.put<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY, { leases });
    });
  }

  async touchContainerSlot(params: TouchContainerSlotParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const record = await this.ctx.storage.get<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY);
      const leases = touchContainerSlotLease(
        record?.leases ?? [],
        params.doName,
        Date.now(),
        params.workKey,
      );
      await this.ctx.storage.put<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY, { leases });
    });
  }

  async clearContainerSlot(params: ClearContainerSlotParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const record = await this.ctx.storage.get<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY);
      const leases = clearContainerSlotLease(
        record?.leases ?? [],
        params.doName,
        Date.now(),
        params.workKey,
      );
      const stopFences =
        (await this.ctx.storage.get<Record<string, ContainerStopFence>>(
          CONTAINER_STOP_FENCES_KEY,
        )) ?? {};
      const fence = stopFences[params.doName];
      if (
        fence !== undefined &&
        (params.workKey === undefined || fence.workKey === params.workKey)
      ) {
        delete stopFences[params.doName];
      }
      await this.ctx.storage.put<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY, { leases });
      if (Object.keys(stopFences).length === 0) {
        await this.ctx.storage.delete(CONTAINER_STOP_FENCES_KEY);
      } else {
        await this.ctx.storage.put(CONTAINER_STOP_FENCES_KEY, stopFences);
      }
    });
  }

  async checkContainerSlotStop(params: CheckContainerSlotStopParams): Promise<boolean> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const record = await this.ctx.storage.get<ContainerSlotsRecord>(CONTAINER_SLOTS_KEY);
      const stopFences =
        (await this.ctx.storage.get<Record<string, ContainerStopFence>>(
          CONTAINER_STOP_FENCES_KEY,
        )) ?? {};
      const existingFence = stopFences[params.doName];
      if (!params.force && params.workKey === undefined) return false;
      if (
        existingFence !== undefined &&
        params.workKey !== undefined &&
        existingFence.workKey !== params.workKey
      ) {
        return false;
      }
      const allowed = isContainerSlotStopAllowed(
        record?.leases ?? [],
        params.doName,
        params.workKey,
        Date.now(),
      );
      if (!allowed) return false;
      const requestedAtMs = Date.parse(params.requestedAt);
      if (!params.force && !Number.isFinite(requestedAtMs)) return false;
      const existingLease = (record?.leases ?? []).find((lease) => lease.doName === params.doName);
      if (!params.force && existingLease !== undefined && existingLease.timestamp > requestedAtMs) {
        return false;
      }
      stopFences[params.doName] = {
        requestedAtMs: params.force ? Date.now() : requestedAtMs,
        ...(params.workKey === undefined ? {} : { workKey: params.workKey }),
      };
      await this.ctx.storage.put(CONTAINER_STOP_FENCES_KEY, stopFences);
      return true;
    });
  }

  private async handleClaim(request: Request): Promise<Response> {
    const body = (await request.json()) as { runYmd: string; category: string };
    const result = await this.claim(body.runYmd, body.category);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleComplete(request: Request): Promise<Response> {
    const body = (await request.json()) as CompleteParams;
    await this.complete(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleState(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const runYmd = url.searchParams.get("runYmd") ?? "";
    const category = url.searchParams.get("category") ?? "";
    const result = await this.getState(runYmd, category);
    return Response.json({ state: result ?? null }, { status: HTTP_OK });
  }

  private async handleClaimRace(request: Request): Promise<Response> {
    const body = (await request.json()) as ClaimRaceParams;
    const result = await this.claimRace(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleClaimRescoreExecution(request: Request): Promise<Response> {
    const body = (await request.json()) as ClaimRescoreExecutionParams;
    const result = await this.claimRescoreExecution(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleCompleteRescoreRace(request: Request): Promise<Response> {
    const body = (await request.json()) as CompleteRescoreRaceParams;
    await this.completeRescoreRace(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleClaimFocusedFullRace(request: Request): Promise<Response> {
    const body = (await request.json()) as ClaimFocusedFullRaceParams;
    const result = await this.claimFocusedFullRace(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleReserveFocusedFullRaceEnqueue(request: Request): Promise<Response> {
    const body = (await request.json()) as ReserveFocusedFullRaceEnqueueParams;
    const result = await this.reserveFocusedFullRaceEnqueue(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleFailFocusedFullRaceEnqueue(request: Request): Promise<Response> {
    const body = (await request.json()) as FailFocusedFullRaceEnqueueParams;
    await this.failFocusedFullRaceEnqueue(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleCompleteFocusedFullRace(request: Request): Promise<Response> {
    const body = (await request.json()) as CompleteFocusedFullRaceParams;
    await this.completeFocusedFullRace(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleClaimContainerSlot(request: Request): Promise<Response> {
    const body = (await request.json()) as ClaimContainerSlotParams;
    const result = await this.claimContainerSlot(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleReleaseContainerSlot(request: Request): Promise<Response> {
    const body = (await request.json()) as ReleaseContainerSlotParams;
    await this.releaseContainerSlot(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleTouchContainerSlot(request: Request): Promise<Response> {
    const body = (await request.json()) as TouchContainerSlotParams;
    await this.touchContainerSlot(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleClearContainerSlot(request: Request): Promise<Response> {
    const body = (await request.json()) as ClearContainerSlotParams;
    await this.clearContainerSlot(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleCheckContainerSlotStop(request: Request): Promise<Response> {
    const body = (await request.json()) as CheckContainerSlotStopParams;
    return Response.json({ allowed: await this.checkContainerSlotStop(body) }, { status: HTTP_OK });
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathMethodKey = `${request.method}:${url.pathname}`;
    const handlers = new Map<string, (req: Request) => Promise<Response>>([
      [`POST:${CLAIM_PATH}`, (req) => this.handleClaim(req)],
      [`POST:${COMPLETE_PATH}`, (req) => this.handleComplete(req)],
      [`GET:${STATE_PATH}`, (req) => this.handleState(req)],
      [`POST:${CLAIM_RACE_PATH}`, (req) => this.handleClaimRace(req)],
      [`POST:${CLAIM_RESCORE_EXECUTION_PATH}`, (req) => this.handleClaimRescoreExecution(req)],
      [`POST:${COMPLETE_RESCORE_RACE_PATH}`, (req) => this.handleCompleteRescoreRace(req)],
      [`POST:${CLAIM_FOCUSED_FULL_RACE_PATH}`, (req) => this.handleClaimFocusedFullRace(req)],
      [
        `POST:${RESERVE_FOCUSED_FULL_RACE_ENQUEUE_PATH}`,
        (req) => this.handleReserveFocusedFullRaceEnqueue(req),
      ],
      [
        `POST:${FAIL_FOCUSED_FULL_RACE_ENQUEUE_PATH}`,
        (req) => this.handleFailFocusedFullRaceEnqueue(req),
      ],
      [`POST:${COMPLETE_FOCUSED_FULL_RACE_PATH}`, (req) => this.handleCompleteFocusedFullRace(req)],
      [`POST:${CLAIM_CONTAINER_SLOT_PATH}`, (req) => this.handleClaimContainerSlot(req)],
      [`POST:${RELEASE_CONTAINER_SLOT_PATH}`, (req) => this.handleReleaseContainerSlot(req)],
      [`POST:${TOUCH_CONTAINER_SLOT_PATH}`, (req) => this.handleTouchContainerSlot(req)],
      [`POST:${CLEAR_CONTAINER_SLOT_PATH}`, (req) => this.handleClearContainerSlot(req)],
      [`POST:${CHECK_CONTAINER_SLOT_STOP_PATH}`, (req) => this.handleCheckContainerSlotStop(req)],
    ]);
    const handler = handlers.get(pathMethodKey);
    if (handler) {
      return handler(request);
    }
    const knownPaths = new Set([
      CLAIM_PATH,
      COMPLETE_PATH,
      STATE_PATH,
      CLAIM_RACE_PATH,
      CLAIM_RESCORE_EXECUTION_PATH,
      COMPLETE_RESCORE_RACE_PATH,
      CLAIM_FOCUSED_FULL_RACE_PATH,
      RESERVE_FOCUSED_FULL_RACE_ENQUEUE_PATH,
      FAIL_FOCUSED_FULL_RACE_ENQUEUE_PATH,
      COMPLETE_FOCUSED_FULL_RACE_PATH,
      CLAIM_CONTAINER_SLOT_PATH,
      RELEASE_CONTAINER_SLOT_PATH,
      TOUCH_CONTAINER_SLOT_PATH,
      CLEAR_CONTAINER_SLOT_PATH,
    ]);
    return knownPaths.has(url.pathname)
      ? Response.json({ error: "Method not allowed" }, { status: HTTP_METHOD_NOT_ALLOWED })
      : Response.json({ error: "Not found" }, { status: HTTP_NOT_FOUND });
  }
}
