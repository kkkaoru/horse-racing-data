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
import type { Env, FocusedFullCompletionMessage, FocusedFullWatchTickMessage } from "./types";

const STORAGE_KEY_PREFIX = "run";
const RESCORE_KEY_PREFIX = "rescore";
const FOCUSED_FULL_KEY_PREFIX = "focused-full";
const FOCUSED_FULL_TERMINAL_WATCH_KEY_PREFIX = "focused-full-terminal-watch";
const CLAIM_PATH = "/claim";
const COMPLETE_PATH = "/complete";
const STATE_PATH = "/state";
const CLAIM_RACE_PATH = "/claim-race";
const RELEASE_RACE_CLAIM_PATH = "/release-race-claim";
const CLAIM_RESCORE_EXECUTION_PATH = "/claim-rescore-execution";
const COMPLETE_RESCORE_RACE_PATH = "/complete-rescore-race";
const CLAIM_FOCUSED_FULL_RACE_PATH = "/claim-focused-full-race";
const RESERVE_FOCUSED_FULL_RACE_ENQUEUE_PATH = "/reserve-focused-full-race-enqueue";
const RESERVE_FOCUSED_FULL_RACE_REPAIR_PATH = "/reserve-focused-full-race-repair";
const CANCEL_FOCUSED_FULL_RACE_REPAIR_PATH = "/cancel-focused-full-race-repair";
const FAIL_FOCUSED_FULL_RACE_ENQUEUE_PATH = "/fail-focused-full-race-enqueue";
const COMPLETE_FOCUSED_FULL_RACE_PATH = "/complete-focused-full-race";
const CLAIM_FOCUSED_FULL_TERMINAL_WATCH_PATH = "/claim-focused-full-terminal-watch";
const COMPLETE_FOCUSED_FULL_TERMINAL_WATCH_PATH = "/complete-focused-full-terminal-watch";
const MARK_FOCUSED_FULL_TERMINAL_WATCH_STOPPED_PATH = "/mark-focused-full-terminal-watch-stopped";
const REGISTER_FOCUSED_FULL_WATCH_OUTBOX_PATH = "/register-focused-full-watch-outbox";
const CLEAR_FOCUSED_FULL_WATCH_OUTBOX_PATH = "/clear-focused-full-watch-outbox";
const CLAIM_CONTAINER_SLOT_PATH = "/claim-container-slot";
const RELEASE_CONTAINER_SLOT_PATH = "/release-container-slot";
const TOUCH_CONTAINER_SLOT_PATH = "/touch-container-slot";
const CLEAR_CONTAINER_SLOT_PATH = "/clear-container-slot";
const CHECK_CONTAINER_SLOT_STOP_PATH = "/check-container-slot-stop";
const MARK_CONTAINER_SLOT_STOPPED_PATH = "/mark-container-slot-stopped";
const CONTAINER_SLOTS_KEY = "container-slots";
const CONTAINER_STOP_FENCES_KEY = "container-stop-fences";
const FOCUSED_FULL_WATCH_OUTBOX_KEY = "focused-full-watch-outbox";
const FOCUSED_FULL_ACTIVE_WATCHES_KEY = "focused-full-active-watches";
const FOCUSED_FULL_WATCH_OUTBOX_ALARM_DELAY_MS = 150_000;
const RESCORE_ENQUEUE_CLAIM_STALE_MS = 5 * 60 * 1000;
const RESCORE_EXECUTION_CLAIM_STALE_MS = 31 * 60 * 1000;
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
  containerStoppedAt?: number;
  reservationId?: string;
  executionId?: string;
  claimId?: string;
  // Number of focused-full repair generations after the initial enqueue.
  // This is persisted in the coordinator so a fresh Queue send cannot reset
  // the retry budget by starting a new delivery lineage.
  focusedFullRepairCount?: number;
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
  weightSnapshotCount?: number;
  weightSnapshotFetchedAt?: string;
  weightSnapshotHash?: string;
}

interface ClaimRescoreEnqueueParams extends ClaimRaceParams {
  claimId: string;
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

interface CancelFocusedFullRaceRepairParams extends FailFocusedFullRaceEnqueueParams {
  doName: string;
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

interface ClaimFocusedFullTerminalWatchParams {
  claimId: string;
  staleAfterMs: number;
  watchId: string;
}

interface CompleteFocusedFullTerminalWatchParams {
  claimId: string;
  watchId: string;
}

interface FocusedFullWatchOutboxEntry {
  delaySeconds?: number;
  message: FocusedFullCompletionMessage | FocusedFullWatchTickMessage;
}

interface RegisterFocusedFullWatchOutboxParams extends FocusedFullWatchOutboxEntry {
  outboxId: string;
}

interface ClearFocusedFullWatchOutboxParams {
  outboxId: string;
}

interface ContainerSlotsRecord {
  leases: ContainerSlotLease[];
}

interface ContainerStopFence {
  claimedAtMs?: number;
  destroyedAtMs?: number;
  requestedAtMs: number;
  workKey?: string;
}

const CONTAINER_STOP_FENCE_STALE_MS = 30 * 1000;

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
  replaceWorkKey?: string;
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
  acceptableWorkKeys?: string[];
  doName: string;
  workKey?: string;
}

interface CheckContainerSlotStopParams {
  acceptableWorkKeys?: string[];
  doName: string;
  force?: boolean;
  requestedAt: string;
  workKey?: string;
}

const buildKey = (runYmd: string, category: string): string =>
  `${STORAGE_KEY_PREFIX}:${runYmd}:${category}`;

const weightGenerationKeySuffix = (params: ClaimRaceParams): string =>
  params.weightSnapshotCount === undefined ||
  params.weightSnapshotFetchedAt === undefined ||
  params.weightSnapshotHash === undefined
    ? ""
    : `:${encodeURIComponent(params.weightSnapshotFetchedAt)}:${params.weightSnapshotCount}:${params.weightSnapshotHash}`;

const buildRaceKey = (params: ClaimRaceParams): string =>
  `${RESCORE_KEY_PREFIX}:${params.runYmd}:${params.category}:${params.keibajoCode}:${params.raceBango}${weightGenerationKeySuffix(params)}`;

const buildFocusedFullRaceKey = (params: ClaimRaceParams): string =>
  `${FOCUSED_FULL_KEY_PREFIX}:${params.runYmd}:${params.category}:${params.keibajoCode}:${params.raceBango}`;

const buildFocusedFullTerminalWatchKey = (watchId: string): string =>
  `${FOCUSED_FULL_TERMINAL_WATCH_KEY_PREFIX}:${watchId}`;

const buildFocusedFullLaneKey = (doName: string): string =>
  `${FOCUSED_FULL_KEY_PREFIX}-lane:${encodeURIComponent(doName)}`;

const TERMINAL_STATUSES = new Set(["success"]);
// A focused-full race gets one initial generation and at most two durable
// repair generations. The bound is intentionally held in the DO record, not
// in Queue message attempts (fresh sends reset those attempts).
export const MAX_FOCUSED_FULL_REPAIRS = 2;
const focusedFullRepairFields = (
  record: RunRecord | undefined,
): Pick<RunRecord, "focusedFullRepairCount"> | Record<string, never> =>
  record?.focusedFullRepairCount === undefined
    ? {}
    : { focusedFullRepairCount: record.focusedFullRepairCount };
const resolveRacePriorityMs = (raceStartAtJst: string | undefined, fallback: number): number => {
  if (raceStartAtJst === undefined) return fallback;
  const parsed = Date.parse(raceStartAtJst);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const recordPriorityMs = (record: RunRecord | undefined): number =>
  record?.priorityMs ?? record?.timestamp ?? Number.MAX_SAFE_INTEGER;

const PENDING_RESERVATION_STATUSES = new Set(["enqueued", "queued"]);

const hasEarlierFreshReservation = async (params: {
  category: string;
  currentPriorityMs: number;
  currentRaceKey: string;
  doName: string;
  now: number;
  runYmd: string;
  staleAfterMs: number;
  storage: DurableObjectStorage;
}): Promise<boolean> => {
  const prefix = `${FOCUSED_FULL_KEY_PREFIX}:${params.runYmd}:${params.category}:`;
  const records = await params.storage.list<RunRecord>({ prefix });
  return [...records].some(([candidateKey, candidate]) => {
    if (candidateKey === params.currentRaceKey) return false;
    if (candidate.doName !== params.doName || candidate.reservationId === undefined) return false;
    if (!PENDING_RESERVATION_STATUSES.has(candidate.status)) return false;
    if (params.now - candidate.timestamp >= params.staleAfterMs) return false;
    const candidatePriorityMs = recordPriorityMs(candidate);
    return (
      candidatePriorityMs < params.currentPriorityMs ||
      (candidatePriorityMs === params.currentPriorityMs &&
        candidateKey.localeCompare(params.currentRaceKey) < 0)
    );
  });
};

export class PredictRunCoordinator extends DurableObject<Env> {
  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
  }

  async registerFocusedFullWatchOutbox(
    params: RegisterFocusedFullWatchOutboxParams,
  ): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const outbox =
        (await this.ctx.storage.get<Record<string, FocusedFullWatchOutboxEntry>>(
          FOCUSED_FULL_WATCH_OUTBOX_KEY,
        )) ?? {};
      const entry: FocusedFullWatchOutboxEntry = {
        message: params.message,
        ...(params.delaySeconds === undefined ? {} : { delaySeconds: params.delaySeconds }),
      };
      outbox[params.outboxId] = entry;
      const activeWatches =
        (await this.ctx.storage.get<Record<string, FocusedFullWatchOutboxEntry>>(
          FOCUSED_FULL_ACTIVE_WATCHES_KEY,
        )) ?? {};
      activeWatches[params.message.watchId] = entry;
      await this.ctx.storage.put(FOCUSED_FULL_WATCH_OUTBOX_KEY, outbox);
      await this.ctx.storage.put(FOCUSED_FULL_ACTIVE_WATCHES_KEY, activeWatches);
      await this.ctx.storage.setAlarm(Date.now() + FOCUSED_FULL_WATCH_OUTBOX_ALARM_DELAY_MS);
    });
  }

  async clearFocusedFullWatchOutbox(params: ClearFocusedFullWatchOutboxParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const outbox =
        (await this.ctx.storage.get<Record<string, FocusedFullWatchOutboxEntry>>(
          FOCUSED_FULL_WATCH_OUTBOX_KEY,
        )) ?? {};
      delete outbox[params.outboxId];
      if (Object.keys(outbox).length === 0) {
        await this.ctx.storage.delete(FOCUSED_FULL_WATCH_OUTBOX_KEY);
      } else {
        await this.ctx.storage.put(FOCUSED_FULL_WATCH_OUTBOX_KEY, outbox);
      }
    });
  }

  override async alarm(): Promise<void> {
    const outbox =
      (await this.ctx.storage.get<Record<string, FocusedFullWatchOutboxEntry>>(
        FOCUSED_FULL_WATCH_OUTBOX_KEY,
      )) ?? {};
    const queue = this.env.FOCUSED_FULL_COMPLETION_QUEUE;
    const activeWatches =
      (await this.ctx.storage.get<Record<string, FocusedFullWatchOutboxEntry>>(
        FOCUSED_FULL_ACTIVE_WATCHES_KEY,
      )) ?? {};
    if (Object.keys(outbox).length > 0 || Object.keys(activeWatches).length > 0) {
      await this.ctx.storage.setAlarm(Date.now() + FOCUSED_FULL_WATCH_OUTBOX_ALARM_DELAY_MS);
    }
    if (
      queue === undefined &&
      (Object.keys(outbox).length > 0 || Object.keys(activeWatches).length > 0)
    ) {
      throw new Error("FOCUSED_FULL_COMPLETION_QUEUE binding is missing");
    }
    const sentWatchIds = new Set<string>();
    for (const [outboxId, entry] of Object.entries(outbox)) {
      if (entry.delaySeconds === undefined) await queue?.send(entry.message);
      else await queue?.send(entry.message, { delaySeconds: entry.delaySeconds });
      await this.clearFocusedFullWatchOutbox({ outboxId });
      sentWatchIds.add(entry.message.watchId);
    }
    for (const [watchId, entry] of Object.entries(activeWatches)) {
      if (sentWatchIds.has(watchId)) continue;
      if (entry.delaySeconds === undefined) await queue?.send(entry.message);
      else await queue?.send(entry.message, { delaySeconds: entry.delaySeconds });
    }
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
  // first caller for a weight generation marks it enqueued. A successful
  // generation and a fresh active claim remain deduplicated. Failed work is
  // immediately reclaimable, while an abandoned enqueue/execution eventually
  // expires so a Worker or Queue termination cannot permanently suppress the
  // same generation. blockConcurrencyWhile serialises the read-check-write.
  async claimRace(params: ClaimRescoreEnqueueParams): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const key = buildRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      const now = Date.now();
      const staleAfterMs =
        existing?.status === "started"
          ? RESCORE_EXECUTION_CLAIM_STALE_MS
          : RESCORE_ENQUEUE_CLAIM_STALE_MS;
      const isFreshActive =
        (existing?.status === "enqueued" || existing?.status === "started") &&
        now - existing.timestamp < staleAfterMs;
      if (existing?.status === "success" || isFreshActive) {
        return { proceed: false, state: existing.status };
      }
      await this.ctx.storage.put<RunRecord>(key, {
        claimId: params.claimId,
        status: "enqueued",
        timestamp: now,
      });
      return { proceed: true };
    });
  }

  async releaseRaceClaim(params: ClaimRescoreEnqueueParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const key = buildRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      if (existing?.status !== "enqueued" || existing.claimId !== params.claimId) return;
      await this.ctx.storage.delete(key);
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
        if (
          await hasEarlierFreshReservation({
            category: params.category,
            currentPriorityMs: priorityMs,
            currentRaceKey: raceKey,
            doName,
            now,
            runYmd: params.runYmd,
            staleAfterMs: params.staleAfterMs,
            storage: this.ctx.storage,
          })
        ) {
          await this.ctx.storage.put<RunRecord>(raceKey, {
            doName,
            priorityMs,
            ...focusedFullRepairFields(existing),
            ...(existing?.reservationId === undefined
              ? {}
              : { reservationId: existing.reservationId }),
            status: "queued",
            timestamp: existing?.timestamp ?? now,
          });
          return { proceed: false, state: "queued" };
        }
        await this.ctx.storage.put<RunRecord>(raceKey, {
          doName,
          priorityMs,
          ...focusedFullRepairFields(existing),
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
        if (existing?.status === "ready" || existing?.status === "enqueued") {
          await this.ctx.storage.put<RunRecord>(raceKey, {
            doName,
            priorityMs,
            ...focusedFullRepairFields(existing),
            status: "started",
            timestamp: lane.startedAt,
          });
          return {
            proceed: true,
            state: existing.status === "ready" ? "promoted" : "repair-reserved",
          };
        }
        if (existing?.status === "started" && now - lane.startedAt < params.staleAfterMs) {
          return { proceed: true, state: "resumed" };
        }
        if (lane.waiters.length === 0) {
          await this.ctx.storage.put<RunRecord>(raceKey, {
            doName,
            priorityMs,
            ...focusedFullRepairFields(existing),
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
          ...focusedFullRepairFields(promoted),
          status: "ready",
          timestamp: now,
        });
        await this.ctx.storage.put<RunRecord>(raceKey, {
          doName,
          priorityMs,
          ...focusedFullRepairFields(existing),
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
          ...focusedFullRepairFields(existing),
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
            ...focusedFullRepairFields(staleActive),
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
            ...focusedFullRepairFields(existing),
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
          ...focusedFullRepairFields(promoted),
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
      const priorRepairCount = existing?.focusedFullRepairCount ?? 0;
      const staleReservation = existing?.status === "enqueued" && !freshReservation;
      if (
        (existing?.status === "error" || staleReservation) &&
        priorRepairCount >= MAX_FOCUSED_FULL_REPAIRS
      ) {
        return { proceed: false, state: "repair-budget-exhausted" };
      }
      const repairCount =
        existing?.status === "error" || staleReservation ? priorRepairCount + 1 : priorRepairCount;
      await this.ctx.storage.put<RunRecord>(raceKey, {
        doName: params.doName,
        priorityMs: resolveRacePriorityMs(params.raceStartAtJst, now),
        reservationId: params.reservationId,
        status: "enqueued",
        timestamp: now,
        ...(repairCount === 0 ? {} : { focusedFullRepairCount: repairCount }),
      });
      return { proceed: true };
    });
  }

  async reserveFocusedFullRaceRepair(
    params: ReserveFocusedFullRaceEnqueueParams,
  ): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const raceKey = buildFocusedFullRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(raceKey);
      const now = Date.now();
      const freshReservation =
        existing?.status === "enqueued" && now - existing.timestamp < params.staleAfterMs;
      if (freshReservation) return { proceed: false, state: "enqueued" };
      const priorRepairCount = existing?.focusedFullRepairCount ?? 0;
      if (priorRepairCount >= MAX_FOCUSED_FULL_REPAIRS) {
        return { proceed: false, state: "repair-budget-exhausted" };
      }
      const laneKey = buildFocusedFullLaneKey(params.doName);
      const lane = await this.ctx.storage.get<FocusedFullLaneRecord>(laneKey);
      if (lane !== undefined && lane.activeRaceKey !== raceKey) {
        return { proceed: false, state: "lane-conflict" };
      }
      const startedAt = now;
      await Promise.all([
        this.ctx.storage.put<RunRecord>(raceKey, {
          doName: params.doName,
          priorityMs:
            existing?.priorityMs ?? resolveRacePriorityMs(params.raceStartAtJst, startedAt),
          reservationId: params.reservationId,
          status: "enqueued",
          timestamp: startedAt,
          focusedFullRepairCount: priorRepairCount + 1,
        }),
        this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
          activeRaceKey: raceKey,
          startedAt,
          waiters: lane?.waiters.filter((waiter) => waiter !== raceKey) ?? [],
        }),
      ]);
      return { proceed: true };
    });
  }

  async cancelFocusedFullRaceRepair(params: CancelFocusedFullRaceRepairParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const raceKey = buildFocusedFullRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(raceKey);
      if (existing?.status !== "enqueued" || existing.reservationId !== params.reservationId)
        return;
      const laneKey = buildFocusedFullLaneKey(params.doName);
      const lane = await this.ctx.storage.get<FocusedFullLaneRecord>(laneKey);
      if (lane !== undefined && lane.activeRaceKey !== raceKey) {
        await Promise.all([
          this.ctx.storage.put<RunRecord>(raceKey, {
            doName: params.doName,
            ...(existing.priorityMs === undefined ? {} : { priorityMs: existing.priorityMs }),
            ...focusedFullRepairFields(existing),
            status: "queued",
            timestamp: existing.timestamp,
          }),
          this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
            ...lane,
            waiters: lane.waiters.includes(raceKey) ? lane.waiters : [...lane.waiters, raceKey],
          }),
        ]);
        return;
      }
      const startedAt = lane?.activeRaceKey === raceKey ? lane.startedAt : Date.now();
      const restored: RunRecord = {
        doName: params.doName,
        ...(existing.priorityMs === undefined ? {} : { priorityMs: existing.priorityMs }),
        ...focusedFullRepairFields(existing),
        status: "started",
        timestamp: startedAt,
      };
      if (lane !== undefined) {
        await this.ctx.storage.put<RunRecord>(raceKey, restored);
        return;
      }
      await Promise.all([
        this.ctx.storage.put<RunRecord>(raceKey, restored),
        this.ctx.storage.put<FocusedFullLaneRecord>(laneKey, {
          activeRaceKey: raceKey,
          startedAt,
          waiters: [],
        }),
      ]);
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
        ...focusedFullRepairFields(existing),
        status: "error",
        timestamp: Date.now(),
      });
    });
  }

  async completeFocusedFullRace(params: CompleteFocusedFullRaceParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const raceKey = buildFocusedFullRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(raceKey);
      // Completion is monotonic: a late timeout/error from an older Queue
      // delivery must never downgrade a durable success. Cache-repair callers
      // use reserveFocusedFullRaceRepair, which keeps the reopen explicit and
      // bounded instead of abusing this terminal transition.
      if (existing?.status === "success" && params.status !== "success") return;
      const now = Date.now();
      await this.ctx.storage.put<RunRecord>(raceKey, {
        ...(existing?.doName === undefined ? {} : { doName: existing.doName }),
        ...(existing?.priorityMs === undefined ? {} : { priorityMs: existing.priorityMs }),
        ...focusedFullRepairFields(existing),
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

  async claimFocusedFullTerminalWatch(
    params: ClaimFocusedFullTerminalWatchParams,
  ): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const key = buildFocusedFullTerminalWatchKey(params.watchId);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      if (existing?.status === "terminal") {
        const activeWatches =
          (await this.ctx.storage.get<Record<string, FocusedFullWatchOutboxEntry>>(
            FOCUSED_FULL_ACTIVE_WATCHES_KEY,
          )) ?? {};
        delete activeWatches[params.watchId];
        if (Object.keys(activeWatches).length === 0) {
          await this.ctx.storage.delete(FOCUSED_FULL_ACTIVE_WATCHES_KEY);
        } else {
          await this.ctx.storage.put(FOCUSED_FULL_ACTIVE_WATCHES_KEY, activeWatches);
        }
        return { proceed: false, state: "terminal" };
      }
      const now = Date.now();
      if (
        existing?.status === "processing" &&
        existing.claimId !== params.claimId &&
        now - existing.timestamp < params.staleAfterMs
      ) {
        return { proceed: false, state: "processing" };
      }
      await this.ctx.storage.put<RunRecord>(key, {
        claimId: params.claimId,
        ...(existing?.containerStoppedAt === undefined
          ? {}
          : { containerStoppedAt: existing.containerStoppedAt }),
        status: "processing",
        timestamp: now,
      });
      if (existing?.containerStoppedAt !== undefined) return { proceed: true, state: "stopped" };
      return existing?.status === "processing" && existing.claimId !== params.claimId
        ? { proceed: true, state: "stale" }
        : { proceed: true };
    });
  }

  async completeFocusedFullTerminalWatch(
    params: CompleteFocusedFullTerminalWatchParams,
  ): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const key = buildFocusedFullTerminalWatchKey(params.watchId);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      if (existing?.status !== "processing" || existing.claimId !== params.claimId) return;
      const now = Date.now();
      await this.ctx.storage.put<RunRecord>(key, {
        claimId: params.claimId,
        completedAt: now,
        status: "terminal",
        timestamp: now,
      });
      const activeWatches =
        (await this.ctx.storage.get<Record<string, FocusedFullWatchOutboxEntry>>(
          FOCUSED_FULL_ACTIVE_WATCHES_KEY,
        )) ?? {};
      delete activeWatches[params.watchId];
      if (Object.keys(activeWatches).length === 0) {
        await this.ctx.storage.delete(FOCUSED_FULL_ACTIVE_WATCHES_KEY);
      } else {
        await this.ctx.storage.put(FOCUSED_FULL_ACTIVE_WATCHES_KEY, activeWatches);
      }
    });
  }

  async markFocusedFullTerminalWatchStopped(
    params: CompleteFocusedFullTerminalWatchParams,
  ): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const key = buildFocusedFullTerminalWatchKey(params.watchId);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      if (existing?.status !== "processing" || existing.claimId !== params.claimId) {
        throw new Error(`Focused-full terminal watch ownership lost watchId=${params.watchId}`);
      }
      await this.ctx.storage.put<RunRecord>(key, {
        ...existing,
        containerStoppedAt: Date.now(),
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
        replaceWorkKey: params.replaceWorkKey,
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
        params.acceptableWorkKeys,
      );
      const stopFences =
        (await this.ctx.storage.get<Record<string, ContainerStopFence>>(
          CONTAINER_STOP_FENCES_KEY,
        )) ?? {};
      const fence = stopFences[params.doName];
      const ownerKeys =
        params.acceptableWorkKeys ?? (params.workKey === undefined ? undefined : [params.workKey]);
      if (
        fence !== undefined &&
        (ownerKeys === undefined ||
          (fence.workKey !== undefined && ownerKeys.includes(fence.workKey)))
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
      const existingLease = (record?.leases ?? []).find((lease) => lease.doName === params.doName);
      const ownerKeys =
        params.acceptableWorkKeys ?? (params.workKey === undefined ? undefined : [params.workKey]);
      if (!params.force && ownerKeys === undefined && existingLease !== undefined) return false;
      if (
        existingFence !== undefined &&
        ownerKeys !== undefined &&
        (existingFence.workKey === undefined || !ownerKeys.includes(existingFence.workKey))
      ) {
        return false;
      }
      if (existingFence?.destroyedAtMs !== undefined) return false;
      if (
        existingFence?.claimedAtMs !== undefined &&
        Date.now() - existingFence.claimedAtMs < CONTAINER_STOP_FENCE_STALE_MS
      ) {
        return false;
      }
      const allowed = isContainerSlotStopAllowed(
        record?.leases ?? [],
        params.doName,
        params.workKey,
        Date.now(),
        params.acceptableWorkKeys,
      );
      if (!allowed) return false;
      const requestedAtMs = Date.parse(params.requestedAt);
      if (!params.force && !Number.isFinite(requestedAtMs)) return false;
      if (!params.force && existingLease !== undefined && existingLease.timestamp > requestedAtMs) {
        return false;
      }
      const fenceWorkKey = existingLease?.workKey ?? params.workKey;
      stopFences[params.doName] = {
        claimedAtMs: Date.now(),
        requestedAtMs: params.force ? Date.now() : requestedAtMs,
        ...(fenceWorkKey === undefined ? {} : { workKey: fenceWorkKey }),
      };
      await this.ctx.storage.put(CONTAINER_STOP_FENCES_KEY, stopFences);
      return true;
    });
  }

  async markContainerSlotStopped(params: ClearContainerSlotParams): Promise<void> {
    await this.ctx.blockConcurrencyWhile(async () => {
      const stopFences =
        (await this.ctx.storage.get<Record<string, ContainerStopFence>>(
          CONTAINER_STOP_FENCES_KEY,
        )) ?? {};
      const fence = stopFences[params.doName];
      const ownerKeys =
        params.acceptableWorkKeys ?? (params.workKey === undefined ? undefined : [params.workKey]);
      if (
        fence === undefined ||
        (ownerKeys !== undefined &&
          (fence.workKey === undefined || !ownerKeys.includes(fence.workKey)))
      ) {
        throw new Error(`Container stop fence ownership lost doName=${params.doName}`);
      }
      stopFences[params.doName] = { ...fence, destroyedAtMs: Date.now() };
      await this.ctx.storage.put(CONTAINER_STOP_FENCES_KEY, stopFences);
    });
  }

  private async getContainerSlotStopStage(
    params: CheckContainerSlotStopParams,
  ): Promise<"blocked" | "destroyed" | "pending" | "resumed"> {
    const stopFences =
      (await this.ctx.storage.get<Record<string, ContainerStopFence>>(CONTAINER_STOP_FENCES_KEY)) ??
      {};
    const fence = stopFences[params.doName];
    const ownerKeys =
      params.acceptableWorkKeys ?? (params.workKey === undefined ? undefined : [params.workKey]);
    if (
      fence === undefined ||
      (ownerKeys !== undefined &&
        (fence.workKey === undefined || !ownerKeys.includes(fence.workKey)))
    )
      return "blocked";
    if (fence.destroyedAtMs !== undefined) return "destroyed";
    if (
      fence.claimedAtMs !== undefined &&
      Date.now() - fence.claimedAtMs >= CONTAINER_STOP_FENCE_STALE_MS
    )
      return "resumed";
    return "pending";
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
    const body = (await request.json()) as ClaimRescoreEnqueueParams;
    const result = await this.claimRace(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleReleaseRaceClaim(request: Request): Promise<Response> {
    const body = (await request.json()) as ClaimRescoreEnqueueParams;
    await this.releaseRaceClaim(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
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

  private async handleReserveFocusedFullRaceRepair(request: Request): Promise<Response> {
    const body = (await request.json()) as ReserveFocusedFullRaceEnqueueParams;
    const result = await this.reserveFocusedFullRaceRepair(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleCancelFocusedFullRaceRepair(request: Request): Promise<Response> {
    const body = (await request.json()) as CancelFocusedFullRaceRepairParams;
    await this.cancelFocusedFullRaceRepair(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
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

  private async handleClaimFocusedFullTerminalWatch(request: Request): Promise<Response> {
    const body = (await request.json()) as ClaimFocusedFullTerminalWatchParams;
    const result = await this.claimFocusedFullTerminalWatch(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleCompleteFocusedFullTerminalWatch(request: Request): Promise<Response> {
    const body = (await request.json()) as CompleteFocusedFullTerminalWatchParams;
    await this.completeFocusedFullTerminalWatch(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleMarkFocusedFullTerminalWatchStopped(request: Request): Promise<Response> {
    const body = (await request.json()) as CompleteFocusedFullTerminalWatchParams;
    await this.markFocusedFullTerminalWatchStopped(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleRegisterFocusedFullWatchOutbox(request: Request): Promise<Response> {
    const body = (await request.json()) as RegisterFocusedFullWatchOutboxParams;
    await this.registerFocusedFullWatchOutbox(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleClearFocusedFullWatchOutbox(request: Request): Promise<Response> {
    const body = (await request.json()) as ClearFocusedFullWatchOutboxParams;
    await this.clearFocusedFullWatchOutbox(body);
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
    const previousStage = await this.getContainerSlotStopStage(body);
    const allowed = await this.checkContainerSlotStop(body);
    const state = allowed
      ? previousStage === "resumed"
        ? "resumed"
        : "claimed"
      : await this.getContainerSlotStopStage(body);
    return Response.json({ allowed, state }, { status: HTTP_OK });
  }

  private async handleMarkContainerSlotStopped(request: Request): Promise<Response> {
    const body = (await request.json()) as ClearContainerSlotParams;
    await this.markContainerSlotStopped(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathMethodKey = `${request.method}:${url.pathname}`;
    const handlers = new Map<string, (req: Request) => Promise<Response>>([
      [`POST:${CLAIM_PATH}`, (req) => this.handleClaim(req)],
      [`POST:${COMPLETE_PATH}`, (req) => this.handleComplete(req)],
      [`GET:${STATE_PATH}`, (req) => this.handleState(req)],
      [`POST:${CLAIM_RACE_PATH}`, (req) => this.handleClaimRace(req)],
      [`POST:${RELEASE_RACE_CLAIM_PATH}`, (req) => this.handleReleaseRaceClaim(req)],
      [`POST:${CLAIM_RESCORE_EXECUTION_PATH}`, (req) => this.handleClaimRescoreExecution(req)],
      [`POST:${COMPLETE_RESCORE_RACE_PATH}`, (req) => this.handleCompleteRescoreRace(req)],
      [`POST:${CLAIM_FOCUSED_FULL_RACE_PATH}`, (req) => this.handleClaimFocusedFullRace(req)],
      [
        `POST:${RESERVE_FOCUSED_FULL_RACE_ENQUEUE_PATH}`,
        (req) => this.handleReserveFocusedFullRaceEnqueue(req),
      ],
      [
        `POST:${RESERVE_FOCUSED_FULL_RACE_REPAIR_PATH}`,
        (req) => this.handleReserveFocusedFullRaceRepair(req),
      ],
      [
        `POST:${CANCEL_FOCUSED_FULL_RACE_REPAIR_PATH}`,
        (req) => this.handleCancelFocusedFullRaceRepair(req),
      ],
      [
        `POST:${FAIL_FOCUSED_FULL_RACE_ENQUEUE_PATH}`,
        (req) => this.handleFailFocusedFullRaceEnqueue(req),
      ],
      [`POST:${COMPLETE_FOCUSED_FULL_RACE_PATH}`, (req) => this.handleCompleteFocusedFullRace(req)],
      [
        `POST:${CLAIM_FOCUSED_FULL_TERMINAL_WATCH_PATH}`,
        (req) => this.handleClaimFocusedFullTerminalWatch(req),
      ],
      [
        `POST:${COMPLETE_FOCUSED_FULL_TERMINAL_WATCH_PATH}`,
        (req) => this.handleCompleteFocusedFullTerminalWatch(req),
      ],
      [
        `POST:${MARK_FOCUSED_FULL_TERMINAL_WATCH_STOPPED_PATH}`,
        (req) => this.handleMarkFocusedFullTerminalWatchStopped(req),
      ],
      [
        `POST:${REGISTER_FOCUSED_FULL_WATCH_OUTBOX_PATH}`,
        (req) => this.handleRegisterFocusedFullWatchOutbox(req),
      ],
      [
        `POST:${CLEAR_FOCUSED_FULL_WATCH_OUTBOX_PATH}`,
        (req) => this.handleClearFocusedFullWatchOutbox(req),
      ],
      [`POST:${CLAIM_CONTAINER_SLOT_PATH}`, (req) => this.handleClaimContainerSlot(req)],
      [`POST:${RELEASE_CONTAINER_SLOT_PATH}`, (req) => this.handleReleaseContainerSlot(req)],
      [`POST:${TOUCH_CONTAINER_SLOT_PATH}`, (req) => this.handleTouchContainerSlot(req)],
      [`POST:${CLEAR_CONTAINER_SLOT_PATH}`, (req) => this.handleClearContainerSlot(req)],
      [`POST:${CHECK_CONTAINER_SLOT_STOP_PATH}`, (req) => this.handleCheckContainerSlotStop(req)],
      [
        `POST:${MARK_CONTAINER_SLOT_STOPPED_PATH}`,
        (req) => this.handleMarkContainerSlotStopped(req),
      ],
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
      RELEASE_RACE_CLAIM_PATH,
      CLAIM_RESCORE_EXECUTION_PATH,
      COMPLETE_RESCORE_RACE_PATH,
      CLAIM_FOCUSED_FULL_RACE_PATH,
      RESERVE_FOCUSED_FULL_RACE_ENQUEUE_PATH,
      RESERVE_FOCUSED_FULL_RACE_REPAIR_PATH,
      CANCEL_FOCUSED_FULL_RACE_REPAIR_PATH,
      FAIL_FOCUSED_FULL_RACE_ENQUEUE_PATH,
      COMPLETE_FOCUSED_FULL_RACE_PATH,
      CLAIM_FOCUSED_FULL_TERMINAL_WATCH_PATH,
      COMPLETE_FOCUSED_FULL_TERMINAL_WATCH_PATH,
      MARK_FOCUSED_FULL_TERMINAL_WATCH_STOPPED_PATH,
      REGISTER_FOCUSED_FULL_WATCH_OUTBOX_PATH,
      CLEAR_FOCUSED_FULL_WATCH_OUTBOX_PATH,
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
