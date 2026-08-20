// Run with bun. Pure concurrency contract for FinishPositionPredictContainer
// instances. Cloudflare max_instances (wrangler.jsonc) is a platform ceiling,
// not a scheduler. Race-sharded DOs plus weight-triggered rescores can otherwise
// occupy every instance and 503 Ban-ei day-base / focused-full starts.

export type ContainerSlotKind = "day-base" | "focused-full" | "rescore";

export interface ContainerSlotLease {
  category: string;
  doName: string;
  holders: number;
  kind: ContainerSlotKind;
  rescoreHolders: number;
  timestamp: number;
}

export interface ContainerSlotClaimParams {
  category: string;
  doName: string;
  kind: ContainerSlotKind;
  now: number;
  staleAfterMs: number;
}

export interface ContainerSlotClaimDecision {
  leases: ContainerSlotLease[];
  proceed: boolean;
  state?: string;
}

export interface ReservedLaneParams {
  category: string;
  kind: ContainerSlotKind;
}

// Matches wrangler.jsonc containers[0].max_instances. Kept here so the
// software cap and the platform ceiling cannot drift independently of the
// comments that document why 12 stays.
export const CONTAINER_MAX_INSTANCES = 12;
// Ban-ei day-base + Ban-ei focused-full keep two start slots even when JRA/NAR
// rescore/focused-full work has filled the general pool.
export const CONTAINER_RESERVED_HEADROOM = 2;
export const CONTAINER_GENERAL_SLOT_MAX = CONTAINER_MAX_INSTANCES - CONTAINER_RESERVED_HEADROOM;
// One shared category DO per rescore category (jra/nar/ban-ei). Combined with
// unsharded rescore DO names this is also the max concurrent rescore instances.
export const CONTAINER_RESCORE_SLOT_MAX = 3;
export const CONTAINER_SLOT_STALE_MS = 20 * 60 * 1000;
export const CONTAINER_DAY_BASE_SLOT_STALE_MS = 60 * 60 * 1000;
export const CONTAINER_SLOT_RETRY_DELAY_SECONDS = 30;
export const CONTAINER_SLOT_BUSY_STATE = "busy";
export const CONTAINER_SLOT_CAPPED_STATE = "capped";
const BAN_EI_CATEGORY = "ban-ei";
const RESCORE_KIND: ContainerSlotKind = "rescore";
const HOLDER_INCREMENT = 1;
const EMPTY_HOLDER_COUNT = 0;

export const isReservedContainerLane = (params: ReservedLaneParams): boolean =>
  params.category === BAN_EI_CATEGORY && params.kind !== RESCORE_KIND;

export const pruneStaleContainerSlots = (
  leases: readonly ContainerSlotLease[],
  now: number,
  staleAfterMs: number,
): ContainerSlotLease[] => leases.filter((lease) => now - lease.timestamp < staleAfterMs);

const countReservedLeases = (leases: readonly ContainerSlotLease[]): number =>
  leases.filter((lease) => isReservedContainerLane(lease)).length;

const countRescoreDos = (leases: readonly ContainerSlotLease[]): number =>
  leases.filter((lease) => lease.rescoreHolders > EMPTY_HOLDER_COUNT).length;

const replaceLease = (
  leases: readonly ContainerSlotLease[],
  next: ContainerSlotLease,
): ContainerSlotLease[] => leases.map((lease) => (lease.doName === next.doName ? next : lease));

const decideSharedContainerSlotClaim = (
  live: readonly ContainerSlotLease[],
  existing: ContainerSlotLease,
  params: ContainerSlotClaimParams,
): ContainerSlotClaimDecision => {
  if (params.kind === RESCORE_KIND && existing.rescoreHolders > EMPTY_HOLDER_COUNT) {
    return { leases: [...live], proceed: false, state: CONTAINER_SLOT_BUSY_STATE };
  }
  const next: ContainerSlotLease = {
    category: existing.category,
    doName: existing.doName,
    holders: existing.holders + HOLDER_INCREMENT,
    kind: existing.kind,
    rescoreHolders:
      existing.rescoreHolders +
      (params.kind === RESCORE_KIND ? HOLDER_INCREMENT : EMPTY_HOLDER_COUNT),
    timestamp: params.now,
  };
  return { leases: replaceLease(live, next), proceed: true };
};

const decideNewContainerSlotClaim = (
  live: readonly ContainerSlotLease[],
  params: ContainerSlotClaimParams,
): ContainerSlotClaimDecision => {
  if (params.kind === RESCORE_KIND && countRescoreDos(live) >= CONTAINER_RESCORE_SLOT_MAX) {
    return { leases: [...live], proceed: false, state: CONTAINER_SLOT_CAPPED_STATE };
  }
  const reservedCount = countReservedLeases(live);
  const generalCount = live.length - reservedCount;
  const reservedLane = isReservedContainerLane(params);
  if (reservedLane && live.length >= CONTAINER_MAX_INSTANCES) {
    return { leases: [...live], proceed: false, state: CONTAINER_SLOT_CAPPED_STATE };
  }
  if (!reservedLane && generalCount >= CONTAINER_GENERAL_SLOT_MAX) {
    return { leases: [...live], proceed: false, state: CONTAINER_SLOT_CAPPED_STATE };
  }
  const created: ContainerSlotLease = {
    category: params.category,
    doName: params.doName,
    holders: HOLDER_INCREMENT,
    kind: params.kind,
    rescoreHolders: params.kind === RESCORE_KIND ? HOLDER_INCREMENT : EMPTY_HOLDER_COUNT,
    timestamp: params.now,
  };
  return { leases: [...live, created], proceed: true };
};

export const decideContainerSlotClaim = (
  leases: readonly ContainerSlotLease[],
  params: ContainerSlotClaimParams,
): ContainerSlotClaimDecision => {
  const live = pruneStaleContainerSlots(leases, params.now, params.staleAfterMs);
  const existing = live.find((lease) => lease.doName === params.doName);
  return existing === undefined
    ? decideNewContainerSlotClaim(live, params)
    : decideSharedContainerSlotClaim(live, existing, params);
};

export const releaseContainerSlotLease = (
  leases: readonly ContainerSlotLease[],
  doName: string,
  kind: ContainerSlotKind,
  now: number,
  staleAfterMs: number,
): ContainerSlotLease[] => {
  const live = pruneStaleContainerSlots(leases, now, staleAfterMs);
  return live.flatMap((lease) => {
    if (lease.doName !== doName) return [lease];
    const nextHolders = lease.holders - HOLDER_INCREMENT;
    if (nextHolders <= EMPTY_HOLDER_COUNT) return [];
    const next: ContainerSlotLease = {
      category: lease.category,
      doName: lease.doName,
      holders: nextHolders,
      kind: lease.kind,
      rescoreHolders: Math.max(
        EMPTY_HOLDER_COUNT,
        lease.rescoreHolders - (kind === RESCORE_KIND ? HOLDER_INCREMENT : EMPTY_HOLDER_COUNT),
      ),
      timestamp: lease.timestamp,
    };
    return [next];
  });
};

export const touchContainerSlotLease = (
  leases: readonly ContainerSlotLease[],
  doName: string,
  now: number,
  staleAfterMs: number,
): ContainerSlotLease[] => {
  const live = pruneStaleContainerSlots(leases, now, staleAfterMs);
  return live.map((lease) =>
    lease.doName === doName
      ? {
          category: lease.category,
          doName: lease.doName,
          holders: lease.holders,
          kind: lease.kind,
          rescoreHolders: lease.rescoreHolders,
          timestamp: now,
        }
      : lease,
  );
};

export const clearContainerSlotLease = (
  leases: readonly ContainerSlotLease[],
  doName: string,
  now: number,
  staleAfterMs: number,
): ContainerSlotLease[] =>
  pruneStaleContainerSlots(leases, now, staleAfterMs).filter((lease) => lease.doName !== doName);
