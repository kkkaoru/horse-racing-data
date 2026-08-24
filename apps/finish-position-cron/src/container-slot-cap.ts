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
  // Optional for backward compatibility with leases persisted before per-lease
  // expiry was introduced. Missing values are derived from `kind` while the
  // old records drain naturally.
  staleAfterMs?: number;
  // Identifies the execution that owns this DO reservation. Older persisted
  // leases have no workKey and remain releasable during the rolling upgrade.
  workKey?: string;
}

export interface ContainerSlotClaimParams {
  allowSameOwner?: boolean;
  category: string;
  doName: string;
  kind: ContainerSlotKind;
  now: number;
  staleAfterMs: number;
  replaceWorkKey?: string;
  workKey?: string;
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
// comments that document the account's observed effective ceiling.
export const CONTAINER_MAX_INSTANCES = 10;
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

const staleAfterMsForLease = (lease: ContainerSlotLease): number =>
  lease.staleAfterMs ??
  (lease.kind === "day-base" ? CONTAINER_DAY_BASE_SLOT_STALE_MS : CONTAINER_SLOT_STALE_MS);

export const pruneStaleContainerSlots = (
  leases: readonly ContainerSlotLease[],
  now: number,
  _legacyCallerStaleAfterMs?: number,
): ContainerSlotLease[] =>
  leases.filter((lease) => now - lease.timestamp < staleAfterMsForLease(lease));

const countReservedLeases = (leases: readonly ContainerSlotLease[]): number =>
  leases.filter((lease) => isReservedContainerLane(lease)).length;

const countRescoreDos = (leases: readonly ContainerSlotLease[]): number =>
  leases.filter((lease) => lease.rescoreHolders > EMPTY_HOLDER_COUNT).length;

const decideSharedContainerSlotClaim = (
  live: readonly ContainerSlotLease[],
  _existing: ContainerSlotLease,
  _params: ContainerSlotClaimParams,
): ContainerSlotClaimDecision => {
  // A Container process serializes every pipeline execution around shared work
  // directories. Counting another caller as a holder only forwards contention
  // to Python, where it returns `busy`, and permits unfair requeue starvation.
  // Keep the existing owner unchanged and make every other delivery wait here.
  return { leases: [...live], proceed: false, state: CONTAINER_SLOT_BUSY_STATE };
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
  const defaultStaleAfterMs =
    params.kind === "day-base" ? CONTAINER_DAY_BASE_SLOT_STALE_MS : CONTAINER_SLOT_STALE_MS;
  const createdWithoutOptionalFields: ContainerSlotLease = {
    category: params.category,
    doName: params.doName,
    holders: HOLDER_INCREMENT,
    kind: params.kind,
    rescoreHolders: params.kind === RESCORE_KIND ? HOLDER_INCREMENT : EMPTY_HOLDER_COUNT,
    timestamp: params.now,
  };
  const createdWithoutWorkKey: ContainerSlotLease =
    params.staleAfterMs === defaultStaleAfterMs
      ? createdWithoutOptionalFields
      : { ...createdWithoutOptionalFields, staleAfterMs: params.staleAfterMs };
  const created: ContainerSlotLease =
    params.workKey === undefined
      ? createdWithoutWorkKey
      : { ...createdWithoutWorkKey, workKey: params.workKey };
  return { leases: [...live, created], proceed: true };
};

const transferExistingContainerSlotClaim = (
  live: readonly ContainerSlotLease[],
  params: ContainerSlotClaimParams,
): ContainerSlotClaimDecision => ({
  leases: live.map((lease) =>
    lease.doName === params.doName
      ? { ...lease, timestamp: params.now, workKey: params.workKey }
      : lease,
  ),
  proceed: true,
});

export const decideContainerSlotClaim = (
  leases: readonly ContainerSlotLease[],
  params: ContainerSlotClaimParams,
): ContainerSlotClaimDecision => {
  const live = pruneStaleContainerSlots(leases, params.now);
  const existing = live.find((lease) => lease.doName === params.doName);
  if (
    existing !== undefined &&
    params.replaceWorkKey !== undefined &&
    params.workKey !== undefined &&
    existing.workKey === params.replaceWorkKey &&
    existing.workKey !== params.workKey
  ) {
    return transferExistingContainerSlotClaim(live, params);
  }
  if (
    existing !== undefined &&
    params.allowSameOwner === true &&
    params.workKey !== undefined &&
    existing.workKey === params.workKey
  ) {
    return { leases: [...live], proceed: true };
  }
  return existing === undefined
    ? decideNewContainerSlotClaim(live, params)
    : decideSharedContainerSlotClaim(live, existing, params);
};

export const isContainerSlotStopAllowed = (
  leases: readonly ContainerSlotLease[],
  doName: string,
  workKey: string | undefined,
  now: number,
  acceptableWorkKeys?: readonly string[],
): boolean => {
  const ownerKeys = acceptableWorkKeys ?? (workKey === undefined ? undefined : [workKey]);
  if (ownerKeys === undefined) return true;
  const existing = pruneStaleContainerSlots(leases, now).find((lease) => lease.doName === doName);
  return (
    existing !== undefined && existing.workKey !== undefined && ownerKeys.includes(existing.workKey)
  );
};

export const releaseContainerSlotLease = (
  leases: readonly ContainerSlotLease[],
  doName: string,
  kind: ContainerSlotKind,
  now: number,
  workKeyOrLegacyStaleAfterMs?: string | number,
): ContainerSlotLease[] => {
  const live = pruneStaleContainerSlots(leases, now);
  const workKey =
    typeof workKeyOrLegacyStaleAfterMs === "string" ? workKeyOrLegacyStaleAfterMs : undefined;
  return live.flatMap((lease) => {
    if (lease.doName !== doName) return [lease];
    if (workKey !== undefined && lease.workKey !== workKey) {
      return [lease];
    }
    const nextHolders = lease.holders - HOLDER_INCREMENT;
    if (nextHolders <= EMPTY_HOLDER_COUNT) return [];
    const nextWithoutOptionalFields: ContainerSlotLease = {
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
    const nextWithStaleAfterMs: ContainerSlotLease =
      lease.staleAfterMs === undefined
        ? nextWithoutOptionalFields
        : { ...nextWithoutOptionalFields, staleAfterMs: lease.staleAfterMs };
    const next: ContainerSlotLease =
      lease.workKey === undefined
        ? nextWithStaleAfterMs
        : { ...nextWithStaleAfterMs, workKey: lease.workKey };
    return [next];
  });
};

export const touchContainerSlotLease = (
  leases: readonly ContainerSlotLease[],
  doName: string,
  now: number,
  workKeyOrLegacyStaleAfterMs?: string | number,
): ContainerSlotLease[] => {
  const live = pruneStaleContainerSlots(leases, now);
  const workKey =
    typeof workKeyOrLegacyStaleAfterMs === "string" ? workKeyOrLegacyStaleAfterMs : undefined;
  return live.map((lease) =>
    lease.doName === doName && (workKey === undefined || lease.workKey === workKey)
      ? {
          category: lease.category,
          doName: lease.doName,
          holders: lease.holders,
          kind: lease.kind,
          rescoreHolders: lease.rescoreHolders,
          timestamp: now,
          ...(lease.staleAfterMs === undefined ? {} : { staleAfterMs: lease.staleAfterMs }),
          ...(lease.workKey === undefined ? {} : { workKey: lease.workKey }),
        }
      : lease,
  );
};

export const clearContainerSlotLease = (
  leases: readonly ContainerSlotLease[],
  doName: string,
  now: number,
  workKeyOrLegacyStaleAfterMs?: string | number,
  acceptableWorkKeys?: readonly string[],
): ContainerSlotLease[] => {
  const workKey =
    typeof workKeyOrLegacyStaleAfterMs === "string" ? workKeyOrLegacyStaleAfterMs : undefined;
  const ownerKeys = acceptableWorkKeys ?? (workKey === undefined ? undefined : [workKey]);
  return pruneStaleContainerSlots(leases, now).filter(
    (lease) =>
      lease.doName !== doName ||
      (ownerKeys !== undefined &&
        (lease.workKey === undefined || !ownerKeys.includes(lease.workKey))),
  );
};
