// Run with bun. Tests for the container instance concurrency contract.

import { expect, test } from "vitest";
import {
  CONTAINER_DAY_BASE_SLOT_STALE_MS,
  CONTAINER_GENERAL_SLOT_MAX,
  CONTAINER_MAX_INSTANCES,
  CONTAINER_RESERVED_HEADROOM,
  CONTAINER_RESCORE_SLOT_MAX,
  CONTAINER_SLOT_BUSY_STATE,
  CONTAINER_SLOT_CAPPED_STATE,
  CONTAINER_SLOT_RETRY_DELAY_SECONDS,
  CONTAINER_SLOT_STALE_MS,
  decideContainerSlotClaim,
  isReservedContainerLane,
  pruneStaleContainerSlots,
  clearContainerSlotLease,
  releaseContainerSlotLease,
  touchContainerSlotLease,
  type ContainerSlotLease,
} from "./container-slot-cap";

const NOW_MS = 1_000_000;

const makeLease = (
  overrides: Partial<ContainerSlotLease> & Pick<ContainerSlotLease, "doName">,
): ContainerSlotLease => ({
  category: "jra",
  holders: 1,
  kind: "focused-full",
  rescoreHolders: 0,
  timestamp: NOW_MS,
  ...overrides,
});

test("container max instances stays at the wrangler ceiling of 12", () => {
  expect(CONTAINER_MAX_INSTANCES).toBe(12);
});

test("reserved Ban-ei headroom is exactly two start slots", () => {
  expect(CONTAINER_RESERVED_HEADROOM).toBe(2);
});

test("general pool is max instances minus reserved Ban-ei headroom", () => {
  expect(CONTAINER_GENERAL_SLOT_MAX).toBe(10);
});

test("rescore unique-DO cap is three so one category instance each", () => {
  expect(CONTAINER_RESCORE_SLOT_MAX).toBe(3);
});

test("slot heartbeat stale window is twenty minutes", () => {
  expect(CONTAINER_SLOT_STALE_MS).toBe(1_200_000);
});

test("day-base stale window is sixty minutes to cover DAY_CHAIN", () => {
  expect(CONTAINER_DAY_BASE_SLOT_STALE_MS).toBe(3_600_000);
});

test("over-cap retry delay is thirty seconds so queue consumers free quickly", () => {
  expect(CONTAINER_SLOT_RETRY_DELAY_SECONDS).toBe(30);
});

test("Ban-ei focused-full uses the reserved lane", () => {
  expect(isReservedContainerLane({ category: "ban-ei", kind: "focused-full" })).toBe(true);
});

test("Ban-ei day-base uses the reserved lane", () => {
  expect(isReservedContainerLane({ category: "ban-ei", kind: "day-base" })).toBe(true);
});

test("Ban-ei rescore does not use the reserved lane", () => {
  expect(isReservedContainerLane({ category: "ban-ei", kind: "rescore" })).toBe(false);
});

test("JRA focused-full does not use the reserved lane", () => {
  expect(isReservedContainerLane({ category: "jra", kind: "focused-full" })).toBe(false);
});

test("NAR day-base does not use the reserved lane", () => {
  expect(isReservedContainerLane({ category: "nar", kind: "day-base" })).toBe(false);
});

test("pruneStaleContainerSlots drops a lease older than the stale window", () => {
  const live = pruneStaleContainerSlots(
    [
      makeLease({ doName: "predict-jra", timestamp: 100 }),
      makeLease({ category: "nar", doName: "predict-nar", timestamp: NOW_MS }),
    ],
    NOW_MS,
    1_000,
  );
  expect(live).toStrictEqual([
    {
      category: "nar",
      doName: "predict-nar",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
  ]);
});

test("decideContainerSlotClaim starts the first unique DO", () => {
  const decision = decideContainerSlotClaim([], {
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(decision.proceed).toBe(true);
  expect(decision.leases).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
  ]);
});

test("a second rescore on the same DO is busy instead of starting another instance", () => {
  const first = decideContainerSlotClaim([], {
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  const second = decideContainerSlotClaim(first.leases, {
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
    now: NOW_MS + 1,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(second.proceed).toBe(false);
  expect(second.state).toBe(CONTAINER_SLOT_BUSY_STATE);
  expect(second.leases).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
  ]);
});

test("JRA NAR and Ban-ei rescores can each hold one unique DO", () => {
  const jra = decideContainerSlotClaim([], {
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  const nar = decideContainerSlotClaim(jra.leases, {
    category: "nar",
    doName: "predict-nar",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  const banei = decideContainerSlotClaim(nar.leases, {
    category: "ban-ei",
    doName: "predict-ban-ei",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(jra.proceed).toBe(true);
  expect(nar.proceed).toBe(true);
  expect(banei.proceed).toBe(true);
  expect(banei.leases).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
    {
      category: "nar",
      doName: "predict-nar",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
    {
      category: "ban-ei",
      doName: "predict-ban-ei",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
  ]);
});

test("a fourth unique rescore DO is capped so five JRA/NAR shards cannot pack the pool", () => {
  const three: ContainerSlotLease[] = [
    makeLease({
      category: "jra",
      doName: "predict-jra",
      kind: "rescore",
      rescoreHolders: 1,
    }),
    makeLease({
      category: "nar",
      doName: "predict-nar",
      kind: "rescore",
      rescoreHolders: 1,
    }),
    makeLease({
      category: "ban-ei",
      doName: "predict-ban-ei",
      kind: "rescore",
      rescoreHolders: 1,
    }),
  ];
  const fourth = decideContainerSlotClaim(three, {
    category: "jra",
    doName: "predict-jra-2",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(fourth.proceed).toBe(false);
  expect(fourth.state).toBe(CONTAINER_SLOT_CAPPED_STATE);
  expect(fourth.leases).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
    {
      category: "nar",
      doName: "predict-nar",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
    {
      category: "ban-ei",
      doName: "predict-ban-ei",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
  ]);
});

test("a stale rescore lease is dropped so a later claim can reuse that unique DO", () => {
  const stale: ContainerSlotLease[] = [
    makeLease({
      category: "jra",
      doName: "predict-jra",
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: NOW_MS - CONTAINER_SLOT_STALE_MS,
    }),
  ];
  const claim = decideContainerSlotClaim(stale, {
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(claim.proceed).toBe(true);
  expect(claim.leases).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
  ]);
});

test("sharing one DO leaves a sibling lease on a different DO unchanged", () => {
  const two: ContainerSlotLease[] = [
    makeLease({
      category: "jra",
      doName: "predict-jra",
      kind: "rescore",
      rescoreHolders: 1,
    }),
    makeLease({ category: "nar", doName: "predict-nar-0" }),
  ];
  const shared = decideContainerSlotClaim(two, {
    category: "jra",
    doName: "predict-jra",
    kind: "focused-full",
    now: NOW_MS + 5,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(shared.proceed).toBe(true);
  expect(shared.leases).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 2,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_005,
    },
    {
      category: "nar",
      doName: "predict-nar-0",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
  ]);
});

test("focused-full can share an existing rescore DO without starting another instance", () => {
  const rescore = decideContainerSlotClaim([], {
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  const focused = decideContainerSlotClaim(rescore.leases, {
    category: "jra",
    doName: "predict-jra",
    kind: "focused-full",
    now: NOW_MS + 5,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(focused.proceed).toBe(true);
  expect(focused.leases).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 2,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_005,
    },
  ]);
});

test("ten general unique DOs cap an eleventh JRA focused-full shard", () => {
  const tenGeneral: ContainerSlotLease[] = [
    makeLease({ doName: "predict-jra-0" }),
    makeLease({ doName: "predict-jra-1" }),
    makeLease({ doName: "predict-jra-2" }),
    makeLease({ category: "nar", doName: "predict-nar-0" }),
    makeLease({ category: "nar", doName: "predict-nar-1" }),
    makeLease({ category: "nar", doName: "predict-nar-2" }),
    makeLease({ category: "jra", doName: "predict-jra", kind: "rescore", rescoreHolders: 1 }),
    makeLease({ category: "nar", doName: "predict-nar", kind: "rescore", rescoreHolders: 1 }),
    makeLease({
      category: "ban-ei",
      doName: "predict-ban-ei",
      kind: "rescore",
      rescoreHolders: 1,
    }),
    makeLease({ category: "jra", doName: "predict-jra-extra" }),
  ];
  const eleventh = decideContainerSlotClaim(tenGeneral, {
    category: "jra",
    doName: "predict-jra-blocked",
    kind: "focused-full",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(eleventh.proceed).toBe(false);
  expect(eleventh.state).toBe(CONTAINER_SLOT_CAPPED_STATE);
  expect(eleventh.leases).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra-0",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
    {
      category: "jra",
      doName: "predict-jra-1",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
    {
      category: "jra",
      doName: "predict-jra-2",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
    {
      category: "nar",
      doName: "predict-nar-0",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
    {
      category: "nar",
      doName: "predict-nar-1",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
    {
      category: "nar",
      doName: "predict-nar-2",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
    {
      category: "nar",
      doName: "predict-nar",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
    {
      category: "ban-ei",
      doName: "predict-ban-ei",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
    {
      category: "jra",
      doName: "predict-jra-extra",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
  ]);
});

test("Ban-ei focused-full still starts when the general pool is full", () => {
  const tenGeneral: ContainerSlotLease[] = [
    makeLease({ doName: "predict-jra-0" }),
    makeLease({ doName: "predict-jra-1" }),
    makeLease({ doName: "predict-jra-2" }),
    makeLease({ category: "nar", doName: "predict-nar-0" }),
    makeLease({ category: "nar", doName: "predict-nar-1" }),
    makeLease({ category: "nar", doName: "predict-nar-2" }),
    makeLease({ category: "jra", doName: "predict-jra", kind: "rescore", rescoreHolders: 1 }),
    makeLease({ category: "nar", doName: "predict-nar", kind: "rescore", rescoreHolders: 1 }),
    makeLease({
      category: "ban-ei",
      doName: "predict-ban-ei",
      kind: "rescore",
      rescoreHolders: 1,
    }),
    makeLease({ category: "jra", doName: "predict-jra-extra" }),
  ];
  const banei = decideContainerSlotClaim(tenGeneral, {
    category: "ban-ei",
    doName: "predict-ban-ei-0",
    kind: "focused-full",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(banei.proceed).toBe(true);
  expect(banei.leases[10]).toStrictEqual({
    category: "ban-ei",
    doName: "predict-ban-ei-0",
    holders: 1,
    kind: "focused-full",
    rescoreHolders: 0,
    timestamp: 1_000_000,
  });
});

test("Ban-ei day-base still starts as the second reserved slot", () => {
  const tenGeneralPlusOneReserved: ContainerSlotLease[] = [
    makeLease({ doName: "predict-jra-0" }),
    makeLease({ doName: "predict-jra-1" }),
    makeLease({ doName: "predict-jra-2" }),
    makeLease({ category: "nar", doName: "predict-nar-0" }),
    makeLease({ category: "nar", doName: "predict-nar-1" }),
    makeLease({ category: "nar", doName: "predict-nar-2" }),
    makeLease({ category: "jra", doName: "predict-jra", kind: "rescore", rescoreHolders: 1 }),
    makeLease({ category: "nar", doName: "predict-nar", kind: "rescore", rescoreHolders: 1 }),
    makeLease({
      category: "ban-ei",
      doName: "predict-ban-ei",
      kind: "rescore",
      rescoreHolders: 1,
    }),
    makeLease({ category: "jra", doName: "predict-jra-extra" }),
    makeLease({
      category: "ban-ei",
      doName: "predict-ban-ei-0",
      kind: "focused-full",
    }),
  ];
  const dayBase = decideContainerSlotClaim(tenGeneralPlusOneReserved, {
    category: "ban-ei",
    doName: "predict-ban-ei-day",
    kind: "day-base",
    now: NOW_MS,
    staleAfterMs: CONTAINER_DAY_BASE_SLOT_STALE_MS,
  });
  expect(dayBase.proceed).toBe(true);
  expect(dayBase.leases[11]).toStrictEqual({
    category: "ban-ei",
    doName: "predict-ban-ei-day",
    holders: 1,
    kind: "day-base",
    rescoreHolders: 0,
    timestamp: 1_000_000,
  });
});

test("a thirteenth unique DO is capped at the platform max_instances ceiling", () => {
  const twelve: ContainerSlotLease[] = [
    makeLease({ doName: "predict-jra-0" }),
    makeLease({ doName: "predict-jra-1" }),
    makeLease({ doName: "predict-jra-2" }),
    makeLease({ category: "nar", doName: "predict-nar-0" }),
    makeLease({ category: "nar", doName: "predict-nar-1" }),
    makeLease({ category: "nar", doName: "predict-nar-2" }),
    makeLease({ category: "jra", doName: "predict-jra", kind: "rescore", rescoreHolders: 1 }),
    makeLease({ category: "nar", doName: "predict-nar", kind: "rescore", rescoreHolders: 1 }),
    makeLease({
      category: "ban-ei",
      doName: "predict-ban-ei",
      kind: "rescore",
      rescoreHolders: 1,
    }),
    makeLease({ category: "jra", doName: "predict-jra-extra" }),
    makeLease({
      category: "ban-ei",
      doName: "predict-ban-ei-0",
      kind: "focused-full",
    }),
    makeLease({
      category: "ban-ei",
      doName: "predict-ban-ei-1",
      kind: "day-base",
    }),
  ];
  const thirteenth = decideContainerSlotClaim(twelve, {
    category: "ban-ei",
    doName: "predict-ban-ei-2",
    kind: "focused-full",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(thirteenth.proceed).toBe(false);
  expect(thirteenth.state).toBe(CONTAINER_SLOT_CAPPED_STATE);
});

test("clearContainerSlotLease drops a multi-holder destroyed DO so the cap can start another instance", () => {
  const remaining = clearContainerSlotLease(
    [
      makeLease({
        category: "nar",
        doName: "predict-nar-0",
        holders: 3,
        kind: "focused-full",
      }),
      makeLease({
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
      }),
    ],
    "predict-nar-0",
    NOW_MS,
    CONTAINER_SLOT_STALE_MS,
  );
  expect(remaining).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra-1",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
  ]);
});

test("clearContainerSlotLease then a new general claim proceeds when the ghost lease had filled the cap", () => {
  const tenth = decideContainerSlotClaim(
    [
      makeLease({ doName: "predict-nar-0", category: "nar", holders: 4 }),
      makeLease({ doName: "predict-nar-1", category: "nar" }),
      makeLease({ doName: "predict-nar-2", category: "nar" }),
      makeLease({ doName: "predict-jra-0", category: "jra" }),
      makeLease({ doName: "predict-jra-1", category: "jra" }),
      makeLease({ doName: "predict-jra-2", category: "jra" }),
      makeLease({ doName: "predict-jra", category: "jra" }),
      makeLease({ doName: "predict-nar", category: "nar" }),
      makeLease({ doName: "predict-ban-ei", category: "ban-ei", kind: "rescore" }),
      makeLease({ doName: "predict-ban-ei-0", category: "ban-ei", kind: "rescore" }),
    ],
    {
      category: "jra",
      doName: "predict-jra-extra",
      kind: "focused-full",
      now: NOW_MS,
      staleAfterMs: CONTAINER_SLOT_STALE_MS,
    },
  );
  expect(tenth.proceed).toBe(false);
  expect(tenth.state).toBe(CONTAINER_SLOT_CAPPED_STATE);
  const cleared = clearContainerSlotLease(
    tenth.leases,
    "predict-nar-0",
    NOW_MS,
    CONTAINER_SLOT_STALE_MS,
  );
  const afterDestroy = decideContainerSlotClaim(cleared, {
    category: "jra",
    doName: "predict-jra-extra",
    kind: "focused-full",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(afterDestroy.proceed).toBe(true);
});

test("releaseContainerSlotLease removes a single-holder lease", () => {
  const remaining = releaseContainerSlotLease(
    [
      makeLease({
        category: "jra",
        doName: "predict-jra",
        kind: "rescore",
        rescoreHolders: 1,
      }),
    ],
    "predict-jra",
    "rescore",
    NOW_MS,
    CONTAINER_SLOT_STALE_MS,
  );
  expect(remaining).toStrictEqual([]);
});

test("releaseContainerSlotLease decrements a shared lease without dropping the instance", () => {
  const remaining = releaseContainerSlotLease(
    [
      makeLease({
        category: "jra",
        doName: "predict-jra",
        holders: 2,
        kind: "rescore",
        rescoreHolders: 1,
      }),
    ],
    "predict-jra",
    "focused-full",
    NOW_MS,
    CONTAINER_SLOT_STALE_MS,
  );
  expect(remaining).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 1,
      timestamp: 1_000_000,
    },
  ]);
});

test("releasing a rescore holder on a shared lease frees the rescore cap", () => {
  const remaining = releaseContainerSlotLease(
    [
      makeLease({
        category: "jra",
        doName: "predict-jra",
        holders: 2,
        kind: "rescore",
        rescoreHolders: 1,
      }),
    ],
    "predict-jra",
    "rescore",
    NOW_MS,
    CONTAINER_SLOT_STALE_MS,
  );
  expect(remaining).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "rescore",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
  ]);
  const nextRescore = decideContainerSlotClaim(remaining, {
    category: "jra",
    doName: "predict-jra",
    kind: "rescore",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(nextRescore.proceed).toBe(true);
});

test("releaseContainerSlotLease leaves unrelated DOs untouched", () => {
  const remaining = releaseContainerSlotLease(
    [
      makeLease({ doName: "predict-jra-0" }),
      makeLease({ category: "nar", doName: "predict-nar-0" }),
    ],
    "predict-jra-0",
    "focused-full",
    NOW_MS,
    CONTAINER_SLOT_STALE_MS,
  );
  expect(remaining).toStrictEqual([
    {
      category: "nar",
      doName: "predict-nar-0",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
  ]);
});

test("touchContainerSlotLease refreshes only the named DO heartbeat", () => {
  const touched = touchContainerSlotLease(
    [
      makeLease({ doName: "predict-jra-0", timestamp: NOW_MS - 100 }),
      makeLease({ category: "nar", doName: "predict-nar-0", timestamp: NOW_MS - 100 }),
    ],
    "predict-jra-0",
    NOW_MS,
    CONTAINER_SLOT_STALE_MS,
  );
  expect(touched).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra-0",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
    {
      category: "nar",
      doName: "predict-nar-0",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 999_900,
    },
  ]);
});

test("touchContainerSlotLease drops stale leases while refreshing a live one", () => {
  const touched = touchContainerSlotLease(
    [
      makeLease({
        doName: "predict-jra-0",
        timestamp: NOW_MS - CONTAINER_SLOT_STALE_MS,
      }),
      makeLease({ category: "nar", doName: "predict-nar-0", timestamp: NOW_MS - 10 }),
    ],
    "predict-nar-0",
    NOW_MS,
    CONTAINER_SLOT_STALE_MS,
  );
  expect(touched).toStrictEqual([
    {
      category: "nar",
      doName: "predict-nar-0",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
    },
  ]);
});
