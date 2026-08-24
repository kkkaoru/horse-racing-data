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
  isContainerSlotStopAllowed,
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

test("container max instances matches the observed effective ceiling of 10", () => {
  expect(CONTAINER_MAX_INSTANCES).toBe(10);
});

test("reserved Ban-ei headroom is exactly two start slots", () => {
  expect(CONTAINER_RESERVED_HEADROOM).toBe(2);
});

test("general pool is max instances minus reserved Ban-ei headroom", () => {
  expect(CONTAINER_GENERAL_SLOT_MAX).toBe(8);
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
      makeLease({ doName: "predict-jra", staleAfterMs: 1_000, timestamp: 100 }),
      makeLease({ category: "nar", doName: "predict-nar", staleAfterMs: 1_000, timestamp: NOW_MS }),
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
      staleAfterMs: 1_000,
      timestamp: 1_000_000,
    },
  ]);
});

test("pruneStaleContainerSlots applies each lease kind's own expiry", () => {
  const live = pruneStaleContainerSlots(
    [
      makeLease({ doName: "predict-jra-day", kind: "day-base", timestamp: NOW_MS - 1_800_000 }),
      makeLease({ category: "nar", doName: "predict-nar-0", timestamp: NOW_MS - 1_800_000 }),
    ],
    NOW_MS,
  );
  expect(live).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra-day",
      holders: 1,
      kind: "day-base",
      rescoreHolders: 0,
      timestamp: -800_000,
    },
  ]);
});

test("decideContainerSlotClaim preserves custom expiry and work ownership", () => {
  const decision = decideContainerSlotClaim([], {
    category: "jra",
    doName: "predict-jra-1",
    kind: "focused-full",
    now: NOW_MS,
    staleAfterMs: 2_000,
    workKey: "focused-full:20260813:jra:30:11",
  });
  expect(decision).toStrictEqual({
    leases: [
      {
        category: "jra",
        doName: "predict-jra-1",
        holders: 1,
        kind: "focused-full",
        rescoreHolders: 0,
        staleAfterMs: 2_000,
        timestamp: NOW_MS,
        workKey: "focused-full:20260813:jra:30:11",
      },
    ],
    proceed: true,
  });
});

test("decideContainerSlotClaim atomically transfers an expected day-base owner", () => {
  const first = decideContainerSlotClaim(
    [
      makeLease({
        doName: "predict-nar",
        kind: "day-base",
        workKey: "day-base:20260824:nar",
      }),
    ],
    {
      category: "nar",
      doName: "predict-nar",
      kind: "day-base",
      now: NOW_MS + 1,
      replaceWorkKey: "day-base:20260824:nar",
      staleAfterMs: CONTAINER_DAY_BASE_SLOT_STALE_MS,
      workKey: "day-base-stale:20260824:nar",
    },
  );
  expect(first).toStrictEqual({
    leases: [
      {
        category: "jra",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 1_000_001,
        workKey: "day-base-stale:20260824:nar",
      },
    ],
    proceed: true,
  });

  const duplicate = decideContainerSlotClaim(first.leases, {
    category: "nar",
    doName: "predict-nar",
    kind: "day-base",
    now: NOW_MS + 2,
    replaceWorkKey: "day-base:20260824:nar",
    staleAfterMs: CONTAINER_DAY_BASE_SLOT_STALE_MS,
    workKey: "day-base-stale:20260824:nar",
  });
  expect(duplicate).toStrictEqual({
    leases: [
      {
        category: "jra",
        doName: "predict-nar",
        holders: 1,
        kind: "day-base",
        rescoreHolders: 0,
        timestamp: 1_000_001,
        workKey: "day-base-stale:20260824:nar",
      },
    ],
    proceed: false,
    state: "busy",
  });
  expect(
    releaseContainerSlotLease(
      duplicate.leases,
      "predict-nar",
      "day-base",
      NOW_MS + 3,
      "day-base:20260824:nar",
    ),
  ).toStrictEqual([
    {
      category: "jra",
      doName: "predict-nar",
      holders: 1,
      kind: "day-base",
      rescoreHolders: 0,
      timestamp: 1_000_001,
      workKey: "day-base-stale:20260824:nar",
    },
  ]);
  expect(
    releaseContainerSlotLease(
      duplicate.leases,
      "predict-nar",
      "day-base",
      NOW_MS + 3,
      "day-base-stale:20260824:nar",
    ),
  ).toStrictEqual([]);
});

test("the same work owner can reclaim its slot for terminal cleanup", () => {
  const lease = makeLease({ doName: "predict-jra-1", workKey: "work-1" });
  const decision = decideContainerSlotClaim([lease], {
    allowSameOwner: true,
    category: "jra",
    doName: "predict-jra-1",
    kind: "focused-full",
    now: NOW_MS + 1,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
    workKey: "work-1",
  });

  expect(decision).toStrictEqual({ leases: [lease], proceed: true });
});

test("terminal stop ownership rejects a newer owner but allows safe cleanup", () => {
  const owned = [makeLease({ doName: "predict-jra-1", workKey: "new-work" })];

  expect(isContainerSlotStopAllowed(owned, "predict-jra-1", "old-work", NOW_MS)).toBe(false);
  expect(isContainerSlotStopAllowed(owned, "predict-jra-1", "new-work", NOW_MS)).toBe(true);
  expect(isContainerSlotStopAllowed(owned, "predict-nar-1", "old-work", NOW_MS)).toBe(false);
  expect(isContainerSlotStopAllowed(owned, "predict-jra-1", undefined, NOW_MS)).toBe(true);
  expect(
    isContainerSlotStopAllowed(
      [makeLease({ doName: "predict-jra-1", workKey: undefined })],
      "predict-jra-1",
      "work-1",
      NOW_MS,
    ),
  ).toBe(false);
  expect(
    isContainerSlotStopAllowed(owned, "predict-jra-1", "canonical-work", NOW_MS, [
      "canonical-work",
      "new-work",
    ]),
  ).toBe(true);
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

test("a different execution cannot share one DO and leaves sibling leases unchanged", () => {
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
  expect(shared.proceed).toBe(false);
  expect(shared.state).toBe("busy");
  expect(shared.leases).toStrictEqual(two);
});

test("focused-full waits while an existing rescore owns the DO", () => {
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
  expect(focused.proceed).toBe(false);
  expect(focused.state).toBe("busy");
  expect(focused.leases).toStrictEqual(rescore.leases);
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
  const eightGeneral: ContainerSlotLease[] = [
    makeLease({ doName: "predict-jra-0" }),
    makeLease({ doName: "predict-jra-1" }),
    makeLease({ category: "nar", doName: "predict-nar-0" }),
    makeLease({ category: "nar", doName: "predict-nar-1" }),
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
  const banei = decideContainerSlotClaim(eightGeneral, {
    category: "ban-ei",
    doName: "predict-ban-ei-0",
    kind: "focused-full",
    now: NOW_MS,
    staleAfterMs: CONTAINER_SLOT_STALE_MS,
  });
  expect(banei.proceed).toBe(true);
  expect(banei.leases[8]).toStrictEqual({
    category: "ban-ei",
    doName: "predict-ban-ei-0",
    holders: 1,
    kind: "focused-full",
    rescoreHolders: 0,
    timestamp: 1_000_000,
  });
});

test("Ban-ei day-base still starts as the second reserved slot", () => {
  const eightGeneralPlusOneReserved: ContainerSlotLease[] = [
    makeLease({ doName: "predict-jra-0" }),
    makeLease({ doName: "predict-jra-1" }),
    makeLease({ category: "nar", doName: "predict-nar-0" }),
    makeLease({ category: "nar", doName: "predict-nar-1" }),
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
  const dayBase = decideContainerSlotClaim(eightGeneralPlusOneReserved, {
    category: "ban-ei",
    doName: "predict-ban-ei-day",
    kind: "day-base",
    now: NOW_MS,
    staleAfterMs: CONTAINER_DAY_BASE_SLOT_STALE_MS,
  });
  expect(dayBase.proceed).toBe(true);
  expect(dayBase.leases[9]).toStrictEqual({
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
  const eighth = decideContainerSlotClaim(
    [
      makeLease({ doName: "predict-nar-0", category: "nar", holders: 4 }),
      makeLease({ doName: "predict-nar-1", category: "nar" }),
      makeLease({ doName: "predict-nar-2", category: "nar" }),
      makeLease({ doName: "predict-jra-0", category: "jra" }),
      makeLease({ doName: "predict-jra-1", category: "jra" }),
      makeLease({ doName: "predict-jra", category: "jra" }),
      makeLease({ doName: "predict-nar", category: "nar" }),
      makeLease({ doName: "predict-ban-ei", category: "ban-ei", kind: "rescore" }),
    ],
    {
      category: "jra",
      doName: "predict-jra-extra",
      kind: "focused-full",
      now: NOW_MS,
      staleAfterMs: CONTAINER_SLOT_STALE_MS,
    },
  );
  expect(eighth.proceed).toBe(false);
  expect(eighth.state).toBe(CONTAINER_SLOT_CAPPED_STATE);
  const cleared = clearContainerSlotLease(
    eighth.leases,
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

test("releaseContainerSlotLease preserves optional lease ownership fields", () => {
  const remaining = releaseContainerSlotLease(
    [
      makeLease({
        doName: "predict-jra",
        holders: 2,
        staleAfterMs: 2_000,
        workKey: "focused-full:20260813:jra:30:11",
      }),
    ],
    "predict-jra",
    "focused-full",
    NOW_MS,
    "focused-full:20260813:jra:30:11",
  );
  expect(remaining).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      staleAfterMs: 2_000,
      timestamp: NOW_MS,
      workKey: "focused-full:20260813:jra:30:11",
    },
  ]);
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

test("releasing a legacy rescore holder keeps the remaining focused execution exclusive", () => {
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
  expect(nextRescore.proceed).toBe(false);
  expect(nextRescore.state).toBe("busy");
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

test("releaseContainerSlotLease cannot release a different work owner's lease", () => {
  const remaining = releaseContainerSlotLease(
    [makeLease({ doName: "predict-nar-1", workKey: "focused-full:20260813:nar:30:11" })],
    "predict-nar-1",
    "focused-full",
    NOW_MS,
    "focused-full:20260813:nar:30:12",
  );
  expect(remaining).toStrictEqual([
    {
      category: "jra",
      doName: "predict-nar-1",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
      workKey: "focused-full:20260813:nar:30:11",
    },
  ]);
});

test("clearContainerSlotLease only clears the matching work owner", () => {
  const leases = [
    makeLease({ doName: "predict-nar-1", workKey: "focused-full:20260813:nar:30:11" }),
  ];
  expect(
    clearContainerSlotLease(leases, "predict-nar-1", NOW_MS, "focused-full:20260813:nar:30:12"),
  ).toStrictEqual([
    {
      category: "jra",
      doName: "predict-nar-1",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 1_000_000,
      workKey: "focused-full:20260813:nar:30:11",
    },
  ]);
  expect(
    clearContainerSlotLease(leases, "predict-nar-1", NOW_MS, "focused-full:20260813:nar:30:11"),
  ).toStrictEqual([]);
});

test("touchContainerSlotLease only refreshes the matching work owner", () => {
  const touched = touchContainerSlotLease(
    [
      makeLease({
        doName: "predict-nar-1",
        timestamp: NOW_MS - 100,
        workKey: "focused-full:20260813:nar:30:11",
      }),
    ],
    "predict-nar-1",
    NOW_MS,
    "focused-full:20260813:nar:30:12",
  );
  expect(touched).toStrictEqual([
    {
      category: "jra",
      doName: "predict-nar-1",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      timestamp: 999_900,
      workKey: "focused-full:20260813:nar:30:11",
    },
  ]);
});

test("touchContainerSlotLease preserves optional lease ownership fields", () => {
  const touched = touchContainerSlotLease(
    [
      makeLease({
        doName: "predict-jra",
        staleAfterMs: 2_000,
        timestamp: NOW_MS - 100,
        workKey: "focused-full:20260813:jra:30:11",
      }),
    ],
    "predict-jra",
    NOW_MS,
    "focused-full:20260813:jra:30:11",
  );
  expect(touched).toStrictEqual([
    {
      category: "jra",
      doName: "predict-jra",
      holders: 1,
      kind: "focused-full",
      rescoreHolders: 0,
      staleAfterMs: 2_000,
      timestamp: NOW_MS,
      workKey: "focused-full:20260813:jra:30:11",
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
