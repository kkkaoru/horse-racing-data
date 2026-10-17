// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import {
  canonicalHeatmapQuery,
  HEATMAP_ALGORITHM_REVISION,
  heatmapExecutionKey,
  HeatmapExecutionCoordinator,
  type HeatmapExecutionRequest,
} from "./heatmap-execution";

const request: HeatmapExecutionRequest = {
  day: "04",
  month: "10",
  year: "2026",
  keibajoCode: "08",
  raceNumber: "01",
  source: "jra",
  query: "",
};

it("versions the corrected Catalog cache protocol independently of source data", () => {
  expect(HEATMAP_ALGORITHM_REVISION).toBe("qualified-frame-sql-v5");
});

it("preserves explicit defaults and sorts without source in the cache destination", () => {
  expect(
    canonicalHeatmapQuery(new URLSearchParams("statsVenue=1&source=jra&analysisCellTrack=0")),
  ).toBe("analysisCellTrack=0&statsVenue=1");
  expect(canonicalHeatmapQuery(new URLSearchParams())).toBe("");
  expect(heatmapExecutionKey(request)).not.toBe(
    heatmapExecutionKey({ ...request, query: "statsVenue=1" }),
  );
  expect(heatmapExecutionKey(request)).toStrictEqual(
    heatmapExecutionKey({ ...request, source: "nar" }),
  );
});
it.each([
  "x=1",
  "statsVenue=2",
  "statsVenue=1&statsVenue=0",
  "statsYears=11",
  "statsYears=0",
  "resultsSourceScope=wrong",
  `statsVenue=${"1".repeat(2049)}`,
])("rejects unsupported or ambiguous query %s", (query) => {
  expect(() => canonicalHeatmapQuery(new URLSearchParams(query))).toThrow("Invalid heatmap query");
});
it.each([
  "statsYears=all",
  "bloodlineStatsYears=10",
  "analysisStatsYears=1",
  "resultsSourceScope=nar",
  "similarStatsSourceScope=jra",
])("admits recognized bounded query %s", (query) => {
  expect(canonicalHeatmapQuery(new URLSearchParams(query))).toStrictEqual(query);
});
it("serializes simultaneous misses, survives re-instantiation and never steals an aborted caller's running owner", async () => {
  const stored: { value: unknown } = { value: undefined };
  const storage = {
    read: () => stored.value,
    write: (value: unknown) => {
      stored.value = value;
    },
  };
  const send = vi.fn<() => Promise<void>>(async () => undefined);
  const coordinator = new HeatmapExecutionCoordinator(storage, { send });
  expect(
    await Promise.all([coordinator.ensure(request), coordinator.ensure(request)]),
  ).toStrictEqual(["queued", "queued"]);
  expect(send).toHaveBeenCalledTimes(1);
  const state = storage.read();
  if (
    typeof state !== "object" ||
    state === null ||
    !("token" in state) ||
    typeof state.token !== "string"
  )
    throw new Error("missing token");
  expect(coordinator.claim(state.token)).toBe(true);
  expect(coordinator.claim(state.token)).toBe(false);
  const recreated = new HeatmapExecutionCoordinator(storage, { send });
  expect(await recreated.ensure(request)).toBe("running");
  expect(recreated.settle("stale", true)).toBe(false);
  expect(recreated.claim("stale")).toBe(false);
  expect(recreated.settle(state.token, true)).toBe(true);
  expect(await recreated.ensure(request)).toBe("queued");
  expect(recreated.settle(state.token, false)).toBe(false);
  expect(send).toHaveBeenCalledTimes(2);
});
it("holds queued ownership on ambiguous workflow creation failure", async () => {
  const stored: { value: unknown } = { value: undefined };
  const storage = {
    read: () => stored.value,
    write: (value: unknown) => {
      stored.value = value;
    },
  };
  const send = vi.fn<() => Promise<void>>(async () => {
    throw new Error("timeout after accepted");
  });
  const coordinator = new HeatmapExecutionCoordinator(storage, { send });
  await expect(coordinator.ensure(request)).rejects.toThrow("timeout after accepted");
  expect(await new HeatmapExecutionCoordinator(storage, { send }).ensure(request)).toBe("queued");
  expect(send).toHaveBeenCalledTimes(1);
});
it.each([
  null,
  {},
  { phase: "unknown" },
  { phase: "running", token: "x", failures: -1, retryAt: 0 },
])("fails closed on unknown persisted state %j", (value) => {
  const send = vi.fn<() => Promise<void>>(async () => undefined);
  const coordinator = new HeatmapExecutionCoordinator(
    { read: () => value, write: vi.fn<(...args: never[]) => unknown>() },
    { send },
  );
  expect(coordinator.claim("x")).toBe(false);
  expect(coordinator.settle("x", true)).toBe(false);
  return expect(coordinator.ensure(request)).resolves.toBe("unknown");
});
it("backs off settled failures without consulting Workflow status", async () => {
  const stored = { value: { phase: "running", token: "x", failures: 0, retryAt: 0 } };
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => stored.value,
      write: (value) => {
        stored.value = value;
      },
    },
    { send: vi.fn<() => Promise<void>>(async () => undefined) },
  );
  expect(coordinator.settle("x", false)).toBe(true);
  expect(await coordinator.ensure(request)).toBe("backoff");
  stored.value = { phase: "settled", token: "x", failures: 1, retryAt: 0 };
  expect(await coordinator.ensure(request)).toBe("queued");
});
it.each([2, 3, 10])(
  "retries confirmed repeated failures once per 15 minutes, prior failures %s",
  async (failures) => {
    const now = vi.spyOn(Date, "now").mockReturnValue(1000);
    try {
      const stored = { value: { phase: "running", token: "old", failures, retryAt: 0 } };
      const storage = {
        read: () => stored.value,
        write: (value: typeof stored.value) => {
          stored.value = value;
        },
      };
      const send = vi.fn<() => Promise<void>>(async () => undefined);
      const coordinator = new HeatmapExecutionCoordinator(storage, { send });
      expect(coordinator.settle("old", false)).toBe(true);
      expect(stored.value.failures).toBe(3);
      expect(stored.value.retryAt).toBe(901000);
      now.mockReturnValue(900999);
      expect(await coordinator.ensure(request)).toBe("backoff");
      expect(send).not.toHaveBeenCalled();
      now.mockReturnValue(901000);
      const recreated = new HeatmapExecutionCoordinator(storage, { send });
      expect(
        await Promise.all([recreated.ensure(request), recreated.ensure(request)]),
      ).toStrictEqual(["queued", "queued"]);
      expect(send).toHaveBeenCalledTimes(1);
      expect(stored.value.failures).toBe(3);
      expect(recreated.claim("old")).toBe(false);
      expect(recreated.settle("old", true)).toBe(false);
      const token = stored.value.token;
      expect(recreated.claim(token)).toBe(true);
      expect(recreated.settle(token, false)).toBe(true);
      expect(stored.value.retryAt).toBe(1801000);
      expect(stored.value.failures).toBe(3);
      expect(await recreated.ensure(request)).toBe("backoff");
      now.mockReturnValue(1801000);
      expect(await recreated.ensure(request)).toBe("queued");
      const recoveredToken = stored.value.token;
      expect(recreated.claim(recoveredToken)).toBe(true);
      expect(recreated.settle(recoveredToken, true)).toBe(true);
      expect(stored.value.failures).toBe(0);
      expect(stored.value.retryAt).toBe(0);
    } finally {
      now.mockRestore();
    }
  },
);

it("recovers legacy permanently blocked state without resetting active ownership", async () => {
  const stored = { value: { phase: "settled", token: "legacy", failures: 3, retryAt: 0 } };
  const send = vi.fn<() => Promise<void>>(async () => undefined);
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => stored.value,
      write: (value) => {
        stored.value = value;
      },
    },
    { send },
  );
  expect(await coordinator.ensure(request)).toBe("queued");
  expect(await coordinator.ensure(request)).toBe("queued");
  expect(send).toHaveBeenCalledTimes(1);
  expect(stored.value.failures).toBe(3);
  expect(coordinator.claim("legacy")).toBe(false);
  expect(coordinator.settle("legacy", true)).toBe(false);
});

it("rejects a noncanonical direct scheduler request", async () => {
  const coordinator = new HeatmapExecutionCoordinator(
    { read: () => undefined, write: vi.fn<(...args: never[]) => unknown>() },
    { send: vi.fn<() => Promise<void>>(async () => undefined) },
  );
  await expect(
    coordinator.ensure({ ...request, query: "statsYears=1&statsVenue=0" }),
  ).rejects.toThrow("Noncanonical");
});

it.each([3, 4])(
  "keeps repeated failures in backoff until retryAt, failures %s",
  async (failures) => {
    const state = { phase: "settled", token: "x", failures, retryAt: Number.MAX_SAFE_INTEGER };
    const write = vi.fn<() => void>();
    const send = vi.fn<() => Promise<void>>(async () => undefined);
    const coordinator = new HeatmapExecutionCoordinator({ read: () => state, write }, { send });
    expect(await coordinator.ensure(request)).toBe("backoff");
    expect(write).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  },
);

it("admits a successor only after settled failure backoff expires", async () => {
  const now = vi.spyOn(Date, "now").mockReturnValue(1000);
  try {
    const stored = { value: { phase: "settled", token: "x", failures: 2, retryAt: 1001 } };
    const write = vi.fn<(value: typeof stored.value) => void>((value) => {
      stored.value = value;
    });
    const send = vi.fn<() => Promise<void>>(async () => undefined);
    const coordinator = new HeatmapExecutionCoordinator(
      { read: () => stored.value, write },
      { send },
    );
    expect(await coordinator.ensure(request)).toBe("backoff");
    expect(write).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
    now.mockReturnValue(1001);
    expect(await coordinator.ensure(request)).toBe("queued");
    expect(send).toHaveBeenCalledTimes(1);
    expect(stored.value.failures).toBe(2);
  } finally {
    now.mockRestore();
  }
});

it.each(["queued", "running"])(
  "does not infer %s completion from elapsed time or failure count",
  async (phase) => {
    const state = { phase, token: "x", failures: 3, retryAt: 0, updatedAt: Date.now() };
    const write = vi.fn<() => void>();
    const send = vi.fn<() => Promise<void>>(async () => undefined);
    const coordinator = new HeatmapExecutionCoordinator({ read: () => state, write }, { send });
    expect(await coordinator.ensure(request)).toStrictEqual(phase);
    expect(write).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  },
);

it.each([
  undefined,
  "qualified-frame-sql-v2:gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
])("recovers settled generation %s once and fences old tokens", async (dependencyRevision) => {
  const state: { value: unknown } = {
    value: {
      phase: "settled",
      token: "old",
      failures: 3,
      retryAt: Number.MAX_SAFE_INTEGER,
      ...(dependencyRevision === undefined ? {} : { dependencyRevision }),
    },
  };
  const send = vi.fn<() => Promise<void>>(async () => undefined);
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => state.value,
      write: (value) => {
        state.value = value;
      },
    },
    { send },
  );
  const input = {
    ...request,
    keibajoCode: "A8",
    dependencyRevision:
      "qualified-frame-sql-v2:gate-v1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  };
  expect(await Promise.all([coordinator.ensure(input), coordinator.ensure(input)])).toStrictEqual([
    "queued",
    "queued",
  ]);
  expect(send).toHaveBeenCalledTimes(1);
  const saved = state.value;
  if (
    typeof saved !== "object" ||
    saved === null ||
    !("token" in saved) ||
    typeof saved.token !== "string"
  )
    throw new Error("Missing token");
  expect(saved).toMatchObject({
    failures: 0,
    retryAt: 0,
    dependencyRevision: input.dependencyRevision,
  });
  expect(saved.token).not.toBe("old");
  expect(coordinator.claim("old")).toBe(false);
  expect(coordinator.settle("old", true)).toBe(false);
  expect(coordinator.claim(saved.token)).toBe(true);
  expect(coordinator.settle("old", false)).toBe(false);
  expect(coordinator.settle(saved.token, false)).toBe(true);
  expect(await coordinator.ensure(input)).toBe("backoff");
  state.value = { ...saved, phase: "settled", failures: 3, retryAt: Number.MAX_SAFE_INTEGER };
  expect(await coordinator.ensure(input)).toBe("backoff");
  expect(send).toHaveBeenCalledTimes(1);
});
it.each([
  undefined,
  "",
  "bad",
  "qualified-frame-sql-v2:gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
])("does not reset same or invalid dependency %s", async (dependencyRevision) => {
  const write = vi.fn<() => void>();
  const send = vi.fn<() => Promise<void>>(async () => undefined);
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => ({
        phase: "settled",
        token: "old",
        failures: 3,
        retryAt: Number.MAX_SAFE_INTEGER,
        dependencyRevision:
          "qualified-frame-sql-v2:gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      }),
      write,
    },
    { send },
  );
  expect(await coordinator.ensure({ ...request, keibajoCode: "A8", dependencyRevision })).toBe(
    "backoff",
  );
  expect(write).not.toHaveBeenCalled();
  expect(send).not.toHaveBeenCalled();
});
it.each(["queued", "running", "unknown"])(
  "retains %s ownership on revision change",
  async (phase) => {
    const write = vi.fn<() => void>();
    const coordinator = new HeatmapExecutionCoordinator(
      {
        read: () => ({ phase, token: "old", failures: 3, retryAt: 0, updatedAt: Date.now() }),
        write,
      },
      { send: vi.fn<() => Promise<void>>() },
    );
    expect(
      await coordinator.ensure({
        ...request,
        keibajoCode: "A8",
        dependencyRevision:
          "qualified-frame-sql-v2:gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      }),
    ).toStrictEqual(phase);
    expect(write).not.toHaveBeenCalled();
  },
);
it("retains malformed persisted provenance and domestic counters", async () => {
  const write = vi.fn<() => void>();
  const stored = {
    value: {
      phase: "settled",
      token: "old",
      failures: 3,
      retryAt: Number.MAX_SAFE_INTEGER,
      dependencyRevision: "bad",
    },
  };
  const coordinator = new HeatmapExecutionCoordinator(
    { read: () => stored.value, write },
    { send: vi.fn<() => Promise<void>>() },
  );
  const dependencyRevision =
    "qualified-frame-sql-v2:gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  expect(await coordinator.ensure({ ...request, keibajoCode: "A8", dependencyRevision })).toBe(
    "unknown",
  );
  stored.value.dependencyRevision =
    "qualified-frame-sql-v2:gate-v1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  expect(await coordinator.ensure({ ...request, dependencyRevision })).toBe("backoff");
  expect(write).not.toHaveBeenCalled();
});
it("waits for actual active settlement before adopting a changed algorithm revision", async () => {
  const stored: { value: unknown } = {
    value: {
      phase: "running",
      token: "old",
      failures: 2,
      retryAt: 0,
      updatedAt: Date.now(),
      dependencyRevision:
        "qualified-frame-sql-v2:gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
  };
  const send = vi.fn<() => Promise<void>>(async () => undefined);
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => stored.value,
      write: (value) => {
        stored.value = value;
      },
    },
    { send },
  );
  const input = {
    ...request,
    keibajoCode: "A8",
    dependencyRevision:
      "qualified-frame-sql-v3:gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  };
  expect(await coordinator.ensure(input)).toBe("running");
  expect(send).not.toHaveBeenCalled();
  expect(coordinator.settle("old", false)).toBe(true);
  expect(await coordinator.ensure(input)).toBe("queued");
  expect(send).toHaveBeenCalledTimes(1);
  expect(coordinator.settle("old", true)).toBe(false);
});
it.each([undefined, "bad"])(
  "never clears legacy overseas counters without verified provenance %s",
  async (dependencyRevision) => {
    const write = vi.fn<() => void>();
    const send = vi.fn<() => Promise<void>>();
    const coordinator = new HeatmapExecutionCoordinator(
      {
        read: () => ({
          phase: "settled",
          token: "legacy",
          failures: 3,
          retryAt: Number.MAX_SAFE_INTEGER,
        }),
        write,
      },
      { send },
    );
    expect(await coordinator.ensure({ ...request, keibajoCode: "A8", dependencyRevision })).toBe(
      "backoff",
    );
    expect(write).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  },
);

it.each([
  ["queued", "2026-10-16T23:30:00.000Z"],
  ["running", "2026-10-17T00:45:00.000Z"],
])("re-queues a %s owner that has not transitioned since %s", async (phase, updatedAt) => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-17T01:00:00.000Z"));
  const stored: { value: unknown } = {
    value: {
      phase,
      token: "dead",
      failures: 1,
      retryAt: 0,
      updatedAt: new Date(updatedAt).getTime(),
    },
  };
  const send = vi.fn<() => Promise<void>>(async () => undefined);
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => stored.value,
      write: (value) => {
        stored.value = value;
      },
    },
    { send },
  );
  expect(await coordinator.ensure(request)).toBe("queued");
  expect(send).toHaveBeenCalledTimes(1);
  expect(stored.value).toMatchObject({
    failures: 1,
    phase: "queued",
    retryAt: 0,
    updatedAt: new Date("2026-10-17T01:00:00.000Z").getTime(),
  });
  expect(coordinator.claim("dead")).toBe(false);
  vi.useRealTimers();
});

it.each([
  ["queued", "2026-10-16T23:30:00.001Z"],
  ["running", "2026-10-17T00:45:00.001Z"],
])(
  "keeps a %s owner that transitioned after %s instead of re-sending it",
  async (phase, updatedAt) => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-17T01:00:00.000Z"));
    const send = vi.fn<() => Promise<void>>(async () => undefined);
    const coordinator = new HeatmapExecutionCoordinator(
      {
        read: () => ({
          phase,
          token: "live",
          failures: 0,
          retryAt: 0,
          updatedAt: new Date(updatedAt).getTime(),
        }),
        write: vi.fn<() => void>(),
      },
      { send },
    );
    expect(await coordinator.ensure(request)).toBe(phase);
    expect(send).not.toHaveBeenCalled();
    vi.useRealTimers();
  },
);

it("re-queues a legacy unsettled owner written before transitions were timestamped", async () => {
  const stored: { value: unknown } = {
    value: { phase: "queued", token: "legacy", failures: 0, retryAt: 0 },
  };
  const send = vi.fn<() => Promise<void>>(async () => undefined);
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => stored.value,
      write: (value) => {
        stored.value = value;
      },
    },
    { send },
  );
  expect(await coordinator.ensure(request)).toBe("queued");
  expect(send).toHaveBeenCalledTimes(1);
});

it("treats a state with a non-numeric updatedAt as unknown", async () => {
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => ({ phase: "running", token: "x", failures: 0, retryAt: 0, updatedAt: "now" }),
      write: vi.fn<() => void>(),
    },
    { send: vi.fn<() => Promise<void>>() },
  );
  expect(await coordinator.ensure(request)).toBe("unknown");
});
