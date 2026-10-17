// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import {
  getHeatmapExecutionOwner,
  heatmapPendingResponse,
  runOwnedHeatmap,
} from "./heatmap-execution.server";
const env = vi.hoisted(() => vi.fn<(...args: never[]) => unknown>());
vi.mock("./cloudflare-context.server", () => ({ safeGetCloudflareEnv: env }));
it("fails closed without the namespace", async () => {
  env.mockResolvedValue(null);
  await expect(
    getHeatmapExecutionOwner({
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "08",
      raceNumber: "01",
      source: "jra",
      query: "",
    }),
  ).rejects.toThrow("binding is unavailable");
});
it("selects the cache destination owner", async () => {
  const owner = {
    ensure: vi.fn<() => Promise<string>>(),
    claim: vi.fn<() => Promise<boolean>>(),
    settle: vi.fn<(token: string, success: boolean) => Promise<boolean>>(),
  };
  const getByName = vi.fn<() => typeof owner>(() => owner);
  env.mockResolvedValue({ HEATMAP_EXECUTION_OWNER: { getByName } });
  expect(
    await getHeatmapExecutionOwner({
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "08",
      raceNumber: "01",
      source: "jra",
      query: "",
    }),
  ).toBe(owner);
  expect(getByName).toHaveBeenCalledTimes(1);
});
it("duplicate delivery cannot compute or release another request", async () => {
  const owner = {
    ensure: vi.fn<() => Promise<string>>(),
    claim: vi.fn<() => Promise<boolean>>(async () => false),
    settle: vi.fn<(token: string, success: boolean) => Promise<boolean>>(),
  };
  const compute = vi.fn<() => Promise<Response>>();
  const response = await runOwnedHeatmap({ owner, token: "x", compute });
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({
    error: "heatmap_execution_unknown",
    preparation: "running_or_unknown",
  });
  expect(response.headers.get("Retry-After")).toBeNull();
  expect(compute).not.toHaveBeenCalled();
  expect(owner.settle).not.toHaveBeenCalled();
});
it("only releases after calculation and publication settle, independent of a caller abort", async () => {
  const finished = Promise.withResolvers<Response>();
  const owner = {
    ensure: vi.fn<() => Promise<string>>(),
    claim: vi.fn<() => Promise<boolean>>(async () => true),
    settle: vi.fn<() => Promise<boolean>>(async () => true),
  };
  const pending = runOwnedHeatmap({ owner, token: "x", compute: () => finished.promise });
  await Promise.resolve();
  expect(owner.settle).not.toHaveBeenCalled();
  finished.resolve(new Response(null));
  expect((await pending).status).toBe(200);
  expect(owner.settle).toHaveBeenCalledWith("x", true);
});
it("acknowledges a settled response failure", async () => {
  const owner = {
    ensure: vi.fn<() => Promise<string>>(),
    claim: vi.fn<() => Promise<boolean>>(async () => true),
    settle: vi.fn<() => Promise<boolean>>(async () => true),
  };
  await runOwnedHeatmap({
    owner,
    token: "x",
    compute: async () => new Response(null, { status: 503 }),
  });
  expect(owner.settle).toHaveBeenCalledWith("x", false);
});
it("acknowledges thrown settled failures", async () => {
  const owner = {
    ensure: vi.fn<() => Promise<string>>(),
    claim: vi.fn<() => Promise<boolean>>(async () => true),
    settle: vi.fn<() => Promise<boolean>>(async () => true),
  };
  await expect(
    runOwnedHeatmap({
      owner,
      token: "x",
      compute: async () => {
        throw new Error("failed");
      },
    }),
  ).rejects.toThrow("failed");
  expect(owner.settle).toHaveBeenCalledWith("x", false);
});
it("reports stale completion as unknown", async () => {
  const owner = {
    ensure: vi.fn<() => Promise<string>>(),
    claim: vi.fn<() => Promise<boolean>>(async () => true),
    settle: vi.fn<() => Promise<boolean>>(async () => false),
  };
  await expect(
    runOwnedHeatmap({ owner, token: "x", compute: async () => new Response(null) }),
  ).rejects.toThrow("ownership lost");
});

it("keeps a real coordinator running after caller loss until the producing route settles", async () => {
  const { HeatmapExecutionCoordinator } = await import("./heatmap-execution");
  const state: { value: unknown } = { value: undefined };
  const coordinator = new HeatmapExecutionCoordinator(
    {
      read: () => state.value,
      write: (value) => {
        state.value = value;
      },
    },
    { send: async () => undefined },
  );
  const request = {
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "08",
    raceNumber: "01",
    source: "jra",
    query: "",
  } satisfies Parameters<typeof coordinator.ensure>[0];
  await coordinator.ensure(request);
  const persisted = state.value;
  if (
    typeof persisted !== "object" ||
    persisted === null ||
    !("token" in persisted) ||
    typeof persisted.token !== "string"
  )
    throw new Error("Missing persisted token");
  const owner = {
    ensure: coordinator.ensure.bind(coordinator),
    claim: async (token: string) => coordinator.claim(token),
    settle: async (token: string, success: boolean) => coordinator.settle(token, success),
  };
  const publication = Promise.withResolvers<Response>();
  const started = Promise.withResolvers<void>();
  const producer = runOwnedHeatmap({
    owner,
    token: persisted.token,
    compute: () => {
      started.resolve();
      return publication.promise;
    },
  });
  await started.promise;
  const callerLost = Promise.reject(new Error("caller timed out"));
  await expect(Promise.race([producer, callerLost])).rejects.toThrow("caller timed out");
  expect(await coordinator.ensure(request)).toBe("running");
  const duplicateCompute = vi.fn<() => Promise<Response>>();
  expect(
    (await runOwnedHeatmap({ owner, token: persisted.token, compute: duplicateCompute })).status,
  ).toBe(503);
  expect(duplicateCompute).not.toHaveBeenCalled();
  publication.resolve(new Response(null));
  expect((await producer).status).toBe(200);
  expect(await coordinator.ensure(request)).toBe("queued");
});

it.each(["queued", "running", "backoff"])(
  "keeps %s retryable with pending headers",
  async (status) => {
    const response = heatmapPendingResponse(status);
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({
      error: "heatmap_not_warmed",
      preparation: status,
    });
    expect(response.headers.get("Retry-After")).toBe("30");
    expect(response.headers.get("X-Win-Rate-Heatmap-Cache")).toBe("PENDING");
    expect(response.headers.get("Cache-Control")).toBe("private, max-age=0, no-store");
  },
);

it.each(["unknown", "running_or_unknown", "binding_unavailable", "unexpected", ""])(
  "reports uncertain status %s as an error without preparation polling",
  async (status) => {
    const response = heatmapPendingResponse(status);
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({
      error: "heatmap_execution_unknown",
      preparation: status,
    });
    expect(response.headers.get("Retry-After")).toBeNull();
    expect(response.headers.get("X-Win-Rate-Heatmap-Cache")).toBe("ERROR");
    expect(response.headers.get("Cache-Control")).toBe("private, max-age=0, no-store");
  },
);

it("reports exhausted failures separately from uncertain ownership and retryable preparation", async () => {
  const response = heatmapPendingResponse("blocked");
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({
    error: "heatmap_execution_blocked",
    preparation: "blocked",
  });
  expect(response.headers.get("Retry-After")).toBeNull();
  expect(response.headers.get("X-Win-Rate-Heatmap-Cache")).toBe("ERROR");
  expect(response.headers.get("Cache-Control")).toBe("private, max-age=0, no-store");
});

it.each([
  "gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "bad",
  undefined,
  null,
])("uses only strict private RPC revision %s for overseas ensure", async (revision) => {
  const ensure = vi.fn<() => Promise<string>>(async () => "blocked");
  const claim = vi.fn<() => Promise<boolean>>(async () => false);
  const settle = vi.fn<() => Promise<boolean>>(async () => false);
  const rpc = vi.fn<() => Promise<unknown>>(async () => revision);
  const getByName = vi.fn<
    () => { ensure: typeof ensure; claim: typeof claim; settle: typeof settle }
  >(() => ({ ensure, claim, settle }));
  env.mockResolvedValue({
    HEATMAP_EXECUTION_OWNER: { getByName },
    R2_CATALOG: { getGateMappingRevision: rpc },
  });
  const input = {
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "A8",
    raceNumber: "05",
    source: "jra",
    query: "",
    dependencyRevision: "caller-untrusted",
  } satisfies Parameters<typeof getHeatmapExecutionOwner>[0];
  const owner = await getHeatmapExecutionOwner(input);
  expect(await owner.ensure(input)).toBe("blocked");
  expect(rpc).toHaveBeenCalledExactlyOnceWith({ date: "20261004", keibajoCode: "A8" });
  expect(ensure).toHaveBeenCalledWith({
    ...input,
    dependencyRevision:
      typeof revision === "string" && revision.startsWith("gate-v1-")
        ? "qualified-frame-sql-v5:" + revision
        : undefined,
  });
  expect(await owner.claim("old")).toBe(false);
  expect(await owner.settle("old", true)).toBe(false);
  expect(claim).toHaveBeenCalledWith("old");
  expect(settle).toHaveBeenCalledWith("old", true);
});
it("does not supply reset evidence when private RPC is unavailable", async () => {
  const ensure = vi.fn<() => Promise<string>>(async () => "blocked");
  const rpc = vi.fn<() => Promise<unknown>>(async () => {
    throw new Error("unavailable");
  });
  env.mockResolvedValue({
    HEATMAP_EXECUTION_OWNER: { getByName: () => ({ ensure }) },
    R2_CATALOG: { getGateMappingRevision: rpc },
  });
  const input = {
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "A8",
    raceNumber: "05",
    source: "jra",
    query: "",
  } satisfies Parameters<typeof getHeatmapExecutionOwner>[0];
  const owner = await getHeatmapExecutionOwner(input);
  expect(await owner.ensure(input)).toBe("blocked");
  expect(ensure).toHaveBeenCalledWith({ ...input, dependencyRevision: undefined });
});
