// Run with bun.
import { expect, it, vi, beforeEach } from "vitest";
vi.mock("server-only", () => ({}));
const { envMock } = vi.hoisted(() => ({ envMock: vi.fn<() => Promise<unknown>>() }));
vi.mock("./cloudflare-context.server", () => ({ safeGetCloudflareEnv: envMock }));
import {
  hasCurrentGateMappingRevision,
  hasCurrentGateMappingBody,
  parseGateMappingRevision,
} from "./gate-mapping-revision.server";
const key: string = "race-detail-section:v18:2026:10:04:A8:05:condition:default";
const revision: string = "gate-v1-4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945";
beforeEach(() => vi.resetAllMocks());
it("invalidates historical corrections even with unchanged current runners", async () => {
  envMock.mockResolvedValue({
    R2_CATALOG: {
      getGateMappingRevision: vi
        .fn<() => Promise<unknown>>()
        .mockResolvedValue(
          "gate-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
    },
  });
  expect(await hasCurrentGateMappingRevision(key, revision)).toBe(false);
});
it("accepts the same exact published revision", async () => {
  envMock.mockResolvedValue({
    R2_CATALOG: {
      getGateMappingRevision: vi.fn<() => Promise<unknown>>().mockResolvedValue(revision),
    },
  });
  expect(
    await hasCurrentGateMappingBody(
      key,
      JSON.stringify({ mappingRevision: revision, frameStats: [] }),
    ),
  ).toBe(true);
});
it.each([undefined, "old", 1])("rejects missing or malformed provenance %s", async (value) => {
  expect(await hasCurrentGateMappingRevision(key, value)).toBe(false);
  expect(() => parseGateMappingRevision(value)).toThrow("provenance");
});
it("fails closed for missing binding", async () => {
  envMock.mockResolvedValue(null);
  expect(await hasCurrentGateMappingRevision(key, revision)).toBe(false);
});
it("fails closed for unsuccessful revision requests", async () => {
  envMock.mockResolvedValue({
    R2_CATALOG: {
      getGateMappingRevision: vi
        .fn<() => Promise<unknown>>()
        .mockResolvedValue(new Response(null, { status: 503 })),
    },
  });
  expect(await hasCurrentGateMappingRevision(key, revision)).toBe(false);
});
it.each([null, 1, { mappingRevision: "wrong" }])(
  "fails closed on malformed response %s",
  async (value) => {
    envMock.mockResolvedValue({
      R2_CATALOG: {
        getGateMappingRevision: vi.fn<() => Promise<unknown>>().mockResolvedValue(value),
      },
    });
    expect(await hasCurrentGateMappingRevision(key, revision)).toBe(false);
  },
);
it("fails closed on network failure", async () => {
  envMock.mockResolvedValue({
    R2_CATALOG: {
      getGateMappingRevision: vi
        .fn<() => Promise<unknown>>()
        .mockRejectedValue(new Error("offline")),
    },
  });
  expect(await hasCurrentGateMappingRevision(key, revision)).toBe(false);
});
it.each(["broken", "null", "1", "{}"])(
  "rejects malformed or legacy condition body %s",
  async (body) => {
    expect(await hasCurrentGateMappingBody(key, body)).toBe(false);
  },
);
it("preserves domestic 77 and unrelated overseas sections without a dependency read", async () => {
  expect(
    await hasCurrentGateMappingRevision(
      "race-detail-section:v18:2026:10:04:77:05:condition:default",
      undefined,
    ),
  ).toBe(true);
  expect(
    await hasCurrentGateMappingBody(
      "race-detail-section:v18:2026:10:04:77:05:condition:default",
      "broken",
    ),
  ).toBe(true);
  expect(
    await hasCurrentGateMappingRevision(
      "race-detail-section:v18:2026:10:04:A8:05:training:default",
      undefined,
    ),
  ).toBe(true);
  expect(envMock).not.toHaveBeenCalled();
});
it("checks heatmap manifests against the same revision", async () => {
  envMock.mockResolvedValue({
    R2_CATALOG: {
      getGateMappingRevision: vi.fn<() => Promise<unknown>>().mockResolvedValue(revision),
    },
  });
  expect(
    await hasCurrentGateMappingRevision(
      "pc-keiba-viewer:win-rate-heatmap:v22:overseas-readers-v1:2026:10:04:A8:05:default",
      revision,
    ),
  ).toBe(true);
});
it("uses the existing binding RPC with only validated scope and no HTTP fallback", async () => {
  const rpc = vi.fn<() => Promise<unknown>>().mockResolvedValue(revision);
  const fetch = vi.fn<() => Promise<unknown>>();
  envMock.mockResolvedValue({ R2_CATALOG: { fetch, getGateMappingRevision: rpc } });
  expect(await hasCurrentGateMappingRevision(key, revision)).toBe(true);
  expect(rpc).toHaveBeenCalledWith({ date: "20261004", keibajoCode: "A8" });
  expect(fetch).not.toHaveBeenCalled();
});
it("fails closed when old Catalog has no RPC", async () => {
  const fetch = vi.fn<() => Promise<unknown>>();
  envMock.mockResolvedValue({ R2_CATALOG: { fetch } });
  expect(await hasCurrentGateMappingRevision(key, revision)).toBe(false);
  expect(fetch).not.toHaveBeenCalled();
});
it.each(["2026:02:30", "2026:13:01", "0000:10:04"])(
  "rejects invalid date before RPC %s",
  async (date) => {
    const rpc = vi.fn<() => Promise<unknown>>();
    envMock.mockResolvedValue({ R2_CATALOG: { getGateMappingRevision: rpc } });
    expect(
      await hasCurrentGateMappingRevision(
        `race-detail-section:v18:${date}:A8:05:condition:default`,
        revision,
      ),
    ).toBe(false);
    expect(rpc).not.toHaveBeenCalled();
  },
);
it("exposes strict current revision for generation-aware owners", async () => {
  const { getCurrentGateMappingRevision } = await import("./gate-mapping-revision.server");
  envMock.mockResolvedValue({
    R2_CATALOG: {
      getGateMappingRevision: vi.fn<() => Promise<unknown>>().mockResolvedValue(revision),
    },
  });
  expect(await getCurrentGateMappingRevision(key)).toBe(
    "gate-v1-4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
  );
});
