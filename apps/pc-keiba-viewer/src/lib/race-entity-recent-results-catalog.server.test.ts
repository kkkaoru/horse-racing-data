// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { safeGetCloudflareEnvMock } = vi.hoisted(() => ({
  safeGetCloudflareEnvMock: vi.fn<() => Promise<CloudflareEnv | null>>(),
}));

vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareEnv: safeGetCloudflareEnvMock,
}));

import {
  buildRaceEntityCatalogUrl,
  fetchRaceEntityRecentResultsCatalog,
} from "./race-entity-recent-results-catalog.server";

const query = {
  cursor: null,
  date: "20260827",
  entityType: "horse",
  horseNumber: "7",
  keibajoCode: "50",
  limit: "5",
  raceNumber: "05",
  source: "nar",
};

beforeEach(() => {
  vi.resetAllMocks();
});

it("builds the R2 Catalog query URL with optional cursor and limit", () => {
  expect(buildRaceEntityCatalogUrl(query).href).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-entity-recent-results?date=20260827&keibajoCode=50&raceBango=05&source=nar&horseNumber=7&entityType=horse&limit=5",
  );
  expect(buildRaceEntityCatalogUrl({ ...query, cursor: "opaque", limit: null }).href).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-entity-recent-results?date=20260827&keibajoCode=50&raceBango=05&source=nar&horseNumber=7&entityType=horse&cursor=opaque",
  );
});

it("returns a bounded R2 Catalog response", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue({
    R2_CATALOG: {
      fetch: vi.fn<typeof fetch>(async () =>
        Response.json({ pagination: { returned: 1 }, results: [{}] }),
      ),
    },
  });
  await expect(fetchRaceEntityRecentResultsCatalog(query)).resolves.toStrictEqual({
    status: 200,
    value: { pagination: { returned: 1 }, results: [{}] },
  });
});

it("distinguishes unavailable, malformed, oversized, timeout, and upstream failures", async () => {
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  await expect(fetchRaceEntityRecentResultsCatalog(query)).resolves.toMatchObject({
    value: { error: { code: "UPSTREAM_ERROR" } },
  });

  safeGetCloudflareEnvMock.mockResolvedValue({
    R2_CATALOG: { fetch: vi.fn<typeof fetch>(async () => new Response("not-json")) },
  });
  await expect(fetchRaceEntityRecentResultsCatalog(query)).resolves.toMatchObject({
    value: { error: { code: "MALFORMED_HISTORY_DATA" } },
  });

  safeGetCloudflareEnvMock.mockResolvedValue({
    R2_CATALOG: {
      fetch: vi.fn<typeof fetch>(async () => new Response("x".repeat(65_537))),
    },
  });
  await expect(fetchRaceEntityRecentResultsCatalog(query)).resolves.toMatchObject({
    value: { error: { code: "MALFORMED_HISTORY_DATA" } },
  });

  safeGetCloudflareEnvMock.mockResolvedValue({
    R2_CATALOG: {
      fetch: vi.fn<typeof fetch>(async () => {
        throw new DOMException("timed out", "TimeoutError");
      }),
    },
  });
  await expect(fetchRaceEntityRecentResultsCatalog(query)).resolves.toMatchObject({
    status: 504,
    value: { error: { code: "TIMEOUT" } },
  });

  safeGetCloudflareEnvMock.mockResolvedValue({
    R2_CATALOG: {
      fetch: vi.fn<typeof fetch>(async () => {
        throw new Error("socket closed");
      }),
    },
  });
  await expect(fetchRaceEntityRecentResultsCatalog(query)).resolves.toMatchObject({
    status: 502,
    value: { error: { code: "UPSTREAM_ERROR" } },
  });
});
