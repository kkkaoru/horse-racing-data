// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

const { fetchCatalogMock } = vi.hoisted(() => ({
  fetchCatalogMock: vi.fn<() => Promise<{ status: number; value: unknown }>>(),
}));

vi.mock("../../../../../../../../../lib/race-entity-recent-results-catalog.server", () => ({
  fetchRaceEntityRecentResultsCatalog: fetchCatalogMock,
}));

import { GET } from "./route";

beforeEach(() => {
  vi.resetAllMocks();
});

it("forwards target race and pagination arguments to R2 Catalog", async () => {
  fetchCatalogMock.mockResolvedValue({
    status: 200,
    value: { pagination: { returned: 5 }, results: [] },
  });
  const response = await GET(
    new Request(
      "https://viewer.test/api/races/2026/08/27/50/05/entity-recent-results?source=nar&horseNumber=7&entityType=horse&limit=5&cursor=opaque",
    ),
    {
      params: Promise.resolve({
        day: "27",
        keibajoCode: "50",
        month: "08",
        raceNumber: "05",
        year: "2026",
      }),
    },
  );
  expect(response.status).toBe(200);
  expect(fetchCatalogMock).toHaveBeenCalledWith({
    cursor: "opaque",
    date: "20260827",
    entityType: "horse",
    horseNumber: "7",
    keibajoCode: "50",
    limit: "5",
    raceNumber: "05",
    source: "nar",
  });
});

it("rejects malformed routes and preserves catalog errors", async () => {
  const invalid = await GET(new Request("https://viewer.test/invalid"), {
    params: Promise.resolve({
      day: "27",
      keibajoCode: "50",
      month: "8",
      raceNumber: "05",
      year: "2026",
    }),
  });
  expect(invalid.status).toBe(400);
  await expect(invalid.json()).resolves.toMatchObject({ error: { code: "RACE_NOT_FOUND" } });

  fetchCatalogMock.mockResolvedValue({
    status: 400,
    value: { error: { code: "INVALID_CURSOR", message: "bad cursor" } },
  });
  const catalogError = await GET(new Request("https://viewer.test/valid"), {
    params: Promise.resolve({
      day: "27",
      keibajoCode: "50",
      month: "08",
      raceNumber: "05",
      year: "2026",
    }),
  });
  expect(catalogError.status).toBe(400);
  await expect(catalogError.json()).resolves.toStrictEqual({
    error: { code: "INVALID_CURSOR", message: "bad cursor" },
  });
});
