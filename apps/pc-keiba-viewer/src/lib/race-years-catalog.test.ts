// Run with bun. Every service request is mocked.
import { expect, it, vi } from "vitest";

import type { CatalogRaceDetailBinding } from "./race-detail-catalog";
import { readCatalogRaceYears } from "./race-years-catalog";

it("reads ordered year summaries privately without administrator credentials", async () => {
  const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>().mockResolvedValue(
    Response.json(
      {
        years: [
          { year: "2026", raceCount: 500, dayCount: 260 },
          { year: "2024", raceCount: 1000, dayCount: 366 },
          { year: "2000", raceCount: 1000, dayCount: 366 },
        ],
      },
      { headers: { "cache-control": "no-store" } },
    ),
  );
  await expect(readCatalogRaceYears({ fetch })).resolves.toStrictEqual([
    { year: "2026", raceCount: 500, dayCount: 260 },
    { year: "2024", raceCount: 1000, dayCount: 366 },
    { year: "2000", raceCount: 1000, dayCount: 366 },
  ]);
  expect(fetch).toHaveBeenCalledTimes(1);
  const request: Request | undefined = fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe("https://pc-keiba-r2-catalog.internal/v1/race-years");
  expect(request?.method).toBe("GET");
  expect(request?.redirect).toBe("manual");
  expect(request?.headers.get("authorization")).toBe(null);
});
it("preserves a successful empty result", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ years: [] }, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceYears({ fetch })).resolves.toStrictEqual([]);
});
it("requires the private binding", async () => {
  await expect(readCatalogRaceYears(undefined)).rejects.toThrow("Catalog race years unavailable");
});
it("sanitizes transport failure without retry", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockRejectedValue(new Error("private transport detail"));
  await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
  expect(fetch).toHaveBeenCalledTimes(1);
});
it.each([204, 301, 302, 303, 307, 308, 401, 404, 500, 503])(
  "rejects HTTP %i rather than inventing absence",
  async (status) => {
    const fetch = vi
      .fn<CatalogRaceDetailBinding["fetch"]>()
      .mockResolvedValue(new Response(null, { status }));
    await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
  },
);
it("cancels a rejected response body", async () => {
  const cancel = vi.fn<() => void>();
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(new ReadableStream({ cancel }), { status: 503 }));
  await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
  expect(cancel).toHaveBeenCalledTimes(1);
});
it("requires no-store on success", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ years: [] }));
  await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
});
it.each([null, [], {}, { years: null }, { years: {} }, { years: [], extra: true }])(
  "rejects malformed envelopes %j",
  async (payload) => {
    const fetch = vi
      .fn<CatalogRaceDetailBinding["fetch"]>()
      .mockResolvedValue(Response.json(payload, { headers: { "cache-control": "no-store" } }));
    await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
  },
);
it.each([
  null,
  [],
  {},
  { year: 2026, raceCount: 1, dayCount: 1 },
  { year: "0000", raceCount: 1, dayCount: 1 },
  { year: "2026\n", raceCount: 1, dayCount: 1 },
  { year: "2026", raceCount: "1", dayCount: 1 },
  { year: "2026", raceCount: 0, dayCount: 1 },
  { year: "2026", raceCount: -1, dayCount: 1 },
  { year: "2026", raceCount: 1.5, dayCount: 1 },
  { year: "2026", raceCount: 9007199254740992, dayCount: 1 },
  { year: "2026", raceCount: 500, dayCount: "260" },
  { year: "2026", raceCount: 500, dayCount: 0 },
  { year: "2026", raceCount: 500, dayCount: 366 },
  { year: "1900", raceCount: 500, dayCount: 366 },
  { year: "2024", raceCount: 500, dayCount: 367 },
  { year: "2026", raceCount: 1, dayCount: 2 },
  { year: "2026", raceCount: 500, dayCount: 260, extra: true },
])("rejects invalid summary rows %j", async (row) => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json({ years: [row] }, { headers: { "cache-control": "no-store" } }),
    );
  await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
});
it.each([
  {
    years: [
      { year: "2026", raceCount: 1, dayCount: 1 },
      { year: "2026", raceCount: 2, dayCount: 1 },
    ],
  },
  {
    years: [
      { year: "2025", raceCount: 1, dayCount: 1 },
      { year: "2026", raceCount: 2, dayCount: 1 },
    ],
  },
])("rejects duplicate or ascending summaries %j", async ({ years }) => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ years }, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
});
it("rejects oversized lists without truncating", async () => {
  const years = Array.from({ length: 257 }, (_, index) => ({
    year: String(2026 - index),
    raceCount: 1,
    dayCount: 1,
  }));
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ years }, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
});
it("accepts the complete bounded list", async () => {
  const years = Array.from({ length: 256 }, (_, index) => ({
    year: String(2026 - index),
    raceCount: 1,
    dayCount: 1,
  }));
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ years }, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceYears({ fetch })).resolves.toHaveLength(256);
});
it("uses the bounded decoder and rejects invalid JSON", async () => {
  const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>().mockResolvedValue(
    new Response("{invalid", {
      headers: { "cache-control": "no-store", "content-type": "application/json" },
    }),
  );
  await expect(readCatalogRaceYears({ fetch })).rejects.toThrow("Catalog race years unavailable");
});
