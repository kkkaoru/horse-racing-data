// Run with bun. All service I/O is mocked.
import { expect, it, vi } from "vitest";

import { readCatalogRaceCalendar } from "./race-calendar-catalog";
import type { CatalogRaceDetailBinding } from "./race-detail-catalog";
import type { RaceDaySummary } from "./race-types";

it("reads validated counts through a private GET without an administrator credential", async () => {
  const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>().mockResolvedValue(
    Response.json(
      {
        days: [
          { year: "2024", month: "09", day: "17", jraCount: 0, narCount: 24 },
          { year: "2024", month: "02", day: "29", jraCount: 12, narCount: 0 },
        ],
      },
      { headers: { "cache-control": "no-store" } },
    ),
  );
  await expect(readCatalogRaceCalendar({ fetch }, "2024")).resolves.toStrictEqual([
    { year: "2024", month: "09", day: "17", jraCount: 0, narCount: 24 },
    { year: "2024", month: "02", day: "29", jraCount: 12, narCount: 0 },
  ]);
  expect(fetch).toHaveBeenCalledTimes(1);
  const request: Request | undefined = fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe("https://pc-keiba-r2-catalog.internal/v1/race-calendar?year=2024");
  expect(request?.method).toBe("GET");
  expect(request?.redirect).toBe("manual");
  expect(request?.headers.get("authorization")).toBe(null);
});
it("preserves an explicitly empty year without fallback", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ days: [] }, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).resolves.toStrictEqual([]);
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("fails closed when its binding is absent", async () => {
  await expect(readCatalogRaceCalendar(undefined, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
});
it.each(["2026'--", "26", "0000", "0200", "20260", "2026\n"])(
  "rejects invalid year %s before I/O",
  async (year) => {
    const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>();
    await expect(readCatalogRaceCalendar({ fetch }, year)).rejects.toThrow(
      "Catalog race calendar unavailable",
    );
    expect(fetch).not.toHaveBeenCalled();
  },
);
it("sanitizes binding rejection without retry", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockRejectedValue(new Error("private endpoint detail"));
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
});
it.each([204, 301, 302, 303, 307, 308, 401, 404, 500, 503])(
  "does not interpret HTTP %i as an empty year",
  async (status) => {
    const fetch = vi
      .fn<CatalogRaceDetailBinding["fetch"]>()
      .mockResolvedValue(new Response(null, { status }));
    await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
      "Catalog race calendar unavailable",
    );
  },
);
it("cancels an error body", async () => {
  const cancel = vi.fn<() => void>();
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(new ReadableStream({ cancel }), { status: 503 }));
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
  expect(cancel).toHaveBeenCalledTimes(1);
});
it("requires no-store on successful responses", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ days: [] }));
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
});
it.each([null, [], {}, { days: null }, { days: {} }, { days: [], extra: true }])(
  "rejects a malformed envelope %j",
  async (payload) => {
    const fetch = vi
      .fn<CatalogRaceDetailBinding["fetch"]>()
      .mockResolvedValue(Response.json(payload, { headers: { "cache-control": "no-store" } }));
    await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
      "Catalog race calendar unavailable",
    );
  },
);
it.each([
  null,
  [],
  {},
  { year: "2025", month: "09", day: "17", jraCount: 0, narCount: 1 },
  { year: "2026", month: 9, day: "17", jraCount: 0, narCount: 1 },
  { year: "2026", month: "09", day: 17, jraCount: 0, narCount: 1 },
  { year: "2026", month: "9", day: "17", jraCount: 0, narCount: 1 },
  { year: "2026", month: "09", day: "7", jraCount: 0, narCount: 1 },
  { year: "2026", month: "00", day: "17", jraCount: 0, narCount: 1 },
  { year: "2026", month: "02", day: "29", jraCount: 0, narCount: 1 },
  { year: "2026", month: "09", day: "17", jraCount: "1", narCount: 1 },
  { year: "2026", month: "09", day: "17", jraCount: -1, narCount: 1 },
  { year: "2026", month: "09", day: "17", jraCount: 1.5, narCount: 1 },
  { year: "2026", month: "09", day: "17", jraCount: 9007199254740992, narCount: 1 },
  { year: "2026", month: "09", day: "17", jraCount: 1, narCount: null },
  { year: "2026", month: "09", day: "17", jraCount: 0, narCount: 0 },
  { year: "2026", month: "09", day: "17", jraCount: 1, narCount: 0, extra: true },
])("rejects invalid date or count rows %j", async (row) => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json({ days: [row] }, { headers: { "cache-control": "no-store" } }),
    );
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
});
it("rejects duplicate days", async () => {
  const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>().mockResolvedValue(
    Response.json(
      {
        days: [
          { year: "2026", month: "09", day: "17", jraCount: 1, narCount: 0 },
          { year: "2026", month: "09", day: "17", jraCount: 0, narCount: 1 },
        ],
      },
      { headers: { "cache-control": "no-store" } },
    ),
  );
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
});
it("rejects incorrect date ordering", async () => {
  const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>().mockResolvedValue(
    Response.json(
      {
        days: [
          { year: "2026", month: "09", day: "16", jraCount: 1, narCount: 0 },
          { year: "2026", month: "09", day: "17", jraCount: 0, narCount: 1 },
        ],
      },
      { headers: { "cache-control": "no-store" } },
    ),
  );
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
});
it("accepts all 366 distinct days in a leap year", async () => {
  const days: RaceDaySummary[] = Array.from({ length: 366 }, (_, index): RaceDaySummary => {
    const date: string = new Date(Date.UTC(2024, 11, 31 - index)).toISOString();
    return {
      year: "2024",
      month: date.slice(5, 7),
      day: date.slice(8, 10),
      jraCount: 0,
      narCount: 1,
    };
  });
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ days }, { headers: { "cache-control": "no-store" } }));
  expect(await readCatalogRaceCalendar({ fetch }, "2024")).toHaveLength(366);
});
it("rejects a response exceeding one year", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json(
        { days: Array.from({ length: 367 }, () => ({})) },
        { headers: { "cache-control": "no-store" } },
      ),
    );
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
});
it("keeps shared decoder errors private", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response("{", { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceCalendar({ fetch }, "2026")).rejects.toThrow(
    "Catalog race calendar unavailable",
  );
});
