// Run with bun. All service I/O is mocked.
import { expect, it, vi } from "vitest";

import { readCatalogOverseasRaceHistory } from "./race-history-overseas-catalog";
import type { CatalogOverseasRaceHistoryBinding } from "./race-history-overseas-catalog";

const query = {
  horseIds: ["2021106753"],
  beforeDate: "20260920",
  minDate: "20230920",
  limit: 2000,
};
const bindingFor = (
  body: unknown,
  init: ResponseInit = {},
): { binding: CatalogOverseasRaceHistoryBinding; fetch: ReturnType<typeof vi.fn> } => {
  const fetch = vi
    .fn<CatalogOverseasRaceHistoryBinding["fetch"]>()
    .mockResolvedValue(
      Response.json(body, { ...init, headers: init.headers ?? { "cache-control": "no-store" } }),
    );
  return { binding: { fetch }, fetch };
};

it("reads overseas rows and requests the overseas mode", async () => {
  const { binding, fetch } = bindingFor({
    rows: [{ sourceHorseId: "2021106753", raceDate: "2026-04-26", distanceMetres: "1600" }],
  });
  await expect(readCatalogOverseasRaceHistory(binding, query)).resolves.toStrictEqual([
    { sourceHorseId: "2021106753", raceDate: "2026-04-26", distanceMetres: 1600 },
  ]);
  const request: Request | undefined = fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-history?source=overseas&horseIds=2021106753&beforeDate=20260920&limit=2000&minDate=20230920",
  );
  expect(request?.redirect).toBe("manual");
});

it("accepts a numeric distance and a null one", async () => {
  const { binding } = bindingFor({
    rows: [
      { sourceHorseId: "2021106753", raceDate: "2026-04-26", distanceMetres: 1600 },
      { sourceHorseId: "2021106753", raceDate: "2026-04-25", distanceMetres: null },
    ],
  });
  await expect(readCatalogOverseasRaceHistory(binding, query)).resolves.toStrictEqual([
    { sourceHorseId: "2021106753", raceDate: "2026-04-26", distanceMetres: 1600 },
    { sourceHorseId: "2021106753", raceDate: "2026-04-25", distanceMetres: null },
  ]);
});

it("fails closed on an unusable binding or query", async () => {
  await expect(readCatalogOverseasRaceHistory(undefined, query)).rejects.toThrow(
    "Catalog race history unavailable",
  );
  const { binding } = bindingFor({ rows: [] });
  await expect(readCatalogOverseasRaceHistory(binding, { ...query, horseIds: [] })).rejects.toThrow(
    "Catalog race history unavailable",
  );
  await expect(
    readCatalogOverseasRaceHistory(binding, { ...query, beforeDate: "2026-09-20" }),
  ).rejects.toThrow("Catalog race history unavailable");
});

it.each([
  { body: { rows: [] }, init: { status: 503 } },
  { body: { rows: [] }, init: { status: 200, headers: {} } },
  { body: { rows: "nope" } },
  { body: { rows: [{ sourceHorseId: "0", raceDate: "2026-04-26", distanceMetres: null }] } },
  {
    body: { rows: [{ sourceHorseId: "2021999999", raceDate: "2026-04-26", distanceMetres: null }] },
  },
  {
    body: { rows: [{ sourceHorseId: "2021106753", raceDate: "2026-02-30", distanceMetres: null }] },
  },
  {
    body: {
      rows: [{ sourceHorseId: "2021106753", raceDate: "2026-04-26", distanceMetres: "abc" }],
    },
  },
  { body: { rows: [{ sourceHorseId: "2021106753", raceDate: "2026-04-26" }] } },
])("rejects a malformed catalog payload %j", async ({ body, init }) => {
  await expect(
    readCatalogOverseasRaceHistory(bindingFor(body, init ?? {}).binding, query),
  ).rejects.toThrow("Catalog race history unavailable");
});
