// Run with bun. All service I/O is mocked.
import { expect, it, vi } from "vitest";

import { readCatalogRaceHistory } from "./race-history-catalog";
import type { CatalogRaceHistoryBinding, RaceHistoryRow } from "./race-history-catalog";

const row: RaceHistoryRow = {
  kettoTorokuBango: "2021106753",
  kaisaiNen: "2023",
  kaisaiTsukihi: "1112",
  keibajoCode: "08",
  raceBango: "07",
  umaban: "05",
  kyori: "1200",
  sohaTime: "1150",
  kohan3f: "375",
  bataiju: "512",
  futanJuryo: "560",
  timeSa: "023",
  kakuteiChakujun: "02",
};
// The route returns the raw R2 column names, so the payload stays snake_case.
const payload = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  ketto_toroku_bango: "2021106753",
  kaisai_nen: "2023",
  kaisai_tsukihi: "1112",
  keibajo_code: "08",
  race_bango: "07",
  umaban: "05",
  kyori: "1200",
  soha_time: "1150",
  kohan_3f: "375",
  bataiju: "512",
  futan_juryo: "560",
  time_sa: "023",
  kakutei_chakujun: "02",
  ...overrides,
});
const query = {
  horseIds: ["2021106753"],
  beforeDate: "20260920",
  minDate: "20230920",
  limit: 2000,
};
const bindingFor = (
  body: unknown,
  init: ResponseInit = {},
): { binding: CatalogRaceHistoryBinding; fetch: ReturnType<typeof vi.fn> } => {
  const fetch = vi
    .fn<CatalogRaceHistoryBinding["fetch"]>()
    .mockResolvedValue(
      Response.json(body, { ...init, headers: init.headers ?? { "cache-control": "no-store" } }),
    );
  return { binding: { fetch }, fetch };
};

it("reads validated history rows and sends the window parameters", async () => {
  const { binding, fetch } = bindingFor({ rows: [payload()] });
  await expect(readCatalogRaceHistory(binding, query)).resolves.toStrictEqual([row]);
  const request: Request | undefined = fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-history?horseIds=2021106753&beforeDate=20260920&limit=2000&minDate=20230920",
  );
  expect(request?.redirect).toBe("manual");
});

it("omits minDate when no lower bound is requested", async () => {
  const { binding, fetch } = bindingFor({ rows: [] });
  await expect(readCatalogRaceHistory(binding, { ...query, minDate: null })).resolves.toStrictEqual(
    [],
  );
  const request: Request | undefined = fetch.mock.calls[0]?.[0];
  expect(request?.url).not.toContain("minDate");
});

it("fails closed on an unusable binding or query", async () => {
  await expect(readCatalogRaceHistory(undefined, query)).rejects.toThrow(
    "Catalog race history unavailable",
  );
  const binding = bindingFor({ rows: [] }).binding;
  await expect(readCatalogRaceHistory(binding, { ...query, horseIds: [] })).rejects.toThrow(
    "Catalog race history unavailable",
  );
  await expect(
    readCatalogRaceHistory(binding, { ...query, horseIds: ["20211067"] }),
  ).rejects.toThrow("Catalog race history unavailable");
  await expect(
    readCatalogRaceHistory(binding, {
      ...query,
      horseIds: Array.from(
        { length: 41 },
        (_unused, index) => `20211067${String(index).padStart(2, "0")}`,
      ),
    }),
  ).rejects.toThrow("Catalog race history unavailable");
});

it.each([
  { body: { rows: [payload()] }, init: { status: 503 } },
  { body: { rows: [payload()] }, init: { status: 200, headers: {} } },
  { body: { rows: "nope" } },
  { body: { rows: [payload({ ketto_toroku_bango: "0" })] } },
  { body: { rows: [payload({ ketto_toroku_bango: "2021999999" })] } },
  { body: { rows: [payload({ kyori: 1200 })] } },
  { body: { rows: [payload({ extra: "1" })] } },
  { body: { rows: [payload({ kaisai_nen: null })] } },
  { body: { rows: Array.from({ length: 4001 }, () => payload()) } },
])("rejects a malformed catalog payload %j", async ({ body, init }) => {
  await expect(readCatalogRaceHistory(bindingFor(body, init ?? {}).binding, query)).rejects.toThrow(
    "Catalog race history unavailable",
  );
});
