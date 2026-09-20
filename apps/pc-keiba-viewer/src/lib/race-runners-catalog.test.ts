// Run with bun. All service I/O is mocked.
import { expect, it, vi } from "vitest";

import { readCatalogRaceRunners } from "./race-runners-catalog";
import type { CatalogRaceRunnersBinding } from "./race-runners-catalog";
import type { Runner } from "./race-types";

const runner: Runner = {
  wakuban: "1",
  umaban: "01",
  kettoTorokuBango: "2024101291",
  bamei: "バビット",
  moshokuCode: "1",
  seibetsuCode: "1",
  barei: "3",
  futanJuryo: "550",
  kishumeiRyakusho: "丹内祐次",
  chokyoshimeiRyakusho: "武市康男",
  banushimei: "　",
  bataiju: "480",
  zogenFugo: " ",
  zogenSa: "   ",
  kakuteiChakujun: "01",
  tanshoOdds: "0123",
  tanshoNinkijun: "02",
  sohaTime: "1234",
  timeSa: "0005",
  corner1: "03",
  corner2: "02",
  corner3: "01",
  corner4: "01",
  kohan3f: "345",
  blinkerShiyoKubun: "0",
  sireName: "Nicobar                             ",
  sireSireName: null,
  damSireName: "Kaldounevees                        ",
};
const identity = {
  umaban: "01",
  identitySource: "netkeiba",
  sourceHorseId: "2021100675",
  sourceUrl: "https://example.test/horse/2021100675",
  horseNameFull: "Horse Name",
  jockeyNameFull: null,
  trainerNameFull: null,
  ownerNameFull: null,
};
const query = {
  source: "jra" as const,
  date: "20260920",
  keibajoCode: "06",
  raceBango: "01",
};
const bindingFor = (body: unknown, init: ResponseInit = {}): CatalogRaceRunnersBinding => ({
  fetch: vi.fn<CatalogRaceRunnersBinding["fetch"]>().mockResolvedValue(
    Response.json(body, {
      ...init,
      headers: init.headers ?? { "cache-control": "no-store" },
    }),
  ),
});

it("reads runners and merges the overseas identity by umaban", async () => {
  const binding = bindingFor({ runners: [runner], identities: [identity] });
  const rows: Runner[] = await readCatalogRaceRunners(binding, query);
  expect(rows).toStrictEqual([{ ...runner, ...identity }]);
  const request: Request | undefined = (
    binding.fetch as unknown as { mock: { calls: [Request][] } }
  ).mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-runners?source=jra&date=20260920&keibajoCode=06&raceBango=01",
  );
  expect(request?.headers.get("authorization")).toBe(null);
  expect(request?.redirect).toBe("manual");
});

it("leaves runners without an identity untouched", async () => {
  await expect(
    readCatalogRaceRunners(bindingFor({ runners: [runner], identities: [] }), query),
  ).resolves.toStrictEqual([runner]);
});

it("fails closed when the binding is unavailable", async () => {
  await expect(readCatalogRaceRunners(undefined, query)).rejects.toThrow(
    "Catalog race runners unavailable",
  );
});

it.each([
  { body: { runners: [runner], identities: [] }, init: { status: 503 } },
  { body: { runners: [runner], identities: [] }, init: { status: 200, headers: {} } },
  { body: { runners: "nope", identities: [] } },
  { body: { runners: [runner] } },
  { body: { runners: [{ ...runner, umaban: "19" }], identities: [] } },
  { body: { runners: [{ ...runner, kettoTorokuBango: "" }], identities: [] } },
  { body: { runners: [{ ...runner, bamei: 1 }], identities: [] } },
  { body: { runners: [{ ...runner, extra: "1" }], identities: [] } },
  { body: { runners: [], identities: [{ ...identity, umaban: "1" }] } },
  { body: { runners: [], identities: [identity, identity] } },
  { body: { runners: [], identities: [{ ...identity, extra: 1 }] } },
  {
    body: {
      runners: Array.from({ length: 19 }, (_unused, index) => ({
        ...runner,
        umaban: String((index % 18) + 1).padStart(2, "0"),
      })),
      identities: [],
    },
  },
])("rejects a malformed catalog payload %j", async ({ body, init }) => {
  await expect(readCatalogRaceRunners(bindingFor(body, init ?? {}), query)).rejects.toThrow(
    "Catalog race runners unavailable",
  );
});
