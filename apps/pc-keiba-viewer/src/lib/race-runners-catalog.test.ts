// Run with bun. All service I/O is mocked.
import { expect, it, vi } from "vitest";

import published from "../fixtures/overseas-published.json";
import evidence from "./__fixtures__/arc-official-card.json";
import { enrichOfficialRunners } from "./arc-official-card";
import { readCatalogRaceRunners } from "./race-runners-catalog";
import type { CatalogRaceRunnersBinding } from "./race-runners-catalog";
import type { Runner } from "./race-types";
import { buildWinRateHeatmapRunnerSignature } from "./win-rate-heatmap-cache";

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
const bindingFor = (
  body: unknown,
  init: ResponseInit = {},
): { binding: CatalogRaceRunnersBinding; fetch: ReturnType<typeof vi.fn> } => {
  const fetch = vi.fn<CatalogRaceRunnersBinding["fetch"]>().mockResolvedValue(
    Response.json(body, {
      ...init,
      headers: init.headers ?? { "cache-control": "no-store" },
    }),
  );
  return { binding: { fetch }, fetch };
};

it("reads runners and merges the overseas identity by umaban", async () => {
  const { binding, fetch } = bindingFor({ runners: [runner], identities: [identity] });
  const rows: Runner[] = await readCatalogRaceRunners(binding, query);
  expect(rows).toStrictEqual([{ ...runner, ...identity }]);
  const request: Request | undefined = fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-runners?source=jra&date=20260920&keibajoCode=06&raceBango=01",
  );
  expect(request?.headers.get("authorization")).toBe(null);
  expect(request?.redirect).toBe("manual");
});

it("leaves runners without an identity untouched", async () => {
  await expect(
    readCatalogRaceRunners(bindingFor({ runners: [runner], identities: [] }).binding, query),
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
  await expect(readCatalogRaceRunners(bindingFor(body, init ?? {}).binding, query)).rejects.toThrow(
    "Catalog race runners unavailable",
  );
});

it("hydrates every full gate and provider ID while retaining display identity", async () => {
  const { binding } = bindingFor({
    runners: published.mappings.map((mapping) => ({
      ...runner,
      umaban: mapping.umaban,
      kettoTorokuBango: "0000000000",
    })),
    identities: [{ ...identity, identitySource: "jra-van", horseNameFull: "Preserved display" }],
    sourceMappings: published.mappings.map((mapping) => ({
      umaban: mapping.umaban,
      sourceHorseId: mapping.source_horse_id,
      gateNumber: mapping.gate_number,
    })),
  });
  const rows = await readCatalogRaceRunners(binding, { ...query, keibajoCode: "A8" });
  expect(rows.map((row) => row.wakuban)).toStrictEqual([
    "5",
    "9",
    "2",
    "12",
    "13",
    "14",
    "15",
    "10",
    "6",
    "7",
    "4",
    "1",
    "8",
    "11",
    "3",
    "16",
  ]);
  expect(rows.map((row) => row.sourceHorseId)).toStrictEqual([
    "000a029d42",
    "000a02cbc8",
    "000a02d860",
    "2021105369",
    "2021103272",
    "000a02bb45",
    "000a024a34",
    "000a029d4f",
    "000a024a0d",
    "000a024841",
    "000a02d8c0",
    "000a02caba",
    "000a02daf6",
    "000a02d68d",
    "000a02d40c",
    "000a02ca5a",
  ]);
  expect(rows[0]).toMatchObject({
    identitySource: "jra-van",
    sourceHorseProvider: "netkeiba",
    horseNameFull: "Preserved display",
  });
});
it("keeps missing mapping and missing gate unknown instead of using lossy JV values", async () => {
  const { binding } = bindingFor({
    runners: [runner, { ...runner, umaban: "02" }],
    identities: [identity],
    sourceMappings: [{ umaban: "02", sourceHorseId: "000a029d42", gateNumber: null }],
  });
  const rows = await readCatalogRaceRunners(binding, { ...query, keibajoCode: "A8" });
  expect(rows.map((row) => ({ id: row.sourceHorseId, gate: row.wakuban }))).toStrictEqual([
    { id: null, gate: null },
    { id: "000a029d42", gate: null },
  ]);
});
it.each([
  undefined,
  [null],
  [{}],
  [{ umaban: "01", sourceHorseId: "x'", gateNumber: 1 }],
  [{ umaban: "01", sourceHorseId: "000a029d42", gateNumber: 0 }],
  [{ umaban: "01", sourceHorseId: "000a029d42", gateNumber: 1.2 }],
  [{ umaban: "01", sourceHorseId: "000a029d42", gateNumber: 32768 }],
  [{ umaban: "01", sourceHorseId: "000a029d42", gateNumber: "12" }],
  [
    { umaban: "01", sourceHorseId: "000a029d42", gateNumber: 12 },
    { umaban: "01", sourceHorseId: "000a029d42", gateNumber: 13 },
  ],
])("rejects legacy or malformed overseas source mappings %j", async (sourceMappings) => {
  const { binding } = bindingFor({ runners: [runner], identities: [], sourceMappings });
  await expect(readCatalogRaceRunners(binding, { ...query, keibajoCode: "A8" })).rejects.toThrow(
    "Catalog race runners unavailable",
  );
});

it("signs all sixteen actual runners without full-name identities after official enrichment", async () => {
  const rows = await readCatalogRaceRunners(
    bindingFor({
      runners: evidence.runners.map((row) =>
        Object.fromEntries(Object.keys(runner).map((key) => [key, Reflect.get(row, key) ?? null])),
      ),
      identities: [],
      sourceMappings: published.mappings.map((mapping) => ({
        umaban: mapping.umaban,
        sourceHorseId: mapping.source_horse_id,
        gateNumber: mapping.gate_number,
      })),
    }).binding,
    { ...query, keibajoCode: "A8" },
  );
  const enriched = enrichOfficialRunners(rows, evidence.card);
  expect(enriched).toHaveLength(16);
  expect(enriched.every((row) => row.sourceHorseProvider === "netkeiba")).toBe(true);
  expect(buildWinRateHeatmapRunnerSignature(enriched)).not.toBeNull();
  expect(buildWinRateHeatmapRunnerSignature(enriched)).toBe(
    buildWinRateHeatmapRunnerSignature(enriched.toReversed()),
  );
});
