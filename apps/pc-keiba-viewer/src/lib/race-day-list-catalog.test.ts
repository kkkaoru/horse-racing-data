// Run with bun. All service I/O is mocked.
import { expect, it, vi } from "vitest";

import { readCatalogRaceDayList, readCatalogRaceDayListWithJockeys } from "./race-day-list-catalog";
import type { CatalogRaceDetailBinding } from "./race-detail-catalog";
import type { RaceListItem } from "./race-types";

const race: RaceListItem = {
  source: "jra",
  kaisaiNen: "2024",
  kaisaiTsukihi: "0229",
  keibajoCode: "05",
  raceBango: "01",
  kyosomeiHondai: "競走　 ",
  kyosomeiFukudai: null,
  gradeCode: "",
  kyosoShubetsuCode: "11",
  kyosoKigoCode: null,
  juryoShubetsuCode: "1",
  jockeyNames: [],
  kyosoJokenCode: "005",
  kyosoJokenMeisho: null,
  kyori: "1600",
  trackCode: "11",
  hassoJikoku: "1000",
  shussoTosu: "18",
};
it("accepts ordered jockey names without changing their order or sharing credentials", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json(
        { races: [{ ...race, jockeyNames: ["\t", "A", "é", "　", "Ｚ", "𐀀"] }] },
        { headers: { "cache-control": "no-store" } },
      ),
    );
  const rows: RaceListItem[] = await readCatalogRaceDayListWithJockeys({ fetch }, "20240229");
  expect(rows.map((row) => row.jockeyNames)).toStrictEqual([["\t", "A", "é", "　", "Ｚ", "𐀀"]]);
  expect(rows[0]?.kyosomeiHondai).toBe("競走　 ");
  expect(fetch).toHaveBeenCalledTimes(1);
  const request: Request | undefined = fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe(
    "https://pc-keiba-r2-catalog.internal/v1/race-day-list-with-jockeys?date=20240229",
  );
  expect(request?.headers.get("authorization")).toBe(null);
  expect(request?.redirect).toBe("manual");
});
it.each(
  [
    null,
    undefined,
    "A",
    [null],
    [1],
    [""],
    [" A"],
    ["A "],
    ["A", "A"],
    ["B", "A"],
    ["𐀀", "Ｚ"],
    ["\ud800"],
  ].map((jockeyNames) => ({ jockeyNames })),
)("rejects malformed or misordered jockey names %j", async ({ jockeyNames }) => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json(
        { races: [{ ...race, jockeyNames }] },
        { headers: { "cache-control": "no-store" } },
      ),
    );
  await expect(readCatalogRaceDayListWithJockeys({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("accepts 89 enriched races exceeding the former 64 KiB body budget", async () => {
  const races: RaceListItem[] = Array.from({ length: 89 }, (_, index) => ({
    ...race,
    raceBango: String(index + 1).padStart(2, "0"),
    jockeyNames: ["騎手名".repeat(80)],
  }));
  const body: string = JSON.stringify({ races });
  expect(new TextEncoder().encode(body).byteLength).toBeGreaterThan(65536);
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(body, { headers: { "cache-control": "no-store" } }));
  const rows: RaceListItem[] = await readCatalogRaceDayListWithJockeys({ fetch }, "20240229");
  expect(rows).toHaveLength(89);
  expect(rows[0]?.raceBango).toBe("01");
  expect(rows[88]?.raceBango).toBe("89");
  expect(rows[88]?.jockeyNames?.[0]?.length).toBe(240);
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("accepts an enriched day response exactly at 1 MiB", async () => {
  const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>().mockResolvedValue(
    new Response('{"races":[]}'.padEnd(1048576, " "), {
      headers: { "cache-control": "no-store" },
    }),
  );
  expect(await readCatalogRaceDayListWithJockeys({ fetch }, "20240229")).toStrictEqual([]);
});
it("rejects and cancels an enriched response above 1 MiB", async () => {
  const cancel = vi.fn<() => void>();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array(1048577));
    },
    cancel,
  });
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(stream, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceDayListWithJockeys({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  expect(cancel).toHaveBeenCalledTimes(1);
});
it("retains the legacy day reader's 64 KiB bound", async () => {
  const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>().mockResolvedValue(
    new Response('{"races":[]}'.padEnd(65537, " "), {
      headers: { "cache-control": "no-store" },
    }),
  );
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it("accepts missing jockeys as an empty array, not an empty race list", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json({ races: [race] }, { headers: { "cache-control": "no-store" } }),
    );
  expect(
    (await readCatalogRaceDayListWithJockeys({ fetch }, "20240229")).map((row) => row.jockeyNames),
  ).toStrictEqual([[]]);
});
it("preserves a genuinely empty jockey-aware day", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ races: [] }, { headers: { "cache-control": "no-store" } }));
  expect(await readCatalogRaceDayListWithJockeys({ fetch }, "20240229")).toStrictEqual([]);
});
it("does not fall back if the jockey-aware binding fails", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockRejectedValue(new Error("private detail"));
  await expect(readCatalogRaceDayListWithJockeys({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("requires a binding for jockey-aware reads", async () => {
  await expect(readCatalogRaceDayListWithJockeys(undefined, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it("reads a complete private day without sharing credentials", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json({ races: [race] }, { headers: { "cache-control": "no-store" } }),
    );
  expect(await readCatalogRaceDayList({ fetch }, "20240229")).toStrictEqual([
    {
      source: "jra",
      kaisaiNen: "2024",
      kaisaiTsukihi: "0229",
      keibajoCode: "05",
      raceBango: "01",
      kyosomeiHondai: "競走　 ",
      kyosomeiFukudai: null,
      gradeCode: "",
      kyosoShubetsuCode: "11",
      kyosoKigoCode: null,
      juryoShubetsuCode: "1",
      jockeyNames: [],
      kyosoJokenCode: "005",
      kyosoJokenMeisho: null,
      kyori: "1600",
      trackCode: "11",
      hassoJikoku: "1000",
      shussoTosu: "18",
    },
  ]);
  expect(fetch).toHaveBeenCalledTimes(1);
  const request: Request | undefined = fetch.mock.calls[0]?.[0];
  expect(request?.url).toBe("https://pc-keiba-r2-catalog.internal/v1/race-day-list?date=20240229");
  expect(request?.method).toBe("GET");
  expect(request?.redirect).toBe("manual");
  expect(request?.headers.get("authorization")).toBe(null);
});
it("preserves genuine empty days", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ races: [] }, { headers: { "cache-control": "no-store" } }));
  expect(await readCatalogRaceDayList({ fetch }, "20240229")).toStrictEqual([]);
});
it("requires the private binding", async () => {
  await expect(readCatalogRaceDayList(undefined, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it.each(["20230229", "20240431", "20241301", "00000229", "2024-02-29", "20240229'"])(
  "rejects invalid date %s without I/O",
  async (date) => {
    const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>();
    await expect(readCatalogRaceDayList({ fetch }, date)).rejects.toThrow(
      "Catalog race day list unavailable",
    );
    expect(fetch).not.toHaveBeenCalled();
  },
);
it("sanitizes transport failure without retry", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockRejectedValue(new Error("private upstream detail"));
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
});
it.each([204, 301, 302, 303, 307, 308, 401, 404, 500, 503])("rejects HTTP %i", async (status) => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(null, { status }));
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it("cancels a rejected body", async () => {
  const cancel = vi.fn<() => void>();
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response(new ReadableStream({ cancel }), { status: 503 }));
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
  expect(cancel).toHaveBeenCalledTimes(1);
});
it("requires no-store", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ races: [] }));
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it.each([null, [], {}, { races: null }, { races: {} }, { races: [], extra: true }])(
  "rejects malformed envelope %j",
  async (value) => {
    const fetch = vi
      .fn<CatalogRaceDetailBinding["fetch"]>()
      .mockResolvedValue(Response.json(value, { headers: { "cache-control": "no-store" } }));
    await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
      "Catalog race day list unavailable",
    );
  },
);
it.each([
  null,
  [],
  {},
  { ...race, source: "unknown" },
  { ...race, kaisaiNen: "2023" },
  { ...race, kaisaiTsukihi: "0228" },
  { ...race, keibajoCode: null },
  { ...race, keibajoCode: "005" },
  { ...race, raceBango: null },
  { ...race, raceBango: "1" },
  { ...race, jockeyNames: null },
  { ...race, jockeyNames: ["騎手"] },
  { ...race, gradeCode: 1 },
  { ...race, gradeCode: undefined },
  { ...race, extra: true },
])("rejects malformed or enriched row %j", async (row) => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json({ races: [row] }, { headers: { "cache-control": "no-store" } }),
    );
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it("rejects duplicate identities", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json({ races: [race, race] }, { headers: { "cache-control": "no-store" } }),
    );
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it("rejects excessive row count without truncation", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(
      Response.json(
        { races: Array.from({ length: 4097 }, () => race) },
        { headers: { "cache-control": "no-store" } },
      ),
    );
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it("accepts ordered ties and null start times", async () => {
  const fetch = vi.fn<CatalogRaceDetailBinding["fetch"]>().mockResolvedValue(
    Response.json(
      {
        races: [
          { ...race, raceBango: "00", hassoJikoku: "0900" },
          race,
          { ...race, source: "nar" },
          { ...race, raceBango: "02" },
          { ...race, keibajoCode: "06" },
          { ...race, raceBango: "03", hassoJikoku: null },
          { ...race, raceBango: "04", hassoJikoku: null },
        ],
      },
      { headers: { "cache-control": "no-store" } },
    ),
  );
  expect(
    (await readCatalogRaceDayList({ fetch }, "20240229")).map(
      (row) => `${row.keibajoCode}/${row.raceBango}/${row.source}`,
    ),
  ).toStrictEqual([
    "05/00/jra",
    "05/01/jra",
    "05/01/nar",
    "05/02/jra",
    "06/01/jra",
    "05/03/jra",
    "05/04/jra",
  ]);
});
it.each([
  { races: [{ ...race, hassoJikoku: null, raceBango: "02" }, race] },
  { races: [{ ...race, hassoJikoku: "1100", raceBango: "02" }, race] },
  { races: [{ ...race, keibajoCode: "06" }, race] },
  { races: [{ ...race, raceBango: "02" }, race] },
  { races: [{ ...race, source: "nar" }, race] },
])("rejects out-of-order provider rows %j", async ({ races }) => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(Response.json({ races }, { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
it("rejects malformed JSON", async () => {
  const fetch = vi
    .fn<CatalogRaceDetailBinding["fetch"]>()
    .mockResolvedValue(new Response("not json", { headers: { "cache-control": "no-store" } }));
  await expect(readCatalogRaceDayList({ fetch }, "20240229")).rejects.toThrow(
    "Catalog race day list unavailable",
  );
});
