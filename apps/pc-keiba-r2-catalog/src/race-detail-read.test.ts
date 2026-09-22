// Runs with bun through Vitest; all provider reads are mocked.
import { expect, it, vi } from "vitest";
import {
  buildRaceDetailReadSql,
  readRaceDetail,
  type RaceDetailReadInput,
} from "./race-detail-read";
const input: RaceDetailReadInput = {
  namespace: "pc_keiba",
  source: "jra",
  date: "20260913",
  keibajoCode: "06",
  raceBango: "11",
};
const row: Record<string, string | null> = {
  kaisai_nen: "2026",
  kaisai_tsukihi: "0913",
  keibajo_code: "06",
  race_bango: "11",
  kaisai_kai: "04",
  kaisai_nichime: "02",
  kyosomei_hondai: "  Race　",
  kyosomei_fukudai: "",
  kyosomei_kakkonai: null,
  grade_code: "C",
  kyoso_shubetsu_code: "13",
  kyoso_kigo_code: "A",
  juryo_shubetsu_code: "2",
  kyoso_joken_code: "999",
  kyoso_joken_meisho: null,
  kyori: "1800",
  track_code: "11",
  hasso_jikoku: "1545",
  toroku_tosu: "16",
  shusso_tosu: "15",
  tenko_code: "1",
  babajotai_code_shiba: "1",
  babajotai_code_dirt: "0",
};
it("uses a year-pruned exact identity query and detects duplicates rather than hiding them", () => {
  expect(buildRaceDetailReadSql(input)).toMatch(
    /FROM pc_keiba\.jvd_ra\nWHERE kaisai_nen = '2026'\n  AND kaisai_tsukihi = '0913'\n  AND keibajo_code = '06'\n  AND race_bango = '11'\nLIMIT 2$/u,
  );
  expect(buildRaceDetailReadSql({ ...input, source: "nar" })).toMatch(/FROM pc_keiba\.nvd_ra/u);
});
it.each([
  { namespace: "pc_keiba; DROP TABLE x" },
  { date: "20260229" },
  { date: "20261301" },
  { date: "x" },
  { keibajoCode: "6" },
  { raceBango: "11'" },
])("rejects invalid inputs before calling the provider: %j", async (change) => {
  const query = vi.fn<() => Promise<unknown[]>>();
  await expect(readRaceDetail({ input: { ...input, ...change }, query })).rejects.toThrow(
    "Invalid race detail input",
  );
  expect(query).not.toHaveBeenCalled();
});
it("rejects a source corrupted at a runtime boundary", () => {
  const invalid: RaceDetailReadInput = { ...input };
  Reflect.set(invalid, "source", "other");
  expect(() => buildRaceDetailReadSql(invalid)).toThrow("Invalid race detail input");
});
it.each(["A8", "A6"])(
  "preserves actual uppercase overseas venue codes: %s",
  async (keibajoCode) => {
    const query = vi.fn().mockResolvedValue([{ ...row, keibajo_code: keibajoCode }]);
    const result = await readRaceDetail({ input: { ...input, keibajoCode }, query });
    expect(result?.keibajoCode).toMatch(/^A[68]$/u);
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toMatch(/AND keibajo_code = 'A[68]'/u);
  },
);
it.each([{ keibajoCode: "a8" }, { keibajoCode: "A'" }, { raceBango: "A8" }])(
  "rejects malformed venues without broadening race numbers: %j",
  (change) => {
    expect(() => buildRaceDetailReadSql({ ...input, ...change })).toThrow(
      "Invalid race detail input",
    );
  },
);
it("accepts a real leap date", () => {
  expect(buildRaceDetailReadSql({ ...input, date: "20240229" })).toMatch(
    /kaisai_tsukihi = '0229'/u,
  );
});
it("retains strings, spaces, leading zeroes, empty values and nulls without coercion", async () => {
  const query = vi.fn().mockResolvedValue([row]);
  expect(await readRaceDetail({ input, query })).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0913",
    keibajoCode: "06",
    raceBango: "11",
    kaisaiKai: "04",
    kaisaiNichime: "02",
    kyosomeiHondai: "  Race　",
    kyosomeiFukudai: "",
    kyosomeiKakkonai: null,
    gradeCode: "C",
    kyosoShubetsuCode: "13",
    kyosoKigoCode: "A",
    juryoShubetsuCode: "2",
    kyosoJokenCode: "999",
    kyosoJokenMeisho: null,
    kyori: "1800",
    trackCode: "11",
    hassoJikoku: "1545",
    torokuTosu: "16",
    shussoTosu: "15",
    tenkoCode: "1",
    babajotaiCodeShiba: "1",
    babajotaiCodeDirt: "0",
    source: "jra",
  });
  expect(query).toHaveBeenCalledTimes(1);
});
it("returns authoritative absence without another provider call", async () => {
  const query = vi.fn().mockResolvedValue([]);
  expect(await readRaceDetail({ input, query })).toBeNull();
  expect(query).toHaveBeenCalledTimes(1);
});
it("collapses identical duplicate rows returned by the R2 SQL planner", async () => {
  const detail = await readRaceDetail({ input, query: vi.fn().mockResolvedValue([row, row, row]) });
  expect(detail).toMatchObject({
    kaisaiNen: "2026",
    raceBango: "11",
    kyori: "1800",
    source: "jra",
  });
});
it("rejects conflicting duplicate rows for one race", async () => {
  await expect(
    readRaceDetail({ input, query: vi.fn().mockResolvedValue([row, { ...row, kyori: "1600" }]) }),
  ).rejects.toThrow("Ambiguous race detail identity");
});
it.each([null, [], 1, "row", { ...row, kyori: 1800 }, { ...row, tenko_code: undefined }])(
  "rejects malformed rows: %j",
  async (value) => {
    await expect(
      readRaceDetail({ input, query: vi.fn().mockResolvedValue([value]) }),
    ).rejects.toThrow(/Invalid race detail row|Missing or invalid race detail field/u);
  },
);
it.each([
  { kaisai_nen: "2025" },
  { kaisai_tsukihi: "0914" },
  { keibajo_code: "05" },
  { race_bango: "12" },
  { kaisai_nen: null },
])("rejects mismatched or null identity: %j", async (change) => {
  await expect(
    readRaceDetail({ input, query: vi.fn().mockResolvedValue([{ ...row, ...change }]) }),
  ).rejects.toThrow("Race detail identity mismatch");
});
it("propagates provider failure instead of returning absence or falling back", async () => {
  const query = vi.fn().mockRejectedValue(new Error("Provider unavailable"));
  await expect(readRaceDetail({ input, query })).rejects.toThrow("Provider unavailable");
  expect(query).toHaveBeenCalledTimes(1);
});
