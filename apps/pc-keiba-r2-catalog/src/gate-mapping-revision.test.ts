// Run with bun.
import { expect, it, vi } from "vitest";
import { loadGateMappingRevision as loadRevision } from "./gate-mapping-revision";
import type { R2SqlCatalogConfig, WinRateHeatmapStatsFilters } from "./types";
const loadGateMappingRevision: typeof loadRevision = (input) =>
  loadRevision({
    ...input,
    fetchImpl: (url, init) =>
      String(init?.body).includes("SELECT race_source, source,")
        ? input.fetchImpl(url, init)
        : Promise.resolve(Response.json({ success: true, result: { rows: [] } })),
  });
const env: R2SqlCatalogConfig = {
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "bucket",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "token",
};
const filters: WinRateHeatmapStatsFilters = {
  date: "20261004",
  keibajoCode: "A8",
  raceBango: "05",
  source: "jra",
  years: 10,
  includeVenue: false,
  includeDistance: false,
  includeSurface: false,
  includeTurn: false,
};
const row = {
  race_source: "jra",
  source: "netkeiba",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0816",
  keibajo_code: "A8",
  race_bango: "04",
  umaban: "01",
  gate_number: 16,
};
it("fingerprints historical content including same-timestamp gate corrections", async () => {
  const fetchImpl = vi
    .fn()
    .mockResolvedValueOnce(Response.json({ success: true, result: { rows: [row] } }))
    .mockResolvedValueOnce(
      Response.json({ success: true, result: { rows: [{ ...row, gate_number: 1 }] } }),
    );
  const first = await loadGateMappingRevision({ env, filters, fetchImpl });
  const second = await loadGateMappingRevision({ env, filters, fetchImpl });
  expect(first).toMatch(/^gate-v1-[a-f0-9]{64}$/u);
  expect(second).not.toEqual(first);
  expect(fetchImpl.mock.calls[0]?.[1]?.body).toMatch("source = 'netkeiba'");
});
it("valid empty publication has a stable revision", async () => {
  const fetchImpl = vi
    .fn()
    .mockImplementation(() =>
      Promise.resolve(Response.json({ success: true, result: { rows: [] } })),
    );
  expect(await loadGateMappingRevision({ env, filters, fetchImpl })).toBe(
    "gate-v1-e47c0fa0e22cdf31706db0c9568edaa6155ca86914e545d1950e88f8d2aa5229",
  );
});
it("does not read or change domestic 77 dependencies", async () => {
  const fetchImpl = vi.fn();
  expect(
    await loadGateMappingRevision({
      env,
      filters: { ...filters, keibajoCode: "77", source: "nar" },
      fetchImpl,
    }),
  ).toBeUndefined();
  expect(fetchImpl).not.toHaveBeenCalled();
});
it("fails closed on read failure", async () => {
  const fetchImpl = vi.fn().mockRejectedValue(new Error("offline"));
  await expect(loadGateMappingRevision({ env, filters, fetchImpl })).rejects.toThrow("offline");
});
it("rejects truncated and malformed publications", async () => {
  const fetchImpl = vi
    .fn()
    .mockResolvedValueOnce(
      Response.json({ success: true, result: { rows: Array.from({ length: 100001 }, () => row) } }),
    )
    .mockResolvedValueOnce(Response.json({ success: true, result: { rows: [{}] } }));
  await expect(loadGateMappingRevision({ env, filters, fetchImpl })).rejects.toThrow(
    "bounded read",
  );
  await expect(loadGateMappingRevision({ env, filters, fetchImpl })).rejects.toThrow("Malformed");
});
it.each([{}, [], "16", "", true, 0, -1, 1.5, 32768])(
  "rejects invalid gate %j",
  async (gate_number) => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(
        Response.json({ success: true, result: { rows: [{ ...row, gate_number }] } }),
      );
    await expect(loadGateMappingRevision({ env, filters, fetchImpl })).rejects.toThrow("Malformed");
  },
);
it.each([
  "race_source",
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "umaban",
])("rejects missing and malformed identity %s", async (field) => {
  const fetchImpl = vi
    .fn()
    .mockImplementation(() =>
      Promise.resolve(
        Response.json({ success: true, result: { rows: [{ ...row, [field]: "" }] } }),
      ),
    );
  await expect(loadGateMappingRevision({ env, filters, fetchImpl })).rejects.toThrow("Malformed");
});
it("rejects the exact suspicious 100000 boundary", async () => {
  const fetchImpl = vi
    .fn()
    .mockResolvedValue(Response.json({ success: true, result: { rows: Array(100000).fill(row) } }));
  await expect(loadGateMappingRevision({ env, filters, fetchImpl })).rejects.toThrow(
    "bounded read",
  );
});
it("hashes explicit NULL and SQL-proven omitted NULL identically", async () => {
  const fetchImpl = vi
    .fn()
    .mockResolvedValueOnce(
      Response.json({ success: true, result: { rows: [{ ...row, gate_number: null }] } }),
    )
    .mockResolvedValueOnce(
      Response.json({
        success: true,
        result: { rows: [{ ...row, gate_number: undefined, gate_number_is_null: true }] },
      }),
    );
  const explicit = await loadGateMappingRevision({ env, filters, fetchImpl });
  expect(await loadGateMappingRevision({ env, filters, fetchImpl })).toStrictEqual(explicit);
});
it.each([
  { gate_number: undefined },
  { gate_number: undefined, gate_number_is_null: false },
  { gate_number: null, gate_number_is_null: false },
  { gate_number: 16, gate_number_is_null: true },
  { gate_number: 16, gate_number_is_null: "false" },
])("rejects unproven NULL or inconsistent marker %j", async (gate) => {
  const fetchImpl = vi
    .fn()
    .mockResolvedValue(Response.json({ success: true, result: { rows: [{ ...row, ...gate }] } }));
  await expect(loadGateMappingRevision({ env, filters, fetchImpl })).rejects.toThrow("Malformed");
});
it.each([1, 32767, null])("accepts gate bounds and nullable marker %s", async (gate_number) => {
  const fetchImpl = vi.fn().mockResolvedValue(
    Response.json({
      success: true,
      result: { rows: [{ ...row, gate_number, gate_number_is_null: gate_number === null }] },
    }),
  );
  expect(await loadGateMappingRevision({ env, filters, fetchImpl })).toMatch(/^gate-v1-/u);
});

const profile = {
  ...row,
  kaisai_tsukihi: "1004",
  source_horse_id: "000a029d42",
  source_jockey_id: "05504",
  source_trainer_id: "05701",
  source_owner_id: "000123",
  sire_source_id: "",
  sire_name: "",
  sire_sire_source_id: "",
  sire_sire_name: "",
  dam_source_id: "",
  dam_name: "",
  dam_sire_source_id: "",
  dam_sire_name: "",
};
const owner = {
  umaban: "01",
  source_person_id: "000123",
  kind: "owner",
  starts: 10,
  wins: 1,
  places: 2,
  shows: 3,
  conflicts: 0,
};
const revision = (profiles: Record<string, unknown>[], owners: Record<string, unknown>[] = []) => {
  const fetchImpl = vi.fn(async (_url, init) =>
    Response.json({
      success: true,
      result: {
        rows: String(init?.body).includes("SELECT m.race_source")
          ? profiles
          : String(init?.body).includes("WITH targets AS")
            ? owners
            : [row],
      },
    }),
  );
  return loadRevision({ env, filters: { ...filters, raceBango: "01" }, fetchImpl });
};
it.each([
  { source_owner_id: "000124" },
  { sire_name: "Sire" },
  { sire_source_id: "000a0115e2" },
  { source_jockey_id: "05505" },
  { source_trainer_id: "05702" },
  { source_horse_id: "000a029d43" },
])("fences changed current dependency %j with unchanged gates", async (change) => {
  expect(await revision([{ ...profile, ...change }])).not.toStrictEqual(await revision([profile]));
});
it("hashes actual normalized owner counts independently of RPC race01", async () => {
  expect(await revision([profile], [owner])).not.toStrictEqual(
    await revision([profile], [{ ...owner, starts: 11 }]),
  );
  expect(await revision([profile], [owner])).toStrictEqual(
    await revision([profile], [{ ...owner, starts: "10" }, owner]),
  );
});
it("is stable under source-qualified profile and aggregate order", async () => {
  const other = { ...profile, race_source: "nar" };
  const venue = { ...owner, kind: "ownerVenue" };
  expect(await revision([profile, other], [owner, venue])).toStrictEqual(
    await revision([other, profile], [venue, owner]),
  );
});
it.each([
  { source_owner_id: undefined },
  { sire_name: null },
  { source_horse_id: "0000000000" },
  { source_owner_id: "bad" },
  { source: "other" },
  { kaisai_tsukihi: "1003" },
  { keibajo_code: "A9" },
  { sire_name: " name " },
])("rejects malformed or stale projection %j", async (change) => {
  await expect(revision([{ ...profile, ...change }])).rejects.toThrow("Malformed");
});
it("rejects ambiguous current joins and capped reads", async () => {
  await expect(revision([profile, profile])).rejects.toThrow("Ambiguous");
  await expect(revision(Array(2048).fill(profile))).rejects.toThrow("bounded");
});
it("projects all race mappings with explicit nulls and fixed owner scope", async () => {
  const fetchImpl = vi.fn(async (_url: unknown, _init?: RequestInit) =>
    Response.json({ success: true, result: { rows: [] } }),
  );
  await loadRevision({ env, filters: { ...filters, raceBango: "01" }, fetchImpl });
  expect(fetchImpl).toHaveBeenCalledTimes(3);
  expect(fetchImpl.mock.calls[1]?.[1]?.body).toMatch(/LEFT JOIN/);
  expect(fetchImpl.mock.calls[1]?.[1]?.body).toMatch(/COALESCE/);
  expect(fetchImpl.mock.calls[1]?.[1]?.body).not.toMatch(/race_bango =/);
  expect(fetchImpl.mock.calls[2]?.[1]?.body).toMatch(/race_bango = '05'/);
});

it("fences actual 30Y venue outputs and rejects malformed owner aggregates", async () => {
  const venue = { ...owner, kind: "ownerVenue" };
  expect(await revision([profile], [venue])).not.toStrictEqual(
    await revision([profile], [{ ...venue, starts: 11 }]),
  );
  await expect(revision([profile], [{ ...owner, conflicts: 1 }])).rejects.toThrow(
    "Invalid foreign owner aggregate",
  );
  await expect(revision([profile], [{ ...owner, starts: undefined }])).rejects.toThrow(
    "Malformed foreign owner count",
  );
});

it("fences race distribution even when all-owner totals remain unchanged", async () => {
  expect(
    await revision([profile], [owner, { ...owner, kind: "ownerRace", starts: 10 }]),
  ).not.toStrictEqual(
    await revision([profile], [owner, { ...owner, kind: "ownerRace", starts: 11 }]),
  );
});

it("private placeholder and exact heatmap share canonical qualified dependencies", async () => {
  const { parseGateMappingRevisionInput } = await import("./gate-mapping-revision");
  const { loadArcQualifiedPeople } = await import("./arc-qualified-people");
  const raw = [{ source: "excluded", finish_position_text: "unknown", updated_at: "fixed" }];
  const queries: string[] = [];
  const fetchImpl: import("./types").Fetcher = async (_url, init) => {
    const body = String(init?.body);
    queries.push(body);
    return Response.json({
      success: true,
      result: { rows: body.includes("SELECT DISTINCT source,") ? raw : [] },
    });
  };
  const qualified = await loadArcQualifiedPeople({
    config: env,
    filters: { ...filters, event: "arc" },
    execute: async (sql) => (sql.includes("SELECT DISTINCT source,") ? raw : []),
  });
  const heatmap = await loadRevision({ env, filters, fetchImpl, arcQualified: qualified });
  expect(queries).toHaveLength(3);
  queries.length = 0;
  const rpc = await loadRevision({
    env,
    filters: parseGateMappingRevisionInput({ date: "20261004", keibajoCode: "A8" }),
    fetchImpl,
  });
  expect(rpc).toStrictEqual(heatmap);
  expect(queries).toHaveLength(6);
  expect(queries[3]).toMatch(/race_bango = '05'/u);
  raw.push({ updated_at: "fixed", finish_position_text: "unknown", source: "excluded" });
  expect(await loadRevision({ env, filters, fetchImpl })).toStrictEqual(heatmap);
  raw[0] = { source: "excluded", finish_position_text: "corrected", updated_at: "fixed" };
  expect(await loadRevision({ env, filters, fetchImpl })).not.toStrictEqual(heatmap);
});

it("fences historical-only Arc pedigree corrections through both heatmap and private RPC", async () => {
  const { parseGateMappingRevisionInput } = await import("./gate-mapping-revision");
  const historical = [{ ...profile, kaisai_nen: "2025", kaisai_tsukihi: "1005", race_bango: "05" }];
  const fetchImpl = vi.fn(async (_url: unknown, init?: RequestInit) =>
    Response.json({
      success: true,
      result: { rows: String(init?.body).includes("SELECT m.race_source") ? historical : [] },
    }),
  );
  const original = await loadRevision({ env, filters, fetchImpl });
  historical[0] = {
    ...profile,
    kaisai_nen: "2025",
    kaisai_tsukihi: "1005",
    race_bango: "05",
    sire_name: "Corrected historical sire",
  };
  const corrected = await loadRevision({ env, filters, fetchImpl });
  expect(corrected).not.toStrictEqual(original);
  expect(
    await loadRevision({
      env,
      filters: parseGateMappingRevisionInput({ date: "20261004", keibajoCode: "A8" }),
      fetchImpl,
    }),
  ).toStrictEqual(corrected);
  expect(fetchImpl.mock.calls[1]?.[1]?.body).toMatch("historical_arc.race_bango = m.race_bango");
  expect(fetchImpl.mock.calls[1]?.[1]?.body).toMatch(
    "btrim(replace(historical_arc.kyosomei_hondai, chr(12288), ''))",
  );
});

it.each([
  { kaisai_nen: "1995", kaisai_tsukihi: "1005", race_source: "jra" },
  { kaisai_nen: "2027", kaisai_tsukihi: "1005", race_source: "jra" },
  { kaisai_nen: "2025", kaisai_tsukihi: "1005", race_source: "nar" },
])("rejects historical profile dependencies outside the exact Arc scope %j", async (change) => {
  const fetchImpl = vi.fn(async (_url: unknown, init?: RequestInit) =>
    Response.json({
      success: true,
      result: {
        rows: String(init?.body).includes("SELECT m.race_source")
          ? [{ ...profile, ...change }]
          : [],
      },
    }),
  );
  await expect(loadRevision({ env, filters, fetchImpl })).rejects.toThrow(
    "Malformed current dependency revision rows",
  );
});

it("qualified read failures propagate through private placeholder revision", async () => {
  const { parseGateMappingRevisionInput } = await import("./gate-mapping-revision");
  const fetchImpl: import("./types").Fetcher = async (_url, init) => {
    if (String(init?.body).includes("SELECT DISTINCT source,"))
      throw new Error("qualified unavailable");
    return Response.json({ success: true, result: { rows: [] } });
  };
  await expect(
    loadRevision({
      env,
      filters: parseGateMappingRevisionInput({ date: "20261004", keibajoCode: "A8" }),
      fetchImpl,
    }),
  ).rejects.toThrow("qualified unavailable");
});

it.each([
  { raceBango: "01" },
  { raceBango: "04" },
  { source: "nar" },
  { date: "20261003" },
] satisfies Partial<WinRateHeatmapStatsFilters>[])(
  "retains non-Arc revision bytes and reads %j",
  async (change) => {
    const fetchImpl = vi.fn(async () => Response.json({ success: true, result: { rows: [] } }));
    expect(await loadRevision({ env, filters: { ...filters, ...change }, fetchImpl })).toBe(
      "gate-v1-bbf6e1fa22ed1f76c1da4f6a32b1f4e0faed7257d10055b50b1ffea3e41d8a7b",
    );
    expect(fetchImpl.mock.calls.length).toBeLessThanOrEqual(3);
  },
);
