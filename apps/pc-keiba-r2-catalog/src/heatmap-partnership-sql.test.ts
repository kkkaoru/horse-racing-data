// Run with bun (bunx vitest).
import { expect, it } from "vitest";
import {
  buildPartnershipCountsQuery,
  buildPartnershipEntriesQuery,
  buildPartnershipTargetRacesQuery,
  type PartnershipQueryInput,
} from "./heatmap-partnership-sql";

const input: PartnershipQueryInput = {
  config: {
    R2_SQL_ACCOUNT_ID: "account",
    R2_SQL_BUCKET_NAME: "bucket",
    R2_SQL_NAMESPACE: "pc_keiba",
    R2_SQL_TOKEN: "test",
  },
  horseIds: [],
  scope: {
    surface: "芝",
    date: "20260913",
    keibajoCode: "06",
    kind: "jockeyVenue",
    revision: "snapshot1",
    source: "jra",
  },
};

it("selects owner-only venue history and all-venue exact triples for thirty years", () => {
  const owner = buildPartnershipCountsQuery({
    ...input,
    scope: { ...input.scope, kind: "ownerVenue" },
  });
  expect(owner).toMatch("nullif(btrim(se.banushi_code), '') AS partner_id");
  expect(owner).toMatch("'owner' AS jockey_id");
  expect(owner).toMatch("upcoming.keibajo_code = '06'");
  expect(owner).toMatch("upcoming.kaisai_nen = '2026'");
  expect(owner).toMatch("upcoming.kaisai_tsukihi = '0913'");
  expect(owner).toMatch(
    "nullif(btrim(upcoming.banushi_code), '') = nullif(btrim(se.banushi_code), '')",
  );
  expect(owner).toMatch("se.keibajo_code = '06'");
  expect(owner).toMatch(">= '19960913'");
  expect(owner).toMatch("END = '芝'");
  const triple = buildPartnershipCountsQuery({
    ...input,
    scope: { ...input.scope, kind: "jockeyTrainerOwner", surface: "障害" },
  });
  expect(triple).toMatch("nullif(btrim(se.banushi_code), '') AS owner_id");
  expect(triple).toMatch("nullif(btrim(se.chokyoshi_code), '') AS partner_id");
  expect(triple).toMatch("nullif(btrim(se.kishu_code), '') AS jockey_id");
  expect(triple).toMatch("GROUP BY jockey_id, partner_id, owner_id");
  expect(triple).toMatch("FROM pc_keiba.jvd_se se");
  expect(triple).toMatch("FROM pc_keiba.nvd_se se");
  expect(triple).toMatch("FROM pc_keiba.jvd_ra ra");
  expect(triple).toMatch("FROM pc_keiba.nvd_ra ra");
  expect(triple).toMatch("UNION ALL");
  const targets = buildPartnershipTargetRacesQuery({
    ...input,
    scope: { ...input.scope, kind: "jockeyTrainerOwner" },
  });
  expect(targets).toMatch("'jra' AS source");
  expect(targets).toMatch("'nar' AS source");
  expect(targets).toMatch("UNION ALL");
  expect(triple).toMatch("btrim(upcoming.kishu_code) = btrim(se.kishu_code)");
  expect(triple).toMatch("btrim(upcoming.chokyoshi_code) = btrim(se.chokyoshi_code)");
  expect(triple).not.toMatch("upcoming.keibajo_code =");
  expect(triple).not.toMatch("se.keibajo_code = '06'");
  expect(triple).toMatch(">= '19960913'");
  expect(triple).toMatch("END = '障害'");
  expect(triple).not.toMatch(/kyori|grade_code|current_race/u);
});

it("selects stable current partnership IDs for one target race", () => {
  const sql = buildPartnershipEntriesQuery({
    config: input.config,
    scope: input.scope,
    raceBango: "01",
  });
  expect(sql).toMatch("se.kishu_code AS jockey_id");
  expect(sql).toMatch("se.chokyoshi_code AS trainer_id");
  expect(sql).toMatch("se.kaisai_nen = '2026'");
  expect(sql).toMatch("se.kaisai_tsukihi = '0913'");
  expect(sql).toMatch("se.race_bango = '01'");
});

it("selects a shared three-year venue cohort matching the surface independently of other race conditions", () => {
  const sql = buildPartnershipTargetRacesQuery(input);
  expect(sql).toMatch("SELECT DISTINCT");
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).toMatch("< '20260913'");
  expect(sql).toMatch(">= '20230913'");
  expect(sql).toMatch("se.keibajo_code = '06'");
  expect(sql).toMatch("ra.track_code");
  expect(sql).toMatch("END = '芝'");
  expect(sql).not.toMatch(/kyori|grade_code|current_race/u);
});

it("aggregates ten-year jockey-trainer pairs by stable IDs with deduplicated starters", () => {
  const sql = buildPartnershipCountsQuery({
    ...input,
    scope: { ...input.scope, kind: "jockeyTrainerVenue", source: "nar", keibajoCode: "44" },
  });
  expect(sql).toMatch("FROM pc_keiba.nvd_se se");
  expect(sql).toMatch(">= '20160913'");
  expect(sql).toMatch("se.keibajo_code = '44'");
  expect(sql).toMatch("nullif(btrim(se.chokyoshi_code), '') AS partner_id");
  expect(sql).toMatch("SELECT DISTINCT");
  expect(sql).toMatch(
    "WHERE finish_position > 0 AND jockey_id IS NOT NULL AND partner_id IS NOT NULL",
  );
  expect(sql).toMatch("GROUP BY jockey_id, partner_id");
  expect(sql).toMatch("END = '芝'");
  expect(sql).not.toMatch(/kishumei|chokyoshimei|kyori/u);
});

it("keeps ban-ei separate from flat and obstacle cohorts", () => {
  const sql = buildPartnershipCountsQuery({
    ...input,
    scope: { ...input.scope, surface: "ばんえい", source: "nar", keibajoCode: "83" },
  });
  expect(sql).toMatch("btrim(ra.track_code) = '90' THEN 'ばんえい'");
  expect(sql).toMatch("END = 'ばんえい'");
  expect(sql).toMatch("FROM pc_keiba.nvd_ra ra");
});

it("limits lifetime horse-jockey scans to target horses across venues", () => {
  const sql = buildPartnershipCountsQuery({
    ...input,
    horseIds: ["2023100002", "2023100001", "2023100001"],
    scope: { ...input.scope, kind: "horseJockey" },
  });
  expect(sql).toMatch("IN ('2023100001', '2023100002')");
  expect(sql).toMatch("nullif(btrim(se.ketto_toroku_bango), '') AS partner_id");
  expect(sql).not.toMatch(/>=|se.keibajo_code =/u);
  expect(sql).toMatch("< '20260913'");
});

it("rejects unbounded horse scans and invalid IDs before querying", () => {
  expect(() =>
    buildPartnershipCountsQuery({ ...input, scope: { ...input.scope, kind: "horseJockey" } }),
  ).toThrow("valid target horse IDs");
  expect(() =>
    buildPartnershipCountsQuery({
      ...input,
      horseIds: ["' OR TRUE"],
      scope: { ...input.scope, kind: "horseJockey" },
    }),
  ).toThrow("valid target horse IDs");
});
