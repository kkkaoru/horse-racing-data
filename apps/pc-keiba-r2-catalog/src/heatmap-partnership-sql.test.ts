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
    date: "20260913",
    keibajoCode: "06",
    kind: "jockeyVenue",
    revision: "snapshot1",
    source: "jra",
  },
};

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

it("selects a shared three-year venue cohort independently of race conditions", () => {
  const sql = buildPartnershipTargetRacesQuery(input);
  expect(sql).toMatch("SELECT DISTINCT");
  expect(sql).toMatch("FROM pc_keiba.jvd_se se");
  expect(sql).toMatch("< '20260913'");
  expect(sql).toMatch(">= '20230913'");
  expect(sql).toMatch("se.keibajo_code = '06'");
  expect(sql).not.toMatch(/kyori|track_code|grade_code|current_race/u);
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
  expect(sql).not.toMatch(/kishumei|chokyoshimei|kyori|track_code/u);
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
