import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";

const sourceIds = readFileSync(
  resolve(process.cwd(), "sql/20260815020100_seed_jacques_le_marois_runner_source_ids.sql"),
  "utf8",
);
const histories = readFileSync(
  resolve(process.cwd(), "sql/20260815020200_seed_jacques_le_marois_horse_history.sql"),
  "utf8",
);
const enrichment = readFileSync(
  resolve(process.cwd(), "sql/20260815020300_enrich_jacques_le_marois_jvd_runner_data.sql"),
  "utf8",
);

it("maps every final Jacques le Marois runner to both source identities", () => {
  expect(sourceIds.match(/^  \('jra', '2026', '0816', 'A8', '04'/gmu)).toHaveLength(20);
  expect(sourceIds.match(/'jra-van'/gu)).toHaveLength(10);
  expect(sourceIds.match(/'netkeiba'/gu)).toHaveLength(10);
  expect(sourceIds).toMatch(/'09', 'netkeiba', '000a02d63e', '05271', 'a0746', 'a00b0a', 9/u);
  expect(sourceIds).toMatch(/'08', 'netkeiba', '000a02ca97', '05504', '05701', 'a00762', 10/u);
});

it("seeds all parsed histories with source-aware partial-index upserts", () => {
  expect(histories.match(/^  \('jra-van'/gmu)).toHaveLength(117);
  expect(histories).toMatch(
    /on conflict \(source, source_horse_id, source_race_id\) where source_race_id is not null/u,
  );
  expect(histories).toMatch(
    /on conflict \(source, source_horse_id, race_date, venue, race_day_sequence\) where source_race_id is null/u,
  );
  expect(histories).toMatch(/null, '取消'/u);
});

it("enriches only placeholder fields from verified masters and canonical display data", () => {
  expect(enrichment).toMatch(/join jvd_ks ks on ks\.kishu_code = mapping\.source_jockey_id/u);
  expect(enrichment).toMatch(/btrim\(se\.kishu_code\) = '00000'/u);
  expect(enrichment).toMatch(/join jvd_ch ch on ch\.chokyoshi_code = mapping\.source_trainer_id/u);
  expect(enrichment).toMatch(/btrim\(se\.chokyoshi_code\) = '00000'/u);
  expect(enrichment).toMatch(/set banushimei = left\(identity\.owner_name_full, 64\)/u);
  expect(enrichment).toMatch(/umaban in \('01', '02', '05', '06', '07', '08', '09', '10'\)/u);
  expect(enrichment).not.toMatch(/delete|truncate|drop/iu);
});
