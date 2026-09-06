import { expect, it } from "vitest";
import {
  FAST_NETWORK_PEDIGREE_SOURCE,
  heatmapPedigreeSupplementSql,
} from "./heatmap-pedigree-supplements";

it.each([
  ["ketto_joho_03b", "High Chaparral"],
  ["ketto_joho_07b", "Sadler''s Wells"],
  ["ketto_joho_09b", "Green Desert"],
  ["ketto_joho_11b", "Zeditave"],
  ["ketto_joho_13b", "Stravinsky"],
])("supplements %s only for the verified horse and both matching parents", (column, name) => {
  const sql = heatmapPedigreeSupplementSql(column, "sire_expr", "dam_expr");
  expect(sql).toContain("se.ketto_toroku_bango = '2020190007'");
  expect(sql).toContain("AND sire_expr = 'Wrote'");
  expect(sql).toContain("AND dam_expr = 'Alberta'");
  expect(sql).toContain(`THEN '${name}' ELSE NULL END`);
});

it("does not invent other ancestors and retains provenance", () => {
  expect(heatmapPedigreeSupplementSql("ketto_joho_01b", "sire", "dam")).toBeNull();
  expect(heatmapPedigreeSupplementSql("unknown", "sire", "dam")).toBeNull();
  expect(FAST_NETWORK_PEDIGREE_SOURCE.url).toBe("https://www.breednet.com.au/horse/fast-network");
});
