// Verified read-time supplements for incomplete imported pedigrees.
// Provider master values always take priority; this never updates master data.
export const FAST_NETWORK_PEDIGREE_SOURCE = {
  registration: "2020190007",
  sire: "Wrote",
  dam: "Alberta",
  verifiedOn: "2026-09-06",
  url: "https://www.breednet.com.au/horse/fast-network",
} as const;

const ANCESTORS: ReadonlyMap<string, string> = new Map([
  ["ketto_joho_03b", "High Chaparral"],
  ["ketto_joho_07b", "Sadler's Wells"],
  ["ketto_joho_09b", "Green Desert"],
  ["ketto_joho_11b", "Zeditave"],
  ["ketto_joho_13b", "Stravinsky"],
]);

// Expressions come only from the internal SQL builder, never request input.
export const heatmapPedigreeSupplementSql = (
  column: string,
  sireSql: string,
  damSql: string,
): string | null => {
  const ancestor = ANCESTORS.get(column);
  if (ancestor === undefined) return null;
  const escaped = ancestor.replaceAll("'", "''");
  return `CASE WHEN se.ketto_toroku_bango = '${FAST_NETWORK_PEDIGREE_SOURCE.registration}'
      AND ${sireSql} = '${FAST_NETWORK_PEDIGREE_SOURCE.sire}'
      AND ${damSql} = '${FAST_NETWORK_PEDIGREE_SOURCE.dam}'
      THEN '${escaped}' ELSE NULL END`;
};
