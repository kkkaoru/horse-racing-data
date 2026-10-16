// Run with bun. Content identity includes historical corrections, even at the same timestamp.
import type { R2SqlCatalogConfig, WinRateHeatmapStatsFilters, Fetcher } from "./types";
import { executeR2Sql } from "./r2-sql";
import { tableName, validateFilters, usesOverseasGate } from "./win-rate-heatmap-stats";
import { buildForeignOwnerQuery, normaliseForeignOwnerRows } from "./foreign-owner-reader";
import { arcTitleSql, isCurrentArc } from "./arc-cohort";
import { loadArcQualifiedPeople, type ArcQualifiedResult } from "./arc-qualified-people";
interface RevisionFilters extends WinRateHeatmapStatsFilters {
  qualifiedArcRevision?: boolean;
}
const GUARD: string = "arc-cohort-policy-v2";
const CURRENT_CAP: number = 2048;
const MAPPING_FIELDS: string[] = [
  "source_horse_id",
  "source_jockey_id",
  "source_trainer_id",
  "source_owner_id",
];
const PEDIGREE_FIELDS: string[] = [
  "sire_source_id",
  "sire_name",
  "sire_sire_source_id",
  "sire_sire_name",
  "dam_source_id",
  "dam_name",
  "dam_sire_source_id",
  "dam_sire_name",
];
const compareTuple = (a: unknown[], b: unknown[]): number =>
  JSON.stringify(a) < JSON.stringify(b) ? -1 : Number(JSON.stringify(a) > JSON.stringify(b));
const MAX_ROWS: number = 100000;
export const loadGateMappingRevision = async (input: {
  env: R2SqlCatalogConfig;
  filters: RevisionFilters;
  fetchImpl: Fetcher;
  arcQualified?: ArcQualifiedResult;
}): Promise<string | undefined> => {
  const filters = validateFilters(input.filters);
  if (!usesOverseasGate(filters)) return undefined;
  const arc: boolean = isCurrentArc(filters) || input.filters.qualifiedArcRevision === true;
  // Include both race sources because similar-history can union them. No updated_at heuristic.
  const rows = await executeR2Sql(
    input.env,
    `SELECT race_source, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, gate_number, gate_number IS NULL AS gate_number_is_null
FROM ${tableName(input.env, "oversea_runner_source_id")}
WHERE source = 'netkeiba' AND keibajo_code = '${filters.keibajoCode}'
  AND concat(kaisai_nen, kaisai_tsukihi) <= '${filters.date}'
ORDER BY race_source, kaisai_nen, kaisai_tsukihi, race_bango, umaban, gate_number
LIMIT 100001`,
    input.fetchImpl,
  );
  if (rows.length >= MAX_ROWS) throw new Error("Gate mapping revision exceeds bounded read");
  const tuples = rows.map((row) => {
    if (!validIdentity(row) || !validGate(row))
      throw new Error("Malformed gate mapping revision rows");
    return [
      row.race_source,
      row.source,
      row.kaisai_nen,
      row.kaisai_tsukihi,
      row.keibajo_code,
      row.race_bango,
      row.umaban,
      row.gate_number === undefined ? null : row.gate_number,
    ];
  });
  // Include historical Arc profiles: correcting a past pedigree changes current role counts.
  const current = await executeR2Sql(
    input.env,
    `SELECT m.race_source, m.source, m.kaisai_nen, m.kaisai_tsukihi, m.keibajo_code, m.race_bango, m.umaban,
${MAPPING_FIELDS.map((field) => `COALESCE(m.${field}, '') AS ${field}`).join(", ")},
${PEDIGREE_FIELDS.map((field) => `COALESCE(p.${field}, '') AS ${field}`).join(", ")}
FROM ${tableName(input.env, "oversea_runner_source_id")} m
LEFT JOIN ${tableName(input.env, "oversea_horse_pedigree")} p
ON p.source = m.source AND p.source_horse_id = m.source_horse_id
WHERE m.source = 'netkeiba' AND m.keibajo_code = '${filters.keibajoCode}'
AND ${arc ? "(" : ""}m.kaisai_nen = '${filters.date.slice(0, 4)}' AND m.kaisai_tsukihi = '${filters.date.slice(4)}'${arc ? ` OR (m.race_source = 'jra' AND concat(m.kaisai_nen, m.kaisai_tsukihi) >= '19961004' AND concat(m.kaisai_nen, m.kaisai_tsukihi) < '${filters.date}' AND EXISTS (SELECT 1 FROM ${tableName(input.env, "jvd_ra")} historical_arc WHERE historical_arc.kaisai_nen = m.kaisai_nen AND historical_arc.kaisai_tsukihi = m.kaisai_tsukihi AND historical_arc.keibajo_code = m.keibajo_code AND historical_arc.race_bango = m.race_bango AND ${arcTitleSql("historical_arc.kyosomei_hondai")})))` : ""}
ORDER BY m.race_source, m.source, m.kaisai_nen, m.kaisai_tsukihi, m.keibajo_code, m.race_bango, m.umaban
LIMIT 2049`,
    input.fetchImpl,
  );
  if (current.length >= CURRENT_CAP)
    throw new Error("Current dependency revision exceeds bounded read");
  const seen: Set<string> = new Set();
  const profiles = current
    .map((row) => {
      const profileDate: string = row.kaisai_nen + String(row.kaisai_tsukihi);
      if (
        !validIdentity(row) ||
        (profileDate !== filters.date &&
          !(
            arc &&
            row.race_source === "jra" &&
            profileDate >= "19961004" &&
            profileDate < filters.date
          )) ||
        row.keibajo_code !== filters.keibajoCode ||
        ![...MAPPING_FIELDS, ...PEDIGREE_FIELDS].every((field) =>
          validProjectedText(field, row[field]),
        )
      )
        throw new Error("Malformed current dependency revision rows");
      const key = [
        row.race_source,
        row.source,
        row.kaisai_nen,
        row.kaisai_tsukihi,
        row.keibajo_code,
        row.race_bango,
        row.umaban,
      ];
      const identity: string = JSON.stringify(key);
      if (seen.has(identity)) throw new Error("Ambiguous current dependency revision rows");
      seen.add(identity);
      return [
        ...key,
        ...MAPPING_FIELDS.map((field) => row[field]),
        ...PEDIGREE_FIELDS.map((field) => row[field]),
      ];
    })
    .sort(compareTuple);
  // RPC race01 is only a parser placeholder. The existing raw reader owns fixed race05 and 10Y/30Y semantics.
  const owners =
    filters.date === "20261004" && filters.keibajoCode === "A8"
      ? normaliseForeignOwnerRows(
          await executeR2Sql(input.env, buildForeignOwnerQuery(input.env), input.fetchImpl),
        )
      : { similarRows: [], foreignOwnerVenueRows: [] };
  const ownerTuples = [...owners.similarRows, ...owners.foreignOwnerVenueRows]
    .map((row) => [
      row.kind,
      row.umaban,
      row.statsSource,
      row.statsScope,
      row.sourcePersonId,
      row.starts,
      row.wins,
      row.places,
      row.shows,
    ])
    .sort(compareTuple);
  const qualified = arc
    ? (input.arcQualified ??
      (await loadArcQualifiedPeople({
        config: input.env,
        filters: { ...filters, raceBango: "05", event: "arc" },
        execute: (sql) => executeR2Sql(input.env, sql, input.fetchImpl),
      })))
    : undefined;
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(
      JSON.stringify(
        qualified === undefined
          ? [GUARD, tuples.sort(compareTuple), profiles, ownerTuples]
          : [
              "arc-cohort-policy-v3-qualified",
              tuples.sort(compareTuple),
              profiles,
              ownerTuples,
              qualified.dependencyTuple,
            ],
      ),
    ),
  );
  return `gate-v1-${Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
};

const validProjectedText = (field: string, value: unknown): boolean => {
  if (typeof value !== "string") return false;
  if (value === "") return true;
  if (field.endsWith("name"))
    return (
      value.trim() === value &&
      Array.from(value).every((character) => character.charCodeAt(0) >= 32)
    );
  const size: number = field === "source_owner_id" ? 6 : 10;
  const length: number = field === "source_jockey_id" || field === "source_trainer_id" ? 5 : size;
  return new RegExp("^[0-9a-z]{" + length + "}$", "u").test(value) && /[1-9a-z]/u.test(value);
};
const validDate = (value: unknown): value is string =>
  typeof value === "string" &&
  /^[1-9][0-9]{3}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])$/u.test(value) &&
  new Date(value.slice(0, 4) + "-" + value.slice(4, 6) + "-" + value.slice(6, 8))
    .toISOString()
    .slice(0, 10)
    .replaceAll("-", "") === value;
const validIdentity = (row: Record<string, unknown>): boolean =>
  (row.race_source === "jra" || row.race_source === "nar") &&
  row.source === "netkeiba" &&
  typeof row.kaisai_nen === "string" &&
  /^[1-9][0-9]{3}$/u.test(row.kaisai_nen) &&
  typeof row.kaisai_tsukihi === "string" &&
  /^[0-9]{4}$/u.test(row.kaisai_tsukihi) &&
  validDate(row.kaisai_nen + row.kaisai_tsukihi) &&
  typeof row.keibajo_code === "string" &&
  /^[A-Z][A-Z0-9]$/u.test(row.keibajo_code) &&
  typeof row.race_bango === "string" &&
  /^(?:0[1-9]|[1-9][0-9])$/u.test(row.race_bango) &&
  typeof row.umaban === "string" &&
  /^(?:0[1-9]|[1-9][0-9])$/u.test(row.umaban);
const validGate = (row: Record<string, unknown>): boolean => {
  // Omitted nullable values require positive SQL evidence; never infer NULL from a missing column.
  if (row.gate_number === undefined) return row.gate_number_is_null === true;
  if (row.gate_number === null)
    return row.gate_number_is_null === undefined || row.gate_number_is_null === true;
  return (
    (row.gate_number_is_null === undefined || row.gate_number_is_null === false) &&
    typeof row.gate_number === "number" &&
    Number.isInteger(row.gate_number) &&
    row.gate_number >= 1 &&
    row.gate_number <= 32767
  );
};
export const parseGateMappingRevisionInput = (value: unknown): RevisionFilters => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid gate revision input");
  const date: unknown = Reflect.get(value, "date");
  const keibajoCode: unknown = Reflect.get(value, "keibajoCode");
  if (
    Object.keys(value).length !== 2 ||
    !validDate(date) ||
    typeof keibajoCode !== "string" ||
    !/^[A-Z0-9]{2}$/u.test(keibajoCode)
  )
    throw new Error("Invalid gate revision input");
  return {
    date,
    keibajoCode,
    raceBango: "01",
    source: "jra",
    years: 10,
    includeVenue: false,
    includeDistance: false,
    includeSurface: false,
    includeTurn: false,
    ...(date === "20261004" && keibajoCode === "A8" ? { qualifiedArcRevision: true } : {}),
  };
};
