// Run with bun. Stored results only; never a published career total.
import type {
  R2SqlCatalogConfig,
  WinRateHeatmapStatsFilters,
  WinRateHeatmapSimilarRow,
} from "./types";
import type { PartnershipStatsRow } from "./heatmap-partnership-stats";
import { tableName } from "./win-rate-heatmap-stats";
export interface ForeignOwnerMetadata {
  statsSource: "netkeiba";
  statsScope: "stored-results-10y" | "stored-results-race-10y" | "stored-results-venue-30y";
  sourcePersonId: string;
}
export interface ForeignOwnerResult {
  similarRows: WinRateHeatmapSimilarRow[];
  foreignOwnerVenueRows: (PartnershipStatsRow & ForeignOwnerMetadata)[];
}
interface LoadInput {
  config: R2SqlCatalogConfig;
  filters: WinRateHeatmapStatsFilters;
  nativeRows: WinRateHeatmapSimilarRow[];
  execute: (sql: string) => Promise<Record<string, unknown>[]>;
}
const scopes: Record<"owner" | "ownerRace" | "ownerVenue", ForeignOwnerMetadata["statsScope"]> = {
  owner: "stored-results-10y",
  ownerRace: "stored-results-race-10y",
  ownerVenue: "stored-results-venue-30y",
};
export const foreignOwnerEligible = (f: WinRateHeatmapStatsFilters): boolean =>
  f.includeForeignSourceStats === true &&
  f.source === "jra" &&
  f.date === "20261004" &&
  f.keibajoCode === "A8" &&
  f.raceBango === "05" &&
  f.years === 10 &&
  ((!f.includeVenue && !f.includeRaceTitle) || (f.includeVenue && f.includeRaceTitle === true)) &&
  !f.includeDistance &&
  !f.includeSurface &&
  !f.includeTurn &&
  !f.includeAge &&
  !f.includeClass &&
  !f.includeConditionKey &&
  !f.includeGrade &&
  !f.includeTrackCode;

// Count the non-null joined identity: R2 rejects count(*) over these UNION cohorts (60104).
export const buildForeignOwnerQuery = (config: R2SqlCatalogConfig): string => `WITH targets AS (
 SELECT umaban, min(source_owner_id) AS source_person_id
 FROM ${tableName(config, "oversea_runner_source_id")}
 WHERE source = 'netkeiba' AND race_source = 'jra'
 AND kaisai_nen = '2026' AND kaisai_tsukihi = '1004'
 AND keibajo_code = 'A8' AND race_bango = '05'
 AND regexp_match(umaban, '^(0[1-9]|1[0-6])$') IS NOT NULL
 GROUP BY umaban HAVING count(*) = 1
 AND min(source_owner_id) <> '000000'
 AND regexp_match(min(source_owner_id), '^[0-9a-z]{6}$') IS NOT NULL
), canonical AS (
 SELECT h.source, h.source_person_id, h.source_race_id,
 coalesce(nullif(h.source_horse_id, ''), h.horse_name) AS horse_key,
 min(h.race_date) AS race_date, min(h.venue) AS venue, min(h.surface) AS surface,
 min(h.race_name) AS race_name, min(h.finish_position) AS finish_position,
 CASE WHEN count(DISTINCT h.race_date) <> 1 OR count(DISTINCT coalesce(h.venue, '')) <> 1
 OR count(DISTINCT coalesce(h.race_name, '')) <> 1
 OR count(DISTINCT coalesce(h.surface, '')) <> 1 OR count(DISTINCT h.finish_position) <> 1
 THEN 1 ELSE 0 END AS conflict
 FROM ${tableName(config, "oversea_person_race_history")} h
 JOIN (SELECT DISTINCT source_person_id FROM targets) t ON t.source_person_id = h.source_person_id
 WHERE h.source = 'netkeiba' AND h.person_kind = 'owner'
 AND h.finish_position > 0
 AND h.race_date >= DATE '1996-10-04' AND h.race_date < DATE '2026-10-04'
 GROUP BY h.source, h.source_person_id, h.source_race_id,
 coalesce(nullif(h.source_horse_id, ''), h.horse_name)
), cohorts AS (
 SELECT *, 'owner' AS kind FROM canonical
 WHERE race_date >= DATE '2016-10-04' AND race_date < DATE '2026-10-04' AND finish_position > 0
 AND race_name IN ('凱旋門賞', '凱旋門賞(GI)')
 UNION ALL
 SELECT *, 'ownerRace' AS kind FROM canonical
 WHERE race_date >= DATE '2016-10-04' AND race_date < DATE '2026-10-04' AND finish_position > 0
 AND venue = 'パリロンシャン' AND race_name IN ('凱旋門賞', '凱旋門賞(GI)')
 UNION ALL
 SELECT *, 'ownerVenue' AS kind FROM canonical
 WHERE race_date >= DATE '1996-10-04' AND race_date < DATE '2026-10-04' AND finish_position > 0
 AND venue = 'パリロンシャン' AND surface = '芝'
 AND race_name IN ('凱旋門賞', '凱旋門賞(GI)')
)
SELECT t.umaban, t.source_person_id, c.kind, count(c.source_person_id) AS starts,
 sum(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS wins,
 sum(CASE WHEN finish_position <= 2 THEN 1 ELSE 0 END) AS places,
 sum(CASE WHEN finish_position <= 3 THEN 1 ELSE 0 END) AS shows,
 (SELECT coalesce(sum(conflict), 0) FROM canonical) AS conflicts
FROM targets t JOIN cohorts c ON c.source_person_id = t.source_person_id
WHERE try_cast(t.umaban AS INT) BETWEEN 1 AND 16
GROUP BY t.umaban, t.source_person_id, c.kind
ORDER BY t.umaban, c.kind LIMIT 49`;

const count = (v: unknown): number => {
  if (
    (typeof v !== "string" && typeof v !== "number" && typeof v !== "bigint") ||
    String(v).trim() === ""
  )
    throw new Error("Malformed foreign owner count");
  const n: number = Number(v);
  if (!Number.isSafeInteger(n) || n < 0) throw new Error("Malformed foreign owner count");
  return n;
};
export const normaliseForeignOwnerRows = (rows: Record<string, unknown>[]): ForeignOwnerResult => {
  if (rows.length > 48) throw new Error("Foreign owner result exceeds bound");
  const result: ForeignOwnerResult = { similarRows: [], foreignOwnerVenueRows: [] };
  const seen: Map<string, string> = new Map();
  rows.forEach((r) => {
    const umaban: number = count(r.umaban);
    const starts: number = count(r.starts);
    const wins: number = count(r.wins);
    const places: number = count(r.places);
    const shows: number = count(r.shows);
    if (
      count(r.conflicts) !== 0 ||
      typeof r.umaban !== "string" ||
      !/^(?:0[1-9]|1[0-6])$/u.test(r.umaban) ||
      umaban < 1 ||
      umaban > 16 ||
      starts === 0 ||
      wins > places ||
      places > shows ||
      shows > starts ||
      typeof r.source_person_id !== "string" ||
      !/^[0-9a-z]{6}$/u.test(r.source_person_id) ||
      r.source_person_id === "000000" ||
      (r.kind !== "owner" && r.kind !== "ownerRace" && r.kind !== "ownerVenue")
    )
      throw new Error("Invalid foreign owner aggregate");
    const metadata: ForeignOwnerMetadata = {
      statsSource: "netkeiba",
      statsScope: scopes[r.kind],
      sourcePersonId: r.source_person_id,
    };
    const row = {
      ...metadata,
      umaban,
      starts,
      wins,
      places,
      shows,
      name: `netkeiba:${r.source_person_id}`,
    };
    const key: string = `${umaban}:${r.kind}`;
    const signature: string = JSON.stringify(row);
    if (seen.has(key)) {
      if (seen.get(key) !== signature) throw new Error("Conflicting foreign owner aggregate");
      return;
    }
    seen.set(key, signature);
    if (r.kind !== "ownerVenue") result.similarRows.push({ ...row, kind: "owner", details: [] });
    else result.foreignOwnerVenueRows.push({ ...row, kind: "ownerVenue" });
  });
  return result;
};
export const loadForeignOwnerStats = async (input: LoadInput): Promise<ForeignOwnerResult> => {
  if (!foreignOwnerEligible(input.filters)) return { similarRows: [], foreignOwnerVenueRows: [] };
  const result: ForeignOwnerResult = normaliseForeignOwnerRows(
    await input.execute(buildForeignOwnerQuery(input.config)),
  );
  return {
    ...result,
    similarRows:
      input.filters.includeOwner === true
        ? result.similarRows.filter(
            (row) =>
              row.statsScope ===
                (input.filters.includeRaceTitle
                  ? "stored-results-race-10y"
                  : "stored-results-10y") &&
              !input.nativeRows.some(
                (native) =>
                  native.kind === "owner" && native.umaban === row.umaban && native.starts > 0,
              ),
          )
        : [],
  };
};
