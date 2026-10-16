// Run with bun. Qualified Arc evidence is the only historical event input.
import { isCurrentArc } from "./arc-cohort";
import type { ArcEvidence, ArcQualifiedResult } from "./arc-qualified-people";
import type {
  WinRateHeatmapBloodlineCategory,
  WinRateHeatmapBloodlineRow,
  WinRateHeatmapStatsFilters,
} from "./types";
interface Ancestor {
  id: string | null;
  name: string;
}
interface Role {
  category: WinRateHeatmapBloodlineCategory;
  parent: string | null;
  field: string;
}
export interface ArcBloodlineInput {
  filters: WinRateHeatmapStatsFilters;
  qualified: ArcQualifiedResult;
  profiles: Record<string, unknown>[];
  labels: Record<string, unknown>[];
}
export interface ArcBloodlineResult {
  rows: WinRateHeatmapBloodlineRow[];
  knownBloodlineCount: number;
  coverage: {
    complete: false;
    eligibleStarts: number;
    roles: {
      category: WinRateHeatmapBloodlineCategory;
      knownAncestry: number;
      unknownAncestry: number;
    }[];
  };
}
const CAP: number = 5000;
const ROLES: Role[] = [
  { category: "sire", parent: null, field: "sire" },
  { category: "sireSire", parent: null, field: "sire_sire" },
  { category: "damSire", parent: null, field: "dam_sire" },
  { category: "sireSireSire", parent: "sire", field: "sire_sire" },
  { category: "sireDamSire", parent: "sire", field: "dam_sire" },
  { category: "damSireSire", parent: "dam_sire", field: "sire" },
  { category: "damDamSire", parent: "dam", field: "dam_sire" },
];
const FIELDS: string[] = ["sire", "sire_sire", "dam", "dam_sire"];
const text = (v: unknown): string => (typeof v === "string" ? v.trim() : "");
const id = (v: unknown): string | null =>
  /^[0-9a-z]{10}$/u.test(text(v)) && /[1-9a-z]/u.test(text(v)) ? text(v) : null;
export const arcBloodlineProfilesQuery = (namespace: string): string => {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(namespace)) throw new Error("Invalid Arc namespace");
  return `SELECT source, source_horse_id, ${FIELDS.flatMap((f) => [`${f}_source_id`, `${f}_name`]).join(", ")} FROM ${namespace}.oversea_horse_pedigree WHERE source = 'netkeiba' ORDER BY source, source_horse_id LIMIT 5001`;
};
const profilesById = (rows: Record<string, unknown>[]): Map<string, Record<string, unknown>> => {
  if (rows.length > CAP) throw new Error("Arc profiles exceed bounded read");
  const result: Map<string, Record<string, unknown>> = new Map();
  rows.forEach((row) => {
    if (row.source !== "netkeiba") return;
    const horse: string | null = id(row.source_horse_id);
    if (horse === null) throw new Error("Invalid Arc profile identity");
    const key: string = `netkeiba:${horse}`;
    const previous = result.get(key);
    if (
      previous &&
      FIELDS.some((f) => id(previous[`${f}_source_id`]) !== id(row[`${f}_source_id`]))
    )
      throw new Error("Conflicting Arc profile IDs");
    result.set(key, previous ?? row);
  });
  return result;
};
const ancestor = (input: {
  horse: string | null;
  role: Role;
  profiles: Map<string, Record<string, unknown>>;
}): Ancestor => {
  const root = input.profiles.get(`netkeiba:${input.horse}`);
  const profile =
    input.role.parent === null
      ? root
      : input.profiles.get(`netkeiba:${id(root?.[`${input.role.parent}_source_id`])}`);
  return {
    id: id(profile?.[`${input.role.field}_source_id`]),
    name: text(profile?.[`${input.role.field}_name`]),
  };
};
const scopedEvidence = (events: ArcEvidence[]): ArcEvidence[] => {
  const result: Map<string, ArcEvidence> = new Map();
  events.forEach((event) => {
    const key: string = `netkeiba:${event.horse}:${event.date}`;
    const previous = result.get(key);
    if (
      previous &&
      (previous.outcome !== event.outcome ||
        previous.venue !== event.venue ||
        previous.surface !== event.surface)
    )
      throw new Error("Conflicting Arc bloodline event");
    result.set(key, event);
  });
  return [...result.values()].filter(
    (e) =>
      e.date >= "2016-10-04" &&
      e.date < "2026-10-04" &&
      e.venue === "パリロンシャン" &&
      e.surface === "芝" &&
      typeof e.outcome === "number",
  );
};
export const aggregateArcBloodlines = (input: ArcBloodlineInput): ArcBloodlineResult => {
  const f = input.filters;
  if (
    !isCurrentArc(f) ||
    f.years !== 10 ||
    !f.includeVenue ||
    !f.includeSurface ||
    f.includeDistance ||
    f.includeTurn ||
    f.includeGrade ||
    f.includeTrackCode ||
    f.includeAge ||
    f.includeClass ||
    f.includeConditionKey
  )
    throw new Error("Unsupported Arc bloodline filters");
  const profiles = profilesById(input.profiles);
  const events = scopedEvidence(input.qualified.evidence);
  const targets = input.qualified.rows.filter((r) => r.kind === "horse10Y");
  const rows = targets.flatMap((target) =>
    ROLES.flatMap((role) => {
      const identity = ancestor({ horse: target.people.horse, role, profiles });
      if (identity.id === null) return [];
      const matched = events.filter(
        (e) => ancestor({ horse: e.horse, role, profiles }).id === identity.id,
      );
      // A missing historical ancestor cannot establish a zero count.
      if (
        matched.length === 0 &&
        events.some((e) => ancestor({ horse: e.horse, role, profiles }).id === null)
      )
        return [];
      const native = input.labels.find(
        (label) =>
          Number(label.umaban) === Number(target.umaban) && label.category === role.category,
      );
      const label: string = text(native?.name);
      return [
        {
          category: role.category,
          name: label && label !== "不明" ? label : identity.name || "不明",
          umaban: Number(target.umaban),
          details: [],
          starts: matched.length,
          wins: matched.filter((e) => e.outcome === 1).length,
          places: matched.filter(
            (e) => typeof e.outcome === "number" && e.outcome > 0 && e.outcome <= 2,
          ).length,
          shows: matched.filter(
            (e) => typeof e.outcome === "number" && e.outcome > 0 && e.outcome <= 3,
          ).length,
        } satisfies WinRateHeatmapBloodlineRow,
      ];
    }),
  );
  return {
    rows,
    knownBloodlineCount: targets.reduce(
      (count, target) =>
        count +
        ROLES.filter((role) => ancestor({ horse: target.people.horse, role, profiles }).id !== null)
          .length,
      0,
    ),
    coverage: {
      complete: false,
      eligibleStarts: events.length,
      roles: ROLES.map((role) => {
        const known: number = events.filter(
          (e) => ancestor({ horse: e.horse, role, profiles }).id !== null,
        ).length;
        return {
          category: role.category,
          knownAncestry: known,
          unknownAncestry: events.length - known,
        };
      }),
    },
  };
};
