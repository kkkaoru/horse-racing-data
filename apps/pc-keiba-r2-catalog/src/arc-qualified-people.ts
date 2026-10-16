// Run with bun. Standalone stored Arc evidence; source IDs are never JV codes.
import { arcTitleSql } from "./arc-cohort";
export interface ArcFilters {
  source: string;
  date: string;
  keibajoCode: string;
  raceBango: string;
  event: "arc";
}
export interface ArcLoadInput {
  config: { R2_SQL_NAMESPACE: string };
  filters: ArcFilters;
  execute: (sql: string) => Promise<Record<string, unknown>[]>;
}
export interface ArcPeople {
  horse: string | null;
  jockey: string | null;
  trainer: string | null;
  owner: string | null;
}
export interface ArcEvidence {
  key: string;
  date: string;
  horse: string;
  people: ArcPeople;
  venue: string;
  surface: string;
  outcome: number | "withdrawn" | "unknown";
  gate: number | null;
  sourceRaceIds: string[];
}
export interface ArcCountRow {
  umaban: string;
  kind: string;
  source: "netkeiba";
  people: ArcPeople;
  scope: "arc-series" | "arc-parislongchamp-turf";
  since: string;
  before: string;
  gate: number | null;
  status: "observed" | "missing-identity" | "unobserved";
  starts: number;
  wins: number;
  top2: number;
  top3: number;
  evidenceKeys: string[];
  unknownKeys: string[];
  withdrawnKeys: string[];
  unverifiedGateKeys: string[];
}
export interface ArcQualifiedResult {
  rows: ArcCountRow[];
  evidence: ArcEvidence[];
  coverage: { complete: false; rawRows: number; invalidHorseRows: number };
  dependencyTuple: readonly unknown[];
}
interface Target {
  umaban: string;
  people: ArcPeople;
  gate: number | null;
}
interface Cohort {
  kind: string;
  roles: (keyof ArcPeople)[];
  since: string;
  venue: boolean;
  gate: boolean;
}
const TITLES: string[] = ["凱旋門賞", "凱旋門賞(GI)"];
const BEFORE: string = "2026-10-04";
const SINCE: string = "1996-10-04";
const CAP: number = 5000;
const ROLES: (keyof ArcPeople)[] = ["horse", "jockey", "trainer", "owner"];
const COHORTS: Cohort[] = [
  { kind: "jockey", roles: ["jockey"], since: "2016-10-04", venue: false, gate: false },
  { kind: "trainer", roles: ["trainer"], since: "2016-10-04", venue: false, gate: false },
  { kind: "owner", roles: ["owner"], since: "2016-10-04", venue: false, gate: false },
  { kind: "horse10Y", roles: ["horse"], since: "2016-10-04", venue: false, gate: false },
  { kind: "horseJockey30Y", roles: ["horse", "jockey"], since: SINCE, venue: false, gate: false },
  { kind: "jockeyVenue3Y", roles: ["jockey"], since: "2023-10-04", venue: true, gate: false },
  {
    kind: "jockeyTrainerVenue10Y",
    roles: ["jockey", "trainer"],
    since: "2016-10-04",
    venue: true,
    gate: false,
  },
  { kind: "ownerVenue30Y", roles: ["owner"], since: SINCE, venue: true, gate: false },
  {
    kind: "triple30Y",
    roles: ["jockey", "trainer", "owner"],
    since: SINCE,
    venue: false,
    gate: false,
  },
  { kind: "jockeyFrame10Y", roles: ["jockey"], since: "2016-10-04", venue: true, gate: true },
];
const text = (v: unknown): string => (typeof v === "string" ? v : "");
const id = (v: unknown, role: keyof ArcPeople): string | null => {
  const size: number = role === "owner" ? 6 : 5;
  const width: number = role === "horse" ? 10 : size;
  return new RegExp(`^[0-9a-z]{${width}}$`, "u").test(text(v)) && /[1-9a-z]/u.test(text(v))
    ? text(v)
    : null;
};
const gate = (v: unknown): number | null =>
  typeof v === "number" && Number.isInteger(v) && v > 0 && v <= 24 ? v : null;
const date = (v: unknown): string => {
  const value: string = text(v);
  if (
    !/^\d{4}-\d{2}-\d{2}$/u.test(value) ||
    !Number.isFinite(Date.parse(value)) ||
    new Date(value).toISOString().slice(0, 10) !== value
  )
    throw new Error("Invalid Arc date");
  return value;
};
const unique = <T>(values: T[]): T[] => [...new Set(values)];
const single = <T>(values: T[]): T | undefined => {
  const found: T[] = unique(values);
  if (found.length > 1) throw new Error("Conflicting Arc evidence");
  return found[0];
};
const group = <T>(rows: T[], key: (row: T) => string): Map<string, T[]> => {
  const result: Map<string, T[]> = new Map();
  rows.forEach((row) => {
    const k: string = key(row);
    result.set(k, [...(result.get(k) ?? []), row]);
  });
  return result;
};
const people = (r: Record<string, unknown>): ArcPeople => ({
  horse: id(r.source_horse_id, "horse"),
  jockey: id(r.source_jockey_id, "jockey"),
  trainer: id(r.source_trainer_id, "trainer"),
  owner: id(r.source_owner_id, "owner"),
});
const bounded = (rows: Record<string, unknown>[]): Record<string, unknown>[] => {
  if (rows.length > CAP) throw new Error("Arc input exceeds bounded read");
  return rows;
};
export const buildArcQualifiedQueries = (
  input: Pick<ArcLoadInput, "config" | "filters">,
): [string, string, string] => {
  const f: ArcFilters = input.filters;
  const ns: string = input.config.R2_SQL_NAMESPACE;
  if (
    f.event !== "arc" ||
    f.source !== "jra" ||
    f.date !== "20261004" ||
    f.keibajoCode !== "A8" ||
    f.raceBango !== "05" ||
    !/^[a-zA-Z_][a-zA-Z0-9_]*$/u.test(ns)
  )
    throw new Error("Expected exact Arc target and safe namespace");
  const mapping: string = `${ns}.oversea_runner_source_id`;
  const fields: string =
    "m.source, m.race_source, m.kaisai_nen, m.kaisai_tsukihi, m.keibajo_code, m.race_bango, m.umaban, m.source_horse_id, m.source_jockey_id, m.source_trainer_id, m.source_owner_id, m.gate_number";
  return [
    `SELECT DISTINCT ${fields} FROM ${mapping} m WHERE m.source = 'netkeiba' AND m.race_source = 'jra' AND m.kaisai_nen = '2026' AND m.kaisai_tsukihi = '1004' AND m.keibajo_code = 'A8' AND m.race_bango = '05' ORDER BY m.umaban LIMIT 5001`,
    `SELECT DISTINCT source, person_kind, source_person_id, source_race_id, source_horse_id, race_date, race_name, venue, surface, finish_position, finish_position_text FROM ${ns}.oversea_person_race_history WHERE source = 'netkeiba' AND person_kind IN ('jockey','trainer','owner') AND ${arcTitleSql("race_name")} AND race_date >= DATE '1996-10-04' AND race_date < DATE '2026-10-04' ORDER BY race_date, source_horse_id, person_kind, source_person_id LIMIT 5001`,
    `SELECT DISTINCT ${fields} FROM ${mapping} m WHERE m.source = 'netkeiba' AND m.race_source = 'jra' AND concat(m.kaisai_nen,m.kaisai_tsukihi) >= '19961004' AND concat(m.kaisai_nen,m.kaisai_tsukihi) < '20261004' AND EXISTS (SELECT 1 FROM ${ns}.jvd_ra r WHERE r.kaisai_nen = m.kaisai_nen AND r.kaisai_tsukihi = m.kaisai_tsukihi AND r.keibajo_code = m.keibajo_code AND r.race_bango = m.race_bango AND ${arcTitleSql("r.kyosomei_hondai")}) ORDER BY m.kaisai_nen, m.kaisai_tsukihi, m.umaban LIMIT 5001`,
  ];
};
const targets = (rows: Record<string, unknown>[]): Target[] => {
  const filtered: Record<string, unknown>[] = bounded(rows).filter(
    (r) =>
      r.source === "netkeiba" &&
      r.race_source === "jra" &&
      r.kaisai_nen === "2026" &&
      r.kaisai_tsukihi === "1004" &&
      r.keibajo_code === "A8" &&
      r.race_bango === "05",
  );
  const grouped: Map<string, Record<string, unknown>[]> = group(filtered, (r) => text(r.umaban));
  const result: Target[] = [...grouped].map(([umaban, items]) => {
    if (!/^(0[1-9]|1[0-6])$/u.test(umaban)) throw new Error("Invalid Arc target number");
    const assigned: ArcPeople = { horse: null, jockey: null, trainer: null, owner: null };
    ROLES.forEach((role) => {
      assigned[role] = single(items.map((r) => people(r)[role])) ?? null;
    });
    return {
      umaban,
      people: assigned,
      gate: single(items.map((r) => gate(r.gate_number))) ?? null,
    };
  });
  if (
    new Set(result.map((r) => r.people.horse).filter((h) => h !== null)).size !==
    result.filter((r) => r.people.horse !== null).length
  )
    throw new Error("Ambiguous current horse");
  return result.sort(compareTarget);
};
const compareTarget = (a: Target, b: Target): number => a.umaban.localeCompare(b.umaban);
const outcome = (r: Record<string, unknown>): ArcEvidence["outcome"] => {
  const status: string = text(r.finish_position_text);
  const positive: number | null =
    typeof r.finish_position === "number" &&
    Number.isInteger(r.finish_position) &&
    r.finish_position > 0
      ? r.finish_position
      : null;
  if (["取", "中", "失"].includes(status)) {
    if (positive !== null) throw new Error("Conflicting Arc outcome");
    return status === "取" ? "withdrawn" : 0;
  }
  return positive ?? "unknown";
};
const mappingDate = (r: Record<string, unknown>): string =>
  date(
    `${text(r.kaisai_nen)}-${text(r.kaisai_tsukihi).slice(0, 2)}-${text(r.kaisai_tsukihi).slice(2)}`,
  );
const historicalGates = (rows: Record<string, unknown>[]): Map<string, number | null> => {
  const qualified: Record<string, unknown>[] = bounded(rows).filter(
    (r) =>
      r.source === "netkeiba" &&
      r.race_source === "jra" &&
      id(r.source_horse_id, "horse") !== null &&
      mappingDate(r) >= SINCE &&
      mappingDate(r) < BEFORE,
  );
  const keys: Map<string, Record<string, unknown>[]> = group(
    qualified,
    (r) => `${mappingDate(r)}:${text(r.source_horse_id)}`,
  );
  const slots: Map<string, Record<string, unknown>[]> = group(qualified, (r) =>
    [mappingDate(r), r.keibajo_code, r.race_bango, r.umaban].join(":"),
  );
  const ambiguous: Set<string> = new Set(
    [...slots.values()]
      .filter((items) => unique(items.map((r) => r.source_horse_id)).length > 1)
      .flatMap((items) => items.map((r) => `${mappingDate(r)}:${text(r.source_horse_id)}`)),
  );
  return new Map(
    [...keys].map(([key, items]) => {
      const assignments: string[] = unique(
        items.map((r) => `${text(r.keibajo_code)}:${text(r.race_bango)}:${text(r.umaban)}`),
      );
      const gates: (number | null)[] = unique(items.map((r) => gate(r.gate_number)));
      return [
        key,
        !ambiguous.has(key) && assignments.length === 1 && gates.length === 1
          ? (gates[0] ?? null)
          : null,
      ];
    }),
  );
};
const evidence = (
  rows: Record<string, unknown>[],
  gates: Map<string, number | null>,
): ArcEvidence[] => {
  const grouped: Map<string, Record<string, unknown>[]> = group(
    rows.filter((r) => id(r.source_horse_id, "horse") !== null),
    (r) => `${date(r.race_date)}:${text(r.source_horse_id)}`,
  );
  return [...grouped]
    .map(([key, items]) => {
      const peopleIds: ArcPeople = {
        horse: key.slice(11),
        jockey: null,
        trainer: null,
        owner: null,
      };
      (["jockey", "trainer", "owner"] satisfies (keyof ArcPeople)[]).forEach((role) => {
        peopleIds[role] =
          single(
            items
              .filter((r) => r.person_kind === role)
              .map((r) => id(r.source_person_id, role))
              .filter((v) => v !== null),
          ) ?? null;
      });
      const outcomes: ArcEvidence["outcome"][] = items.map(outcome);
      return {
        key,
        date: key.slice(0, 10),
        horse: key.slice(11),
        people: peopleIds,
        venue: single(items.map((r) => text(r.venue))) ?? "",
        surface: single(items.map((r) => text(r.surface))) ?? "",
        outcome: single(outcomes) ?? "unknown",
        gate: gates.get(key) ?? null,
        sourceRaceIds: unique(items.map((r) => text(r.source_race_id))).sort(),
      };
    })
    .sort(compareEvidence);
};
const compareEvidence = (a: ArcEvidence, b: ArcEvidence): number => a.key.localeCompare(b.key);
const countRow = (input: {
  target: Target;
  cohort: Cohort;
  evidence: ArcEvidence[];
}): ArcCountRow => {
  const { target: t, cohort: c } = input;
  const known: boolean =
    c.roles.every((role) => t.people[role] !== null) && (!c.gate || t.gate !== null);
  const candidates: ArcEvidence[] = known
    ? input.evidence.filter(
        (e) =>
          e.date >= c.since &&
          c.roles.every((role) => t.people[role] === e.people[role]) &&
          (!c.venue || (e.venue === "パリロンシャン" && e.surface === "芝")),
      )
    : [];
  const matched: ArcEvidence[] = candidates.filter((e) => !c.gate || e.gate === t.gate);
  const starts: ArcEvidence[] = matched.filter((e) => typeof e.outcome === "number");
  const status: ArcCountRow["status"] = starts.length > 0 ? "observed" : "unobserved";
  return {
    umaban: t.umaban,
    kind: c.kind,
    source: "netkeiba",
    people: t.people,
    scope: c.venue ? "arc-parislongchamp-turf" : "arc-series",
    since: c.since,
    before: BEFORE,
    gate: c.gate ? t.gate : null,
    status: known ? status : "missing-identity",
    starts: starts.length,
    wins: starts.filter((e) => e.outcome === 1).length,
    top2: starts.filter((e) => typeof e.outcome === "number" && e.outcome > 0 && e.outcome <= 2)
      .length,
    top3: starts.filter((e) => typeof e.outcome === "number" && e.outcome > 0 && e.outcome <= 3)
      .length,
    evidenceKeys: starts.map((e) => e.key),
    unknownKeys: matched.filter((e) => e.outcome === "unknown").map((e) => e.key),
    withdrawnKeys: matched.filter((e) => e.outcome === "withdrawn").map((e) => e.key),
    unverifiedGateKeys: candidates.filter((e) => c.gate && e.gate === null).map((e) => e.key),
  };
};
// Sorted, deduplicated raw dependencies retain changes to rejected/unavailable inputs too.
export const arcQualifiedDependencyTuple = (
  inputs: Record<string, unknown>[][],
): readonly unknown[] => [
  "arc-qualified-people-v1",
  COHORTS,
  TITLES,
  BEFORE,
  ...inputs.map((rows) =>
    unique(
      rows.map((r) =>
        JSON.stringify(
          Object.keys(r)
            .sort()
            .map((k) => [k, r[k]]),
        ),
      ),
    ).sort(),
  ),
];
export const loadArcQualifiedPeople = async (input: ArcLoadInput): Promise<ArcQualifiedResult> => {
  const queries: [string, string, string] = buildArcQualifiedQueries(input);
  const current: Record<string, unknown>[] = bounded(await input.execute(queries[0]));
  const raw: Record<string, unknown>[] = bounded(await input.execute(queries[1]));
  const mappings: Record<string, unknown>[] = bounded(await input.execute(queries[2]));
  const qualified: Record<string, unknown>[] = raw.filter(
    (r) =>
      r.source === "netkeiba" &&
      ["jockey", "trainer", "owner"].includes(text(r.person_kind)) &&
      TITLES.includes(text(r.race_name).trim()) &&
      date(r.race_date) >= SINCE &&
      date(r.race_date) < BEFORE,
  );
  const ledger: ArcEvidence[] = evidence(qualified, historicalGates(mappings));
  return {
    rows: targets(current).flatMap((target) =>
      COHORTS.map((cohort) => countRow({ target, cohort, evidence: ledger })),
    ),
    evidence: ledger,
    coverage: {
      complete: false,
      rawRows: raw.length,
      invalidHorseRows: qualified.filter((r) => id(r.source_horse_id, "horse") === null).length,
    },
    dependencyTuple: arcQualifiedDependencyTuple([current, raw, mappings]),
  };
};
