// Run with bun (bunx vitest).
import type { PartnershipKind, PartnershipScope } from "./heatmap-partnership";
import type { PartnershipCohortCache, PartnershipCohortRequest } from "./heatmap-partnership-cache";
import { buildPartnershipEntriesQuery } from "./heatmap-partnership-sql";

export interface PartnershipStatsRow {
  kind: PartnershipKind;
  umaban: number;
  name: string;
  starts: number;
  wins: number;
  places: number;
  shows: number;
}

export interface PartnershipStatsRequest {
  cache: PartnershipCohortCache;
  cohort: PartnershipCohortRequest;
  raceBango: string;
}

interface Target {
  horseId: string | null;
  jockeyId: string | null;
  trainerId: string | null;
  horseName: string;
  jockeyName: string;
  trainerName: string;
  umaban: number;
}

interface Counts {
  starts: number;
  wins: number;
  places: number;
  shows: number;
}

const KINDS: readonly PartnershipKind[] = ["horseJockey", "jockeyVenue", "jockeyTrainerVenue"];
const ZERO_COUNTS: Counts = { starts: 0, wins: 0, places: 0, shows: 0 };

const text = (value: unknown): string | null =>
  typeof value === "string" && value.trim() !== "" ? value.trim() : null;
const count = (value: unknown): number => {
  if (typeof value !== "number" && typeof value !== "string" && typeof value !== "bigint")
    throw new Error("Partnership count is malformed");
  const numeric: number = Number(value);
  if (value === "" || !Number.isSafeInteger(numeric) || numeric < 0)
    throw new Error("Partnership count is malformed");
  return numeric;
};
const counts = (row: Record<string, unknown>): Counts => {
  const result: Counts = {
    starts: count(row.starts),
    wins: count(row.wins),
    places: count(row.places),
    shows: count(row.shows),
  };
  if (result.wins > result.places || result.places > result.shows || result.shows > result.starts)
    throw new Error("Partnership counts are inconsistent");
  return result;
};
const target = (row: Record<string, unknown>): Target => ({
  horseId: text(row.horse_id),
  jockeyId: text(row.jockey_id),
  trainerId: text(row.trainer_id),
  horseName: text(row.horse_name) ?? "不明",
  jockeyName: text(row.jockey_name) ?? "不明",
  trainerName: text(row.trainer_name) ?? "不明",
  umaban: count(row.umaban),
});
const partnerId = (entry: Target, kind: PartnershipKind): string | null => {
  if (kind === "jockeyVenue") return entry.jockeyId;
  return kind === "horseJockey" ? entry.horseId : entry.trainerId;
};
const displayName = (entry: Target, kind: PartnershipKind): string => {
  if (kind === "jockeyVenue") return entry.jockeyName;
  return kind === "horseJockey"
    ? `${entry.horseName} × ${entry.jockeyName}`
    : `${entry.jockeyName} × ${entry.trainerName}`;
};
const mappedRows = (input: {
  entries: Target[];
  kind: PartnershipKind;
  values: Record<string, unknown>[];
}): PartnershipStatsRow[] => {
  const index: Map<string, Counts> = new Map(
    input.values.map((row) => [
      JSON.stringify([text(row.partner_id), text(row.jockey_id)]),
      counts(row),
    ]),
  );
  return input.entries.flatMap((entry) => {
    const partner: string | null = partnerId(entry, input.kind);
    if (entry.umaban <= 0 || partner === null || entry.jockeyId === null) return [];
    const rate: Counts = index.get(JSON.stringify([partner, entry.jockeyId])) ?? ZERO_COUNTS;
    return [
      { kind: input.kind, umaban: entry.umaban, name: displayName(entry, input.kind), ...rate },
    ];
  });
};

export const loadPartnershipStats = async (
  input: PartnershipStatsRequest,
): Promise<PartnershipStatsRow[] | null> => {
  const entries: Target[] = (
    await input.cohort.execute(
      buildPartnershipEntriesQuery({
        config: input.cohort.query.config,
        scope: input.cohort.query.scope,
        raceBango: input.raceBango,
      }),
    )
  ).map(target);
  const horseIds: string[] = entries.flatMap((entry) =>
    entry.horseId === null ? [] : [entry.horseId],
  );
  const groups: (PartnershipStatsRow[] | null)[] = [];
  // Bound R2 SQL concurrency to the two queries inside each shared cohort.
  for (const kind of KINDS) {
    if (kind === "horseJockey" && horseIds.length === 0) continue;
    const scope: PartnershipScope = { ...input.cohort.query.scope, kind };
    const cohort = await input.cache.load({
      ...input.cohort,
      query: { ...input.cohort.query, scope, horseIds },
    });
    groups.push(cohort === null ? null : mappedRows({ entries, kind, values: cohort.values }));
  }
  return groups.some((group) => group === null) ? null : groups.flatMap((group) => group ?? []);
};
