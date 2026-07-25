// This file runs with Bun.
//
// Numeric-only master backfill planner (option 1).
// Proposes INSERT-only candidates when a secondary-source entity id already has a
// valid JV primary-key shape (pure ASCII digits, exact width) and that code is
// absent from the local master. Never mints synthetic/alphanumeric keys. Never
// proposes placeholders. Owner master (jvd_bn) is intentionally skipped.

import {
  HORSE_CODE_PLACEHOLDER,
  JOCKEY_CODE_PLACEHOLDER,
  TRAINER_CODE_PLACEHOLDER,
  isValidHorseCode,
  isValidJockeyCode,
  isValidTrainerCode,
  type MasterLookupPort,
} from "./entity-resolver";
import type { MergedRunner } from "./reconcile";

export type MasterBackfillEntityKind = "horse" | "jockey" | "trainer";

export interface MasterBackfillCandidate {
  readonly kind: MasterBackfillEntityKind;
  readonly code: string;
  readonly displayName: string;
  /** JRA card fields used to build a full fixed-width master row. */
  readonly horseName: string;
  readonly sex: string;
  readonly coatColour: string;
  readonly sire: string;
  readonly dam: string;
  readonly damsire: string;
  readonly jockeyAbbrev: string;
  readonly trainerAbbrev: string;
  readonly owner: string;
  readonly trainerCode: string | null;
  readonly raceDate: string;
}

export interface PlanMasterBackfillInput {
  readonly mergedRunners: readonly MergedRunner[];
  readonly lookup: MasterLookupPort;
  /** Compact yyyymmdd from the race date, used as data_sakusei_nengappi. */
  readonly raceDateCompact: string;
}

interface CandidateKey {
  readonly kind: MasterBackfillEntityKind;
  readonly code: string;
}

const candidateKey = ({ kind, code }: CandidateKey): string => `${kind}:${code}`;

const isNonPlaceholderHorse = (code: string): boolean =>
  isValidHorseCode(code) && code !== HORSE_CODE_PLACEHOLDER;

const isNonPlaceholderJockey = (code: string): boolean =>
  isValidJockeyCode(code) && code !== JOCKEY_CODE_PLACEHOLDER;

const isNonPlaceholderTrainer = (code: string): boolean =>
  isValidTrainerCode(code) && code !== TRAINER_CODE_PLACEHOLDER;

const toCandidate = (
  kind: MasterBackfillEntityKind,
  code: string,
  displayName: string,
  runner: MergedRunner,
  raceDateCompact: string,
): MasterBackfillCandidate => ({
  kind,
  code,
  displayName,
  horseName: runner.horseName,
  sex: runner.sex,
  coatColour: runner.coatColour,
  sire: runner.sire,
  dam: runner.dam,
  damsire: runner.damsire,
  jockeyAbbrev: runner.jockeyAbbrev,
  trainerAbbrev: runner.trainerAbbrev,
  owner: runner.owner,
  trainerCode: runner.secondaryTrainerId,
  raceDate: raceDateCompact,
});

/**
 * Plan insert-only master backfill candidates from reconciled runners.
 * Shape-valid + missing from local master only. Deduped by kind+code (first runner wins).
 */
export const planMasterBackfill = async ({
  mergedRunners,
  lookup,
  raceDateCompact,
}: PlanMasterBackfillInput): Promise<readonly MasterBackfillCandidate[]> => {
  const seen: Set<string> = new Set();
  const candidates: MasterBackfillCandidate[] = [];

  for (const runner of mergedRunners) {
    const horseId: string | null = runner.secondaryHorseId;
    if (horseId !== null && isNonPlaceholderHorse(horseId)) {
      const key: string = candidateKey({ kind: "horse", code: horseId });
      if (!seen.has(key)) {
        seen.add(key);
        const record = await lookup.findHorse(horseId);
        if (!record.exists) {
          candidates.push(toCandidate("horse", horseId, runner.horseName, runner, raceDateCompact));
        }
      }
    }

    const jockeyId: string | null = runner.secondaryJockeyId;
    if (jockeyId !== null && isNonPlaceholderJockey(jockeyId)) {
      const key: string = candidateKey({ kind: "jockey", code: jockeyId });
      if (!seen.has(key)) {
        seen.add(key);
        const record = await lookup.findJockey(jockeyId);
        if (!record.exists) {
          candidates.push(
            toCandidate("jockey", jockeyId, runner.jockeyAbbrev, runner, raceDateCompact),
          );
        }
      }
    }

    const trainerId: string | null = runner.secondaryTrainerId;
    if (trainerId !== null && isNonPlaceholderTrainer(trainerId)) {
      const key: string = candidateKey({ kind: "trainer", code: trainerId });
      if (!seen.has(key)) {
        seen.add(key);
        const record = await lookup.findTrainer(trainerId);
        if (!record.exists) {
          candidates.push(
            toCandidate("trainer", trainerId, runner.trainerAbbrev, runner, raceDateCompact),
          );
        }
      }
    }
  }

  return candidates;
};

/**
 * Wrap a master lookup so planned insert candidates appear as existing with the
 * JRA card display name. Used so race-row entity resolution sees masters that
 * will be inserted before SE write on --apply.
 */
export const createBackfillAwareLookup = (
  lookup: MasterLookupPort,
  candidates: readonly MasterBackfillCandidate[],
): MasterLookupPort => {
  const horseNames: ReadonlyMap<string, string> = new Map(
    candidates
      .filter((c) => c.kind === "horse")
      .map((c): readonly [string, string] => [c.code, c.displayName]),
  );
  const jockeyNames: ReadonlyMap<string, string> = new Map(
    candidates
      .filter((c) => c.kind === "jockey")
      .map((c): readonly [string, string] => [c.code, c.displayName]),
  );
  const trainerNames: ReadonlyMap<string, string> = new Map(
    candidates
      .filter((c) => c.kind === "trainer")
      .map((c): readonly [string, string] => [c.code, c.displayName]),
  );

  return {
    findHorse: async (kettoTorokuBango: string) => {
      const existing = await lookup.findHorse(kettoTorokuBango);
      if (existing.exists) {
        return existing;
      }
      const name: string | undefined = horseNames.get(kettoTorokuBango);
      return name === undefined ? existing : { exists: true, canonicalName: name };
    },
    findJockey: async (kishuCode: string) => {
      const existing = await lookup.findJockey(kishuCode);
      if (existing.exists) {
        return existing;
      }
      const name: string | undefined = jockeyNames.get(kishuCode);
      return name === undefined ? existing : { exists: true, canonicalName: name };
    },
    findTrainer: async (chokyoshiCode: string) => {
      const existing = await lookup.findTrainer(chokyoshiCode);
      if (existing.exists) {
        return existing;
      }
      const name: string | undefined = trainerNames.get(chokyoshiCode);
      return name === undefined
        ? existing
        : { exists: true, canonicalName: name, tozaiShozokuCode: "4" };
    },
    findOwnerByName: (ownerName: string) => lookup.findOwnerByName(ownerName),
  };
};

export const formatMasterBackfillReport = (
  candidates: readonly MasterBackfillCandidate[],
): readonly string[] => {
  const header: string = "=== Master backfill (numeric-only) ===";
  if (candidates.length === 0) {
    return [header, "(none)"];
  }
  return [
    header,
    ...candidates.map(
      (c: MasterBackfillCandidate): string => `${c.kind} code=${c.code} name=${c.displayName}`,
    ),
  ];
};
