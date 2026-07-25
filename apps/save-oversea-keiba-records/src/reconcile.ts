// This file runs with Bun.
//
// Cross-source reconciler for overseas race runners.
//
// Precedence rule (documented for wiring consumers):
// - JRA wins for every descriptive field: horse name, gate, sex, age, coat colour,
//   weight carried, jockey name, trainer name, trainer country, owner, win odds,
//   popularity, form record, sire, dam, and damsire.
// - The secondary source contributes opaque entity ids only (horse id, jockey id,
//   trainer id) plus a stable affiliation label. Its truncated names must never
//   override JRA values; name mismatches are informational notes, not errors.
// - Join key is horse number (umaban), never array index. Secondary pages may be
//   ordered by gate rather than horse number.
// - A JRA-only runner is kept in the merged output with null secondary ids.
// - A secondary-only runner is reported but is not invented into the merged output.

export type NameField = "horseName" | "jockeyName" | "trainerName";

export interface JraReconcileRunner {
  readonly horseNumber: number;
  readonly gate: number;
  readonly horseName: string;
  readonly sex: string;
  readonly age: number;
  readonly coatColour: string;
  readonly weightCarriedKg: number;
  readonly jockeyAbbrev: string;
  readonly trainerAbbrev: string;
  readonly trainerCountry: string;
  readonly owner: string;
  readonly winOdds: number | null;
  readonly popularity: number | null;
  readonly formRecord: string;
  readonly sire: string;
  readonly dam: string;
  readonly damsire: string;
}

export interface SecondaryReconcileRunner {
  readonly horseNumber: number;
  readonly gate: number;
  readonly horseName: string;
  readonly jockeyName: string;
  readonly trainerName: string;
  readonly horseId: string | null;
  readonly jockeyId: string | null;
  readonly trainerId: string | null;
  readonly affiliationLabel: string;
}

export interface MergedRunner {
  readonly horseNumber: number;
  readonly gate: number;
  readonly horseName: string;
  readonly sex: string;
  readonly age: number;
  readonly coatColour: string;
  readonly weightCarriedKg: number;
  readonly jockeyAbbrev: string;
  readonly trainerAbbrev: string;
  readonly trainerCountry: string;
  readonly owner: string;
  readonly winOdds: number | null;
  readonly popularity: number | null;
  readonly formRecord: string;
  readonly sire: string;
  readonly dam: string;
  readonly damsire: string;
  readonly secondaryHorseId: string | null;
  readonly secondaryJockeyId: string | null;
  readonly secondaryTrainerId: string | null;
  readonly secondaryAffiliationLabel: string | null;
  readonly hasSecondaryMatch: boolean;
}

export interface NameDifferenceNote {
  readonly field: NameField;
  readonly jraValue: string;
  readonly secondaryValue: string;
}

export interface RunnerReconciliationEntry {
  readonly horseNumber: number;
  readonly hasSecondaryMatch: boolean;
  readonly gateDisagrees: boolean;
  readonly jraGate: number;
  readonly secondaryGate: number | null;
  readonly nameDifferences: readonly NameDifferenceNote[];
}

export interface GateDisagreementWarning {
  readonly horseNumber: number;
  readonly jraGate: number;
  readonly secondaryGate: number;
  readonly message: string;
}

export interface ReconciliationReport {
  readonly runners: readonly RunnerReconciliationEntry[];
  readonly unmatchedJraHorseNumbers: readonly number[];
  readonly unmatchedSecondaryRunners: readonly SecondaryReconcileRunner[];
  readonly gateDisagreements: readonly GateDisagreementWarning[];
  readonly hasGateDisagreement: boolean;
}

export interface ReconcileInput {
  readonly jraRunners: readonly JraReconcileRunner[];
  readonly secondaryRunners: readonly SecondaryReconcileRunner[];
}

export interface ReconcileResult {
  readonly mergedRunners: readonly MergedRunner[];
  readonly report: ReconciliationReport;
}

interface MergeRunnerInput {
  readonly jraRunner: JraReconcileRunner;
  readonly secondary: SecondaryReconcileRunner | undefined;
}

interface BuildEntryInput {
  readonly jraRunner: JraReconcileRunner;
  readonly secondary: SecondaryReconcileRunner | undefined;
}

const GATE_DISAGREEMENT_PREFIX: string =
  "WARNING: gate disagreement between JRA and secondary source for horse number";

const buildNameDifferences = (
  jraRunner: JraReconcileRunner,
  secondary: SecondaryReconcileRunner,
): readonly NameDifferenceNote[] => {
  const candidates: readonly NameDifferenceNote[] = [
    {
      field: "horseName",
      jraValue: jraRunner.horseName,
      secondaryValue: secondary.horseName,
    },
    {
      field: "jockeyName",
      jraValue: jraRunner.jockeyAbbrev,
      secondaryValue: secondary.jockeyName,
    },
    {
      field: "trainerName",
      jraValue: jraRunner.trainerAbbrev,
      secondaryValue: secondary.trainerName,
    },
  ];
  return candidates.filter(
    (note: NameDifferenceNote): boolean => note.jraValue !== note.secondaryValue,
  );
};

const mergeRunner = ({ jraRunner, secondary }: MergeRunnerInput): MergedRunner => {
  if (secondary === undefined) {
    return {
      horseNumber: jraRunner.horseNumber,
      gate: jraRunner.gate,
      horseName: jraRunner.horseName,
      sex: jraRunner.sex,
      age: jraRunner.age,
      coatColour: jraRunner.coatColour,
      weightCarriedKg: jraRunner.weightCarriedKg,
      jockeyAbbrev: jraRunner.jockeyAbbrev,
      trainerAbbrev: jraRunner.trainerAbbrev,
      trainerCountry: jraRunner.trainerCountry,
      owner: jraRunner.owner,
      winOdds: jraRunner.winOdds,
      popularity: jraRunner.popularity,
      formRecord: jraRunner.formRecord,
      sire: jraRunner.sire,
      dam: jraRunner.dam,
      damsire: jraRunner.damsire,
      secondaryHorseId: null,
      secondaryJockeyId: null,
      secondaryTrainerId: null,
      secondaryAffiliationLabel: null,
      hasSecondaryMatch: false,
    };
  }

  return {
    horseNumber: jraRunner.horseNumber,
    gate: jraRunner.gate,
    horseName: jraRunner.horseName,
    sex: jraRunner.sex,
    age: jraRunner.age,
    coatColour: jraRunner.coatColour,
    weightCarriedKg: jraRunner.weightCarriedKg,
    jockeyAbbrev: jraRunner.jockeyAbbrev,
    trainerAbbrev: jraRunner.trainerAbbrev,
    trainerCountry: jraRunner.trainerCountry,
    owner: jraRunner.owner,
    winOdds: jraRunner.winOdds,
    popularity: jraRunner.popularity,
    formRecord: jraRunner.formRecord,
    sire: jraRunner.sire,
    dam: jraRunner.dam,
    damsire: jraRunner.damsire,
    secondaryHorseId: secondary.horseId,
    secondaryJockeyId: secondary.jockeyId,
    secondaryTrainerId: secondary.trainerId,
    secondaryAffiliationLabel: secondary.affiliationLabel,
    hasSecondaryMatch: true,
  };
};

const buildRunnerEntry = ({ jraRunner, secondary }: BuildEntryInput): RunnerReconciliationEntry => {
  if (secondary === undefined) {
    return {
      horseNumber: jraRunner.horseNumber,
      hasSecondaryMatch: false,
      gateDisagrees: false,
      jraGate: jraRunner.gate,
      secondaryGate: null,
      nameDifferences: [],
    };
  }

  const gateDisagrees: boolean = jraRunner.gate !== secondary.gate;
  return {
    horseNumber: jraRunner.horseNumber,
    hasSecondaryMatch: true,
    gateDisagrees,
    jraGate: jraRunner.gate,
    secondaryGate: secondary.gate,
    nameDifferences: buildNameDifferences(jraRunner, secondary),
  };
};

const toGateDisagreementWarning = (
  entry: RunnerReconciliationEntry,
): GateDisagreementWarning | null => {
  if (!entry.gateDisagrees || entry.secondaryGate === null) {
    return null;
  }
  return {
    horseNumber: entry.horseNumber,
    jraGate: entry.jraGate,
    secondaryGate: entry.secondaryGate,
    message: `${GATE_DISAGREEMENT_PREFIX} ${String(entry.horseNumber)}: JRA gate ${String(entry.jraGate)} vs secondary gate ${String(entry.secondaryGate)}.`,
  };
};

const isGateDisagreementWarning = (
  warning: GateDisagreementWarning | null,
): warning is GateDisagreementWarning => warning !== null;

export const reconcileRunners = ({
  jraRunners,
  secondaryRunners,
}: ReconcileInput): ReconcileResult => {
  const secondaryByHorseNumber: ReadonlyMap<number, SecondaryReconcileRunner> = new Map(
    secondaryRunners.map((runner: SecondaryReconcileRunner): [number, SecondaryReconcileRunner] => [
      runner.horseNumber,
      runner,
    ]),
  );
  const jraHorseNumbers: ReadonlySet<number> = new Set(
    jraRunners.map((runner: JraReconcileRunner): number => runner.horseNumber),
  );

  const runners: readonly RunnerReconciliationEntry[] = jraRunners.map(
    (jraRunner: JraReconcileRunner): RunnerReconciliationEntry =>
      buildRunnerEntry({
        jraRunner,
        secondary: secondaryByHorseNumber.get(jraRunner.horseNumber),
      }),
  );

  const mergedRunners: readonly MergedRunner[] = jraRunners.map(
    (jraRunner: JraReconcileRunner): MergedRunner =>
      mergeRunner({
        jraRunner,
        secondary: secondaryByHorseNumber.get(jraRunner.horseNumber),
      }),
  );

  const unmatchedJraHorseNumbers: readonly number[] = runners
    .filter((entry: RunnerReconciliationEntry): boolean => !entry.hasSecondaryMatch)
    .map((entry: RunnerReconciliationEntry): number => entry.horseNumber);

  const unmatchedSecondaryRunners: readonly SecondaryReconcileRunner[] = secondaryRunners.filter(
    (runner: SecondaryReconcileRunner): boolean => !jraHorseNumbers.has(runner.horseNumber),
  );

  const gateDisagreements: readonly GateDisagreementWarning[] = runners
    .map(toGateDisagreementWarning)
    .filter(isGateDisagreementWarning);

  return {
    mergedRunners,
    report: {
      runners,
      unmatchedJraHorseNumbers,
      unmatchedSecondaryRunners,
      gateDisagreements,
      hasGateDisagreement: gateDisagreements.length > 0,
    },
  };
};
