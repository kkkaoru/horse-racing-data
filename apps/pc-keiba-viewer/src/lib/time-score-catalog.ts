// Run with bun. Glue between the Catalog readers and the pure time-score
// pipeline: it applies the incumbent SQL's `current_horses` invariants (an
// all-zero registration number is usable only through the overseas mapping)
// and emits one row per surviving horse, ordered like the SQL.
import { buildTimeScoreRow, buildWeightedProfile, orderTimeScoreRows } from "./time-score-pipeline";
import type {
  TimeScoreHistoryRow,
  TimeScoreRow,
  TimeScoreTargetProfile,
} from "./time-score-pipeline";

export interface CatalogTimeScoreHorse {
  horseNumber: string;
  horseNumberSort: number | null;
  horseName: string;
  // `null` when the registration number is all zeros and the overseas mapping
  // did not resolve it; the SQL drops those horses entirely.
  historyHorseId: string | null;
  currentAge: number | null;
}

export interface CatalogTimeScoreInput {
  raceDate: string;
  keibajoCode: string;
  targetDistance: number | null;
  target: TimeScoreTargetProfile;
  horses: readonly CatalogTimeScoreHorse[];
  historyByHorseId: ReadonlyMap<string, readonly TimeScoreHistoryRow[]>;
}

export const composeCatalogTimeScoreRows = (input: CatalogTimeScoreInput): TimeScoreRow[] => {
  const sorts: Map<string, number | null> = new Map();
  const rows: TimeScoreRow[] = [];
  for (const horse of input.horses) {
    if (horse.historyHorseId === null || horse.historyHorseId === "") continue;
    const history: readonly TimeScoreHistoryRow[] =
      input.historyByHorseId.get(horse.historyHorseId) ?? [];
    rows.push(
      buildTimeScoreRow(
        {
          horseNumber: horse.horseNumber,
          horseNumberSort: horse.horseNumberSort,
          horseName: horse.horseName,
          currentAge: horse.currentAge,
        },
        buildWeightedProfile(
          history,
          horse,
          input.raceDate,
          input.targetDistance,
          input.keibajoCode,
        ),
        input.target,
      ),
    );
    sorts.set(horse.horseNumber, horse.horseNumberSort);
  }
  return orderTimeScoreRows(rows, sorts);
};
