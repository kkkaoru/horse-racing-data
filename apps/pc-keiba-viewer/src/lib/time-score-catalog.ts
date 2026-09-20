import type { RaceHistoryRow } from "./race-history-catalog";
import { buildTimeScoreRow, buildWeightedProfile, orderTimeScoreRows } from "./time-score-pipeline";
import type {
  TimeScoreHistoryRow,
  TimeScoreRow,
  TimeScoreTargetProfile,
} from "./time-score-pipeline";
// Run with bun. Glue between the Catalog readers and the pure time-score
// pipeline: it applies the incumbent SQL's `current_horses` invariants (an
// all-zero registration number is usable only through the overseas mapping)
// and emits one row per surviving horse, ordered like the SQL.
import { normaliseHorseNumber, parseDigitsOnly, parseRaceTimeTenths } from "./time-score-scoring";

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

// Converts Catalog history rows into the pipeline's row shape. `sohaTime`,
// `kohan3f`, `futanJuryo` and `timeSa` are tenths-scaled text; `kyori` and
// `bataiju` are plain integers.
export const groupHistoryByHorseId = (
  rows: readonly RaceHistoryRow[],
): Map<string, TimeScoreHistoryRow[]> => {
  const grouped: Map<string, TimeScoreHistoryRow[]> = new Map();
  for (const row of rows) {
    const horseId: string = row.kettoTorokuBango.trim();
    if (horseId === "") continue;
    const bucket: TimeScoreHistoryRow[] = grouped.get(horseId) ?? [];
    bucket.push({
      horseNumber: normaliseHorseNumber(row.umaban),
      raceDate: `${row.kaisaiNen}${row.kaisaiTsukihi}`,
      keibajoCode: row.keibajoCode.trim() === "" ? null : row.keibajoCode,
      distance: parseDigitsOnly(row.kyori),
      raceTime: parseRaceTimeTenths(row.sohaTime),
      last3f: parseRaceTimeTenths(row.kohan3f),
      bodyWeight: parseDigitsOnly(row.bataiju),
      carriedWeight: parseRaceTimeTenths(row.futanJuryo),
      margin: parseRaceTimeTenths(row.timeSa),
    });
    grouped.set(horseId, bucket);
  }
  return grouped;
};
