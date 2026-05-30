// Run with bun. Server-only fallback that reads the latest 馬体重 snapshot from
// the legacy sync-realtime-data D1 when the HorseWeightDO (the primary fast
// path) does not have a snapshot yet. The DO is in-memory + storage-backed but
// can be empty just after deploys or when the worker hibernates the DO before
// the first weight push. D1 always has the data; this helper closes the gap.

import "server-only";

interface HorseWeightSnapshotRow {
  horse_number: string;
  horse_name: string | null;
  weight: number | null;
  change_sign: string | null;
  change_amount: number | null;
  fetched_at: string;
}

export interface HorseWeightEntry {
  changeAmount: number | null;
  changeSign: string | null;
  horseName: string | null;
  horseNumber: string;
  weight: number | null;
}

export interface HorseWeightSnapshot {
  fetchedAt: string;
  horses: HorseWeightEntry[];
}

export interface FetchHorseWeightsFromD1Params {
  db: PcKeibaD1Database;
  raceKey: string;
}

const SELECT_HORSE_WEIGHTS_SQL =
  "select horse_number, horse_name, weight, change_sign, change_amount, fetched_at from horse_weight_snapshots where race_key = ?";

const DECIMAL_RADIX = 10;

const mapRow = (row: HorseWeightSnapshotRow): HorseWeightEntry => ({
  changeAmount: row.change_amount,
  changeSign: row.change_sign,
  horseName: row.horse_name,
  horseNumber: row.horse_number,
  weight: row.weight,
});

const compareByHorseNumber = (a: HorseWeightEntry, b: HorseWeightEntry): number =>
  Number.parseInt(a.horseNumber, DECIMAL_RADIX) - Number.parseInt(b.horseNumber, DECIMAL_RADIX);

export const fetchHorseWeightsFromD1 = async (
  params: FetchHorseWeightsFromD1Params,
): Promise<HorseWeightSnapshot | null> => {
  const result = await params.db
    .prepare(SELECT_HORSE_WEIGHTS_SQL)
    .bind(params.raceKey)
    .all<HorseWeightSnapshotRow>();
  const rows = result.results;
  const [first] = rows;
  if (first === undefined) return null;
  const horses = rows.map(mapRow).toSorted(compareByHorseNumber);
  return { fetchedAt: first.fetched_at, horses };
};
