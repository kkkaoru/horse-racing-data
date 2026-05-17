// Run with bun. Computes race-internal "field_*" features (peer pressure /
// peer averages) from per-horse past-style aggregates. The Mac feature
// builder ships per-horse values only; this module derives anything that
// requires looking at other runners in the same race so the result still
// reflects same-day jockey changes or scratches.

export interface HorsePeerInputs {
  pastNigeRate: number | null;
  pastSenkouRate: number | null;
  pastSashiRate: number | null;
  pastOikomiRate: number | null;
  pastCorner1NormAvg5: number | null;
  speedIndexAvg5: number | null;
  speedIndexBest5: number | null;
  pastFirst3fAvg5: number | null;
  kohan3fAvg5: number | null;
  careerWinRate: number | null;
}

export interface FieldFeatures {
  field_nige_pressure: number | null;
  field_senkou_pressure: number | null;
  field_sashi_pressure: number | null;
  field_oikomi_pressure: number | null;
  field_pace_index: number | null;
  field_nige_candidate_count: number;
  field_max_past_corner_1_norm: number | null;
  field_min_past_corner_1_norm: number | null;
  field_spread_past_corner_1_norm: number | null;
  field_has_pure_nige_horse: boolean;
  field_avg_speed_index: number | null;
  field_top_speed_index: number | null;
  field_avg_past_first_3f: number | null;
  field_avg_past_kohan_3f: number | null;
  field_avg_career_win_rate: number | null;
}

export interface HorseFieldRow extends FieldFeatures {
  self_nige_rate_minus_field_avg: number | null;
  self_speed_index_vs_field_top: number | null;
}

const PACE_NIGE_WEIGHT = 2.0;
const PACE_SENKOU_WEIGHT = 1.0;
const PURE_NIGE_THRESHOLD = 0.7;
const NIGE_CANDIDATE_THRESHOLD = 0.4;

const sumExcluding = (values: ReadonlyArray<number | null>, selfIndex: number): number | null => {
  const filtered = values.filter((value, idx): value is number => value !== null && idx !== selfIndex);
  if (filtered.length === 0) return null;
  return filtered.reduce((acc, value) => acc + value, 0);
};

const averageExcluding = (values: ReadonlyArray<number | null>, selfIndex: number): number | null => {
  const filtered = values.filter((value, idx): value is number => value !== null && idx !== selfIndex);
  if (filtered.length === 0) return null;
  return filtered.reduce((acc, value) => acc + value, 0) / filtered.length;
};

const minOfNumbers = (values: ReadonlyArray<number>): number | null => {
  if (values.length === 0) return null;
  return values.reduce((acc, value) => (value < acc ? value : acc), values[0] ?? 0);
};

const maxOfNumbers = (values: ReadonlyArray<number>): number | null => {
  if (values.length === 0) return null;
  return values.reduce((acc, value) => (value > acc ? value : acc), values[0] ?? 0);
};

const countAbove = (values: ReadonlyArray<number | null>, threshold: number): number =>
  values.filter((value): value is number => value !== null && value > threshold).length;

const pickColumn = <K extends keyof HorsePeerInputs>(
  horses: ReadonlyArray<HorsePeerInputs>,
  key: K,
): ReadonlyArray<HorsePeerInputs[K]> => horses.map((horse) => horse[key]);

const pickNonNull = (values: ReadonlyArray<number | null>): ReadonlyArray<number> =>
  values.filter((value): value is number => value !== null);

const safeDivide = (numerator: number | null, denominator: number | null): number | null => {
  if (numerator === null || denominator === null || denominator === 0) return null;
  return numerator / denominator;
};

const buildFieldFeatures = (
  horses: ReadonlyArray<HorsePeerInputs>,
  selfIndex: number,
): FieldFeatures => {
  const nigeRates = pickColumn(horses, "pastNigeRate");
  const senkouRates = pickColumn(horses, "pastSenkouRate");
  const sashiRates = pickColumn(horses, "pastSashiRate");
  const oikomiRates = pickColumn(horses, "pastOikomiRate");
  const corner1NormPeers = pickNonNull(
    horses
      .filter((_, idx) => idx !== selfIndex)
      .map((horse) => horse.pastCorner1NormAvg5),
  );
  const speedAvg = pickColumn(horses, "speedIndexAvg5");
  const speedBest = pickColumn(horses, "speedIndexBest5");
  const first3f = pickColumn(horses, "pastFirst3fAvg5");
  const kohan3f = pickColumn(horses, "kohan3fAvg5");
  const careerWin = pickColumn(horses, "careerWinRate");
  const peerNigeRatesAll = nigeRates.filter((_, idx) => idx !== selfIndex);
  const fieldNige = sumExcluding(nigeRates, selfIndex);
  const fieldSenkou = sumExcluding(senkouRates, selfIndex);
  const fieldSashi = sumExcluding(sashiRates, selfIndex);
  const fieldOikomi = sumExcluding(oikomiRates, selfIndex);
  return {
    field_avg_career_win_rate: averageExcluding(careerWin, selfIndex),
    field_avg_past_first_3f: averageExcluding(first3f, selfIndex),
    field_avg_past_kohan_3f: averageExcluding(kohan3f, selfIndex),
    field_avg_speed_index: averageExcluding(speedAvg, selfIndex),
    field_has_pure_nige_horse: countAbove(peerNigeRatesAll, PURE_NIGE_THRESHOLD) > 0,
    field_max_past_corner_1_norm: maxOfNumbers(corner1NormPeers),
    field_min_past_corner_1_norm: minOfNumbers(corner1NormPeers),
    field_nige_candidate_count: countAbove(peerNigeRatesAll, NIGE_CANDIDATE_THRESHOLD),
    field_nige_pressure: fieldNige,
    field_oikomi_pressure: fieldOikomi,
    field_pace_index:
      fieldNige === null || fieldSenkou === null
        ? null
        : fieldNige * PACE_NIGE_WEIGHT + fieldSenkou * PACE_SENKOU_WEIGHT,
    field_sashi_pressure: fieldSashi,
    field_senkou_pressure: fieldSenkou,
    field_spread_past_corner_1_norm:
      maxOfNumbers(corner1NormPeers) === null || minOfNumbers(corner1NormPeers) === null
        ? null
        : (maxOfNumbers(corner1NormPeers) ?? 0) - (minOfNumbers(corner1NormPeers) ?? 0),
    field_top_speed_index: maxOfNumbers(pickNonNull(speedBest)),
  };
};

export const computeFieldFeaturesPerHorse = (
  horses: ReadonlyArray<HorsePeerInputs>,
): ReadonlyArray<HorseFieldRow> =>
  horses.map((horse, selfIndex) => {
    const fieldFeatures = buildFieldFeatures(horses, selfIndex);
    const selfNigeMinusAvg =
      horse.pastNigeRate !== null && fieldFeatures.field_nige_pressure !== null && horses.length > 1
        ? horse.pastNigeRate - fieldFeatures.field_nige_pressure / (horses.length - 1)
        : null;
    return {
      ...fieldFeatures,
      self_nige_rate_minus_field_avg: selfNigeMinusAvg,
      self_speed_index_vs_field_top: safeDivide(horse.speedIndexBest5, fieldFeatures.field_top_speed_index),
    };
  });
