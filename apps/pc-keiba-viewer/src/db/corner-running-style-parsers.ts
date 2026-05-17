// Run with: imported by corner-running-style-queries.ts and corresponding components (bun runtime)

const RUNNING_STYLE_LABELS = ["nige", "senkou", "sashi", "oikomi"] as const;
type RunningStyleLabel = (typeof RUNNING_STYLE_LABELS)[number];

interface RunningStyleProbabilities {
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
}

interface RaceRunningStyleRow extends RunningStyleProbabilities {
  raceKey: string;
  horseNumber: number;
  kettoTorokuBango: string;
  bamei: string | null;
  category: string;
  kaisaiNen: string;
  modelVersion: string;
  predictedLabel: RunningStyleLabel;
  predictedAt: string;
}

interface RaceLookupKeys {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const isRunningStyleLabel = (value: string): value is RunningStyleLabel => {
  for (const candidate of RUNNING_STYLE_LABELS) {
    if (candidate === value) return true;
  }
  return false;
};

const numericOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const requireNumber = (value: unknown, field: string): number => {
  const parsed = numericOrNull(value);
  if (parsed === null) throw new Error(`D1 row missing ${field}`);
  return parsed;
};

const requireString = (value: unknown, field: string): string => {
  if (typeof value === "string") return value;
  throw new Error(`D1 row missing ${field}`);
};

const stringOrNull = (value: unknown): string | null => {
  return typeof value === "string" ? value : null;
};

const requireRunningStyleLabel = (value: unknown): RunningStyleLabel => {
  if (typeof value === "string" && isRunningStyleLabel(value)) return value;
  throw new Error("D1 row predicted_label not in nige/senkou/sashi/oikomi");
};

const buildRaceKey = (keys: RaceLookupKeys): string =>
  `${keys.source}:${keys.kaisaiNen}${keys.kaisaiTsukihi}:${keys.keibajoCode}:${keys.raceBango}`;

const parseRaceRunningStyleRow = (raw: Record<string, unknown>): RaceRunningStyleRow => ({
  bamei: stringOrNull(raw.bamei),
  category: requireString(raw.category, "category"),
  horseNumber: requireNumber(raw.horse_number, "horse_number"),
  kaisaiNen: requireString(raw.kaisai_nen, "kaisai_nen"),
  kettoTorokuBango: requireString(raw.ketto_toroku_bango, "ketto_toroku_bango"),
  modelVersion: requireString(raw.model_version, "model_version"),
  p_nige: requireNumber(raw.p_nige, "p_nige"),
  p_oikomi: requireNumber(raw.p_oikomi, "p_oikomi"),
  p_sashi: requireNumber(raw.p_sashi, "p_sashi"),
  p_senkou: requireNumber(raw.p_senkou, "p_senkou"),
  predictedAt: requireString(raw.predicted_at, "predicted_at"),
  predictedLabel: requireRunningStyleLabel(raw.predicted_label),
  raceKey: requireString(raw.race_key, "race_key"),
});

export {
  buildRaceKey,
  isRunningStyleLabel,
  numericOrNull,
  parseRaceRunningStyleRow,
  requireNumber,
  requireRunningStyleLabel,
  requireString,
  RUNNING_STYLE_LABELS,
  stringOrNull,
};

export type {
  RaceLookupKeys,
  RaceRunningStyleRow,
  RunningStyleLabel,
  RunningStyleProbabilities,
};
