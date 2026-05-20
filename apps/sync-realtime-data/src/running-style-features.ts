// Run with bun. Shared identity helpers for per-race running-style generation.

export type RunningStyleSource = "jra" | "nar";

export interface RunningStyleRaceParams {
  source: RunningStyleSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const RACE_BANGO_PAD_WIDTH = 2;
const KEIBAJO_CODE_PAD_WIDTH = 2;

export const normalizeKeibajoCode = (value: string): string =>
  value.padStart(KEIBAJO_CODE_PAD_WIDTH, "0");

export const normalizeRaceBango = (value: string): string =>
  value.padStart(RACE_BANGO_PAD_WIDTH, "0");

export const buildRunningStyleRaceKey = (params: RunningStyleRaceParams): string =>
  `${params.source}:${params.kaisaiNen}${params.kaisaiTsukihi}:${normalizeKeibajoCode(params.keibajoCode)}:${normalizeRaceBango(params.raceBango)}`;
