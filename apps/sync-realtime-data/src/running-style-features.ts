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

const RUNNING_STYLE_RACE_KEY_PATTERN =
  /^(jra|nar):(\d{4})(\d{4}):(\d{2}):(\d{2})$/u;

export const parseRunningStyleRaceKey = (raceKey: string): (RunningStyleRaceParams & { raceKey: string }) | null => {
  const match = raceKey.match(RUNNING_STYLE_RACE_KEY_PATTERN);
  if (match === null) {
    return null;
  }
  const source = match[1];
  if (source !== "jra" && source !== "nar") {
    return null;
  }
  const params: RunningStyleRaceParams = {
    kaisaiNen: match[2] ?? "",
    kaisaiTsukihi: match[3] ?? "",
    keibajoCode: match[4] ?? "",
    raceBango: match[5] ?? "",
    source,
  };
  return {
    ...params,
    raceKey: buildRunningStyleRaceKey(params),
  };
};
