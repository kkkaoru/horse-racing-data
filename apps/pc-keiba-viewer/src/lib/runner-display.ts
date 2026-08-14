import { cleanText } from "./format";
import type { Runner } from "./race-types";

export interface RunnerDisplayNames {
  horse: string;
  jockey: string;
  owner: string;
  trainer: string;
}

export const getRunnerDisplayNames = (runner: Runner): RunnerDisplayNames => ({
  horse: cleanText(runner.horseNameFull, cleanText(runner.bamei, "")),
  jockey: cleanText(runner.jockeyNameFull, cleanText(runner.kishumeiRyakusho, "")),
  owner: cleanText(runner.ownerNameFull, cleanText(runner.banushimei, "")),
  trainer: cleanText(runner.trainerNameFull, cleanText(runner.chokyoshimeiRyakusho, "")),
});
