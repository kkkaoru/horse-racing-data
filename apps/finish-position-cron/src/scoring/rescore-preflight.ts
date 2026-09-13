// Run with bun. Reject obsolete weight generations before starting a Container.
import { fetchWeightForRace } from "./rescore-realtime";
import type { FetchRaceInput, WeightSnapshotGeneration } from "./rescore-realtime";

export interface RescorePreflightInput extends FetchRaceInput {
  weightGeneration: WeightSnapshotGeneration;
}

const WEIGHT_MISMATCH: string = "horse weight snapshot generation mismatch:";

export const verifyRescoreWeightGeneration = async (
  input: RescorePreflightInput,
): Promise<void> => {
  try {
    await fetchWeightForRace(input);
  } catch (error) {
    if (error instanceof Error && error.message.includes(WEIGHT_MISMATCH)) {
      // Retain the consumer's superseded-generation handling, without weakening
      // the authoritative in-Container validation against changes after this read.
      throw new Error(`post-weight snapshot generation mismatch: ${error.message}`, {
        cause: error,
      });
    }
    throw error;
  }
};
