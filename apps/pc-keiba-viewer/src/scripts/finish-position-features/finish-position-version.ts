// Run with: imported via bun runtime (vitest / generate-finish-position-local.ts)
import finishPositionVersionJson from "./finish-position-version.json" with { type: "json" };

interface FinishPositionVersionJson {
  version: string;
  description: string;
}

const data: FinishPositionVersionJson = finishPositionVersionJson;

export const FINISH_POSITION_VERSION: string = data.version;
export const FINISH_POSITION_VERSION_DESCRIPTION: string = data.description;
