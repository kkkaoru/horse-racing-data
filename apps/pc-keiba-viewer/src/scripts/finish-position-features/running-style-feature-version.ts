// Run with: imported via bun runtime (vitest / generate-running-style-local.ts)
import runningStyleFeatureVersionJson from "./running-style-feature-version.json" with { type: "json" };

interface RunningStyleFeatureVersionJson {
  version: string;
  description: string;
}

const data: RunningStyleFeatureVersionJson = runningStyleFeatureVersionJson;

export const RUNNING_STYLE_FEATURE_VERSION: string = data.version;
export const RUNNING_STYLE_FEATURE_VERSION_DESC: string = data.description;
