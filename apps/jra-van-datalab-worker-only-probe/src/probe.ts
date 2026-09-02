// Run with bun. Capability boundary probe for a lightweight Workers isolate.

interface ProcessResult {
  status: number | null;
  stderr: string;
  stdout: string;
}

interface WorkerOnlyProbeResult {
  childProcessModule: "non-functional-stub" | "unexpectedly-executed";
  jvDataAcquisition: false;
  officialSdkExecutable: false;
  reason: string;
  runtime: "cloudflare-workers-isolate";
}

export type SpawnWine = () => ProcessResult;

const UNSUPPORTED_REASON: string =
  "Cloudflare Workers isolates cannot execute Wine, Windows PE binaries, or JV-Link COM. Use Containers or an official documented serverless wire API.";

export const probeWorkerOnly = (spawnWine: SpawnWine): WorkerOnlyProbeResult => {
  try {
    spawnWine();
    return {
      childProcessModule: "unexpectedly-executed",
      jvDataAcquisition: false,
      officialSdkExecutable: false,
      reason: UNSUPPORTED_REASON,
      runtime: "cloudflare-workers-isolate",
    };
  } catch {
    return {
      childProcessModule: "non-functional-stub",
      jvDataAcquisition: false,
      officialSdkExecutable: false,
      reason: UNSUPPORTED_REASON,
      runtime: "cloudflare-workers-isolate",
    };
  }
};
