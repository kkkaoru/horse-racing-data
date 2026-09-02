// Run with bun.
import { expect, it } from "vitest";
import { probeWorkerOnly } from "./probe";

it("reports the production child_process stub as unsupported", () => {
  const result = probeWorkerOnly(() => {
    throw new Error("node:child_process is not implemented");
  });

  expect(result).toStrictEqual({
    childProcessModule: "non-functional-stub",
    jvDataAcquisition: false,
    officialSdkExecutable: false,
    reason:
      "Cloudflare Workers isolates cannot execute Wine, Windows PE binaries, or JV-Link COM. Use Containers or an official documented serverless wire API.",
    runtime: "cloudflare-workers-isolate",
  });
});

it("remains fail-closed if a local test runtime unexpectedly executes a process", () => {
  const result = probeWorkerOnly(() => ({ status: 0, stderr: "", stdout: "wine-11" }));

  expect(result).toStrictEqual({
    childProcessModule: "unexpectedly-executed",
    jvDataAcquisition: false,
    officialSdkExecutable: false,
    reason:
      "Cloudflare Workers isolates cannot execute Wine, Windows PE binaries, or JV-Link COM. Use Containers or an official documented serverless wire API.",
    runtime: "cloudflare-workers-isolate",
  });
});
