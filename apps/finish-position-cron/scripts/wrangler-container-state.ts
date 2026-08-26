// Run with bun. Wrangler JSON helpers for safe Container-aware deployment scripts.

import {
  findUnsafeContainerInstances,
  finishPositionContainerApplications,
  parseContainerApplications,
  parseContainerInstances,
  type UnsafeContainerInstance,
} from "../src/deploy-safety";

export const runCommand = async (command: readonly string[]): Promise<void> => {
  const process = Bun.spawn(command, { stderr: "inherit", stdout: "inherit" });
  const exitCode = await process.exited;
  if (exitCode !== 0) throw new Error(`${command.join(" ")} exited with status ${exitCode}`);
};

export const runWranglerJson = async (args: readonly string[]): Promise<unknown> => {
  const process = Bun.spawn(["bunx", "wrangler", ...args], {
    stderr: "inherit",
    stdout: "pipe",
  });
  const output = await new Response(process.stdout).text();
  const exitCode = await process.exited;
  if (exitCode !== 0) throw new Error(`Wrangler exited with status ${exitCode}`);
  return JSON.parse(output) as unknown;
};

export const listUnsafePredictionContainers = async (): Promise<UnsafeContainerInstance[]> => {
  const applications = finishPositionContainerApplications(
    parseContainerApplications(
      await runWranglerJson(["containers", "list", "--json", "--per-page", "100"]),
    ),
  );
  if (applications.length === 0) {
    throw new Error("No finish-position Container applications were found");
  }

  const unsafe: UnsafeContainerInstance[] = [];
  for (const application of applications) {
    const instances = parseContainerInstances(
      await runWranglerJson([
        "containers",
        "instances",
        application.id,
        "--json",
        "--per-page",
        "100",
      ]),
    );
    unsafe.push(...findUnsafeContainerInstances(application, instances));
  }
  return unsafe;
};

export const describeUnsafePredictionContainers = (
  unsafe: readonly UnsafeContainerInstance[],
): string =>
  unsafe
    .map(
      (instance) => `${instance.applicationName}/${instance.name ?? "unnamed"}:${instance.state}`,
    )
    .join(", ");
