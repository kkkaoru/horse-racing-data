import { expect, it, vi } from "vitest";
import {
  parseParallelsVmStatus,
  runPcKeibaUpdateAndSync,
  type CommandRunner,
} from "./run-pc-keiba-update-and-sync";

it("parses supported Parallels VM statuses", () => {
  expect(parseParallelsVmStatus("VM Windows 11 exist stopped\n")).toBe("stopped");
  expect(parseParallelsVmStatus("VM Windows 11 exist running\n")).toBe("running");
  expect(parseParallelsVmStatus("VM Windows 11 exist suspended\n")).toBe("suspended");
  expect(parseParallelsVmStatus("VM Windows 11 exist paused\n")).toBe("paused");
});

it("rejects an unknown Parallels VM status", () => {
  expect(() => parseParallelsVmStatus("unexpected output")).toThrow(
    "Could not parse Parallels VM status: unexpected output",
  );
});

it("updates PC-KEIBA, verifies the stopped VM, then syncs replicas", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({
      exitCode: 0,
      stderr: "",
      stdout: "VM Windows 11 exist stopped\n",
    })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" });
  const log = vi.fn<(message: string) => void>();
  const triggerRealtimeDiscovery = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);

  await runPcKeibaUpdateAndSync({
    appDir: "/repo/apps/local-postgresql",
    bunExecutable: "/usr/local/bin/bun",
    log,
    runCommand,
    triggerRealtimeDiscovery,
    vmName: "Windows 11",
  });

  expect(runCommand.mock.calls).toStrictEqual([
    [
      ["/usr/local/bin/bun", "run", "--cwd", "/repo/apps/local-postgresql", "pc-keiba:update"],
      { env: { PARALLELS_STOP_AFTER_SUCCESS: "1" } },
    ],
    [["prlctl", "status", "Windows 11"], { captureOutput: true }],
    [
      [
        "/usr/local/bin/bun",
        "run",
        "--cwd",
        "/repo/apps/local-postgresql",
        "scrape:netkeiba-training",
      ],
    ],
    [["/usr/local/bin/bun", "run", "--cwd", "/repo/apps/local-postgresql", "replica:push"]],
  ]);
  expect(log.mock.calls).toStrictEqual([
    ["Step 1/5: updating PC-KEIBA data through the Parallels Windows VM..."],
    ["Step 2/5: verifying that the Windows VM stopped after the update..."],
    ["Step 3/5: importing JRA training workouts from netkeiba as backup..."],
    ["Step 4/5: syncing local PostgreSQL to R2 Catalog and Neon..."],
    ["Step 5/5: discovering synced races and planning premium fetches..."],
    ["PC-KEIBA update, R2 Catalog/Neon sync, and realtime discovery completed successfully."],
  ]);
  expect(triggerRealtimeDiscovery).toHaveBeenCalledOnce();
});

it("does not inspect the VM or sync when the PC-KEIBA update fails", async () => {
  const runCommand = vi.fn<CommandRunner>().mockResolvedValueOnce({
    exitCode: 7,
    stderr: "guest update failed",
    stdout: "",
  });

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      runCommand,
      triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("guest update failed");
  expect(runCommand).toHaveBeenCalledTimes(1);
});

it("does not sync when the VM status command fails", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 2, stderr: "VM not found", stdout: "" });

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      runCommand,
      triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("VM not found");
  expect(runCommand).toHaveBeenCalledTimes(2);
});

it("does not sync while the VM is still running", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({
      exitCode: 0,
      stderr: "",
      stdout: "VM Windows 11 exist running\n",
    });

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      runCommand,
      triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
      vmName: "Windows 11",
    }),
  ).rejects.toThrow(
    "Parallels VM 'Windows 11' must be stopped before replica sync; current status: running",
  );
  expect(runCommand).toHaveBeenCalledTimes(2);
});

it("reports a replica sync failure without re-running the update", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({
      exitCode: 0,
      stderr: "",
      stdout: "VM Windows 11 exist stopped\n",
    })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 9, stderr: "", stdout: "" });

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      runCommand,
      triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
      vmName: "Windows 11",
    }),
  ).rejects.toThrow(
    "Command failed (9): /usr/local/bin/bun run --cwd /repo/apps/local-postgresql replica:push",
  );
  expect(runCommand).toHaveBeenCalledTimes(4);
});

it("fails closed when realtime discovery fails after a successful replica sync", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({
      exitCode: 0,
      stderr: "",
      stdout: "VM Windows 11 exist stopped\n",
    })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" });
  const triggerRealtimeDiscovery = vi
    .fn<() => Promise<void>>()
    .mockRejectedValue(new Error("inline discovery failed"));
  const log = vi.fn<(message: string) => void>();

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      bunExecutable: "/usr/local/bin/bun",
      log,
      runCommand,
      triggerRealtimeDiscovery,
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("inline discovery failed");
  expect(runCommand).toHaveBeenCalledTimes(4);
  expect(triggerRealtimeDiscovery).toHaveBeenCalledOnce();
  expect(log.mock.calls).toStrictEqual([
    ["Step 1/5: updating PC-KEIBA data through the Parallels Windows VM..."],
    ["Step 2/5: verifying that the Windows VM stopped after the update..."],
    ["Step 3/5: importing JRA training workouts from netkeiba as backup..."],
    ["Step 4/5: syncing local PostgreSQL to R2 Catalog and Neon..."],
    ["Step 5/5: discovering synced races and planning premium fetches..."],
  ]);
});

it("does not start replica sync when the independent training import fails", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({
      exitCode: 0,
      stderr: "",
      stdout: "VM Windows 11 exist stopped\n",
    })
    .mockResolvedValueOnce({ exitCode: 6, stderr: "training API forbidden", stdout: "" });
  const triggerRealtimeDiscovery = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      runCommand,
      triggerRealtimeDiscovery,
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("training API forbidden");
  expect(runCommand).toHaveBeenCalledTimes(3);
  expect(triggerRealtimeDiscovery).not.toHaveBeenCalled();
});
