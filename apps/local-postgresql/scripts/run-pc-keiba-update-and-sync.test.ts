import { expect, it, vi } from "vitest";
import {
  parseParallelsVmStatus,
  resolveFeatureBuildDateRange,
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

it("resolves a JST update window covering the upcoming publication horizon", () => {
  expect(resolveFeatureBuildDateRange(new Date("2026-08-25T15:00:00.000Z"))).toStrictEqual({
    fromDate: "20260826",
    toDate: "20260902",
  });
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
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" });
  const log = vi.fn<(message: string) => void>();
  const triggerRealtimeDiscovery = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const attestFinishPosition = vi
    .fn<(runYmd: string) => Promise<void>>()
    .mockResolvedValue(undefined);
  const stateStore = {
    clearCheckpoint: vi.fn<() => Promise<void>>().mockResolvedValue(undefined),
    loadCheckpoint: vi.fn().mockResolvedValue(null),
    recordCompletion: vi.fn().mockResolvedValue(undefined),
    saveCheckpoint: vi.fn().mockResolvedValue(undefined),
  };

  await runPcKeibaUpdateAndSync({
    appDir: "/repo/apps/local-postgresql",
    attestFinishPosition,
    bunExecutable: "/usr/local/bin/bun",
    log,
    now: () => new Date("2026-08-25T09:00:00.000Z"),
    runCommand,
    stateStore,
    triggerRealtimeDiscovery,
    vmName: "Windows 11",
  });

  expect(runCommand.mock.calls[0]).toStrictEqual([
    ["/usr/local/bin/bun", "run", "--cwd", "/repo/apps/local-postgresql", "pc-keiba:update"],
    { env: { PARALLELS_STOP_AFTER_SUCCESS: "1" } },
  ]);
  expect(runCommand.mock.calls[1]).toStrictEqual([
    ["prlctl", "status", "Windows 11"],
    { captureOutput: true },
  ]);
  expect(runCommand.mock.calls[2]?.[0]).toEqual([
    "/usr/local/bin/bun",
    "run",
    "--cwd",
    "/repo/apps/pc-keiba-viewer",
    "dev:build-corner-features",
    "--",
    "--target",
    "local",
    "--source-scope",
    "all",
    "--from-date",
    expect.any(String),
    "--to-date",
    expect.any(String),
  ]);
  expect(runCommand.mock.calls[3]?.[0]).toStrictEqual([
    "/usr/local/bin/bun",
    "run",
    "--cwd",
    "/repo/apps/local-postgresql",
    "scrape:netkeiba-training",
  ]);
  expect(runCommand.mock.calls[4]?.[0]).toStrictEqual([
    "/usr/local/bin/bun",
    "run",
    "--cwd",
    "/repo/apps/local-postgresql",
    "replica:push",
  ]);
  expect(runCommand.mock.calls[5]?.[0]).toStrictEqual([
    "/usr/local/bin/bun",
    "run",
    "--env-file=/repo/.env",
    "--cwd",
    "/repo/apps/pc-keiba-r2-catalog",
    "sync:entity-history-serving",
    "--",
    "--year",
    "2026",
  ]);
  expect(log.mock.calls.map(([message]) => message)).toEqual([
    "Step 1/8: updating PC-KEIBA data through the Parallels Windows VM...",
    "Step 2/8: verifying that the Windows VM stopped after the update...",
    expect.stringMatching(/^Step 3\/8: materializing local corner features/),
    "Step 4/8: importing JRA training workouts from netkeiba as backup...",
    "Step 5/8: syncing local PostgreSQL to R2 Catalog and Neon...",
    "Step 6/8: publishing direct-Catalog entity history for the selected year...",
    "Step 7/8: discovering synced races and planning premium fetches...",
    "Step 8/8: attesting pre-weight prediction and KV readiness for upcoming 20260825 races...",
    "PC-KEIBA update, R2 Catalog/Neon sync, direct-Catalog entity history publication, realtime discovery, and prediction readiness attestation completed successfully.",
  ]);
  expect(triggerRealtimeDiscovery).toHaveBeenCalledOnce();
  expect(attestFinishPosition).toHaveBeenCalledWith("20260825");
  expect(stateStore.recordCompletion).toHaveBeenCalledWith({
    completedAt: "2026-08-25T09:00:00.000Z",
    runYmd: "20260825",
    version: 1,
  });
  expect(stateStore.clearCheckpoint).toHaveBeenCalledOnce();
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
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand,
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint: vi.fn().mockResolvedValue(undefined),
      },
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
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand,
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint: vi.fn().mockResolvedValue(undefined),
      },
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
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand,
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint: vi.fn().mockResolvedValue(undefined),
      },
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
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 9, stderr: "", stdout: "" });

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand,
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint: vi.fn().mockResolvedValue(undefined),
      },
      triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
      vmName: "Windows 11",
    }),
  ).rejects.toThrow(
    "Command failed (9): /usr/local/bin/bun run --cwd /repo/apps/local-postgresql replica:push",
  );
  expect(runCommand).toHaveBeenCalledTimes(5);
});

it("fails before discovery when direct-Catalog entity history publication fails", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({
      exitCode: 0,
      stderr: "",
      stdout: "VM Windows 11 exist stopped\n",
    })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 8, stderr: "manifest publish failed", stdout: "" });
  const triggerRealtimeDiscovery = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand,
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint: vi.fn().mockResolvedValue(undefined),
      },
      triggerRealtimeDiscovery,
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("manifest publish failed");
  expect(runCommand).toHaveBeenCalledTimes(6);
  expect(triggerRealtimeDiscovery).not.toHaveBeenCalled();
});

it("retries a replica sync broken-pipe exit without re-running the update", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({
      exitCode: 0,
      stderr: "",
      stdout: "VM Windows 11 exist stopped\n",
    })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 141, stderr: "broken pipe", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" });
  const log = vi.fn<(message: string) => void>();

  await runPcKeibaUpdateAndSync({
    appDir: "/repo/apps/local-postgresql",
    attestFinishPosition: vi.fn().mockResolvedValue(undefined),
    bunExecutable: "/usr/local/bin/bun",
    log,
    now: () => new Date("2026-08-25T09:00:00.000Z"),
    runCommand,
    stateStore: {
      clearCheckpoint: vi.fn().mockResolvedValue(undefined),
      loadCheckpoint: vi.fn().mockResolvedValue(null),
      recordCompletion: vi.fn().mockResolvedValue(undefined),
      saveCheckpoint: vi.fn().mockResolvedValue(undefined),
    },
    triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
    vmName: "Windows 11",
  });

  expect(runCommand).toHaveBeenCalledTimes(7);
  expect(runCommand.mock.calls[4]?.[0]).toEqual(runCommand.mock.calls[5]?.[0]);
  expect(runCommand.mock.calls[6]?.[0]).toStrictEqual([
    "/usr/local/bin/bun",
    "run",
    "--env-file=/repo/.env",
    "--cwd",
    "/repo/apps/pc-keiba-r2-catalog",
    "sync:entity-history-serving",
    "--",
    "--year",
    "2026",
  ]);
  expect(log).toHaveBeenCalledWith(
    expect.stringContaining("broken pipe (141); retrying without repeating PC-KEIBA update"),
  );
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
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" });
  const triggerRealtimeDiscovery = vi
    .fn<() => Promise<void>>()
    .mockRejectedValue(new Error("inline discovery failed"));
  const log = vi.fn<(message: string) => void>();

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log,
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand,
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint: vi.fn().mockResolvedValue(undefined),
      },
      triggerRealtimeDiscovery,
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("inline discovery failed");
  expect(runCommand).toHaveBeenCalledTimes(6);
  expect(triggerRealtimeDiscovery).toHaveBeenCalledOnce();
  expect(log.mock.calls).toStrictEqual([
    ["Step 1/8: updating PC-KEIBA data through the Parallels Windows VM..."],
    ["Step 2/8: verifying that the Windows VM stopped after the update..."],
    [expect.stringMatching(/^Step 3\/8: materializing local corner features/)],
    ["Step 4/8: importing JRA training workouts from netkeiba as backup..."],
    ["Step 5/8: syncing local PostgreSQL to R2 Catalog and Neon..."],
    ["Step 6/8: publishing direct-Catalog entity history for the selected year..."],
    ["Step 7/8: discovering synced races and planning premium fetches..."],
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
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 6, stderr: "training API forbidden", stdout: "" });
  const triggerRealtimeDiscovery = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand,
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint: vi.fn().mockResolvedValue(undefined),
      },
      triggerRealtimeDiscovery,
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("training API forbidden");
  expect(runCommand).toHaveBeenCalledTimes(4);
  expect(triggerRealtimeDiscovery).not.toHaveBeenCalled();
});

it("resumes a failed same-day run at replica sync without repeating completed data acquisition", async () => {
  const runCommand = vi.fn<CommandRunner>().mockResolvedValue({
    exitCode: 0,
    stderr: "",
    stdout: "",
  });
  const triggerRealtimeDiscovery = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const attestFinishPosition = vi
    .fn<(runYmd: string) => Promise<void>>()
    .mockResolvedValue(undefined);
  const saveCheckpoint = vi.fn().mockResolvedValue(undefined);
  const clearCheckpoint = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();

  await runPcKeibaUpdateAndSync({
    appDir: "/repo/apps/local-postgresql",
    attestFinishPosition,
    bunExecutable: "/usr/local/bin/bun",
    log,
    now: () => new Date("2026-08-25T09:00:00.000Z"),
    runCommand,
    stateStore: {
      clearCheckpoint,
      loadCheckpoint: vi.fn().mockResolvedValue({
        nextStep: "sync",
        runYmd: "20260825",
        updatedAt: "2026-08-25T08:00:00.000Z",
        version: 1,
      }),
      recordCompletion: vi.fn().mockResolvedValue(undefined),
      saveCheckpoint,
    },
    triggerRealtimeDiscovery,
    vmName: "Windows 11",
  });

  expect(runCommand.mock.calls).toStrictEqual([
    [["/usr/local/bin/bun", "run", "--cwd", "/repo/apps/local-postgresql", "replica:push"]],
    [
      [
        "/usr/local/bin/bun",
        "run",
        "--env-file=/repo/.env",
        "--cwd",
        "/repo/apps/pc-keiba-r2-catalog",
        "sync:entity-history-serving",
        "--",
        "--year",
        "2026",
      ],
    ],
  ]);
  expect(triggerRealtimeDiscovery).toHaveBeenCalledOnce();
  expect(attestFinishPosition).toHaveBeenCalledWith("20260825");
  expect(saveCheckpoint.mock.calls).toStrictEqual([
    [
      {
        nextStep: "entity-history",
        runYmd: "20260825",
        updatedAt: "2026-08-25T09:00:00.000Z",
        version: 1,
      },
    ],
    [
      {
        nextStep: "discovery",
        runYmd: "20260825",
        updatedAt: "2026-08-25T09:00:00.000Z",
        version: 1,
      },
    ],
    [
      {
        nextStep: "readiness",
        runYmd: "20260825",
        updatedAt: "2026-08-25T09:00:00.000Z",
        version: 1,
      },
    ],
  ]);
  expect(clearCheckpoint).toHaveBeenCalledOnce();
  expect(log).toHaveBeenCalledWith("Resuming update-and-sync run 20260825 from step 'sync'.");
});

it("does not use a stale prior-day checkpoint to skip a new PC-KEIBA update", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({
      exitCode: 0,
      stderr: "",
      stdout: "VM Windows 11 exist stopped\n",
    })
    .mockResolvedValue({ exitCode: 0, stderr: "", stdout: "" });
  const log = vi.fn<(message: string) => void>();

  await runPcKeibaUpdateAndSync({
    appDir: "/repo/apps/local-postgresql",
    attestFinishPosition: vi.fn().mockResolvedValue(undefined),
    bunExecutable: "/usr/local/bin/bun",
    log,
    now: () => new Date("2026-08-25T09:00:00.000Z"),
    runCommand,
    stateStore: {
      clearCheckpoint: vi.fn().mockResolvedValue(undefined),
      loadCheckpoint: vi.fn().mockResolvedValue({
        nextStep: "readiness",
        runYmd: "20260824",
        updatedAt: "2026-08-24T09:00:00.000Z",
        version: 1,
      }),
      recordCompletion: vi.fn().mockResolvedValue(undefined),
      saveCheckpoint: vi.fn().mockResolvedValue(undefined),
    },
    triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
    vmName: "Windows 11",
  });

  expect(runCommand.mock.calls[0]).toStrictEqual([
    ["/usr/local/bin/bun", "run", "--cwd", "/repo/apps/local-postgresql", "pc-keiba:update"],
    { env: { PARALLELS_STOP_AFTER_SUCCESS: "1" } },
  ]);
  expect(log).toHaveBeenCalledWith(
    "Ignoring stale update-and-sync checkpoint for 20260824; starting 20260825 from the PC-KEIBA update.",
  );
});

it("does not advance the update checkpoint when PC-KEIBA reports a failure", async () => {
  const saveCheckpoint = vi.fn().mockResolvedValue(undefined);

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand: vi.fn<CommandRunner>().mockResolvedValue({
        exitCode: 7,
        stderr: "guest update failed",
        stdout: "",
      }),
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint,
      },
      triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("guest update failed");
  expect(saveCheckpoint).not.toHaveBeenCalled();
});

it("persists verify-vm only after the PC-KEIBA update command succeeds", async () => {
  const runCommand = vi
    .fn<CommandRunner>()
    .mockResolvedValueOnce({ exitCode: 0, stderr: "", stdout: "" })
    .mockResolvedValueOnce({ exitCode: 4, stderr: "status unavailable", stdout: "" });
  const saveCheckpoint = vi.fn().mockResolvedValue(undefined);

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      attestFinishPosition: vi.fn().mockResolvedValue(undefined),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand,
      stateStore: {
        clearCheckpoint: vi.fn().mockResolvedValue(undefined),
        loadCheckpoint: vi.fn().mockResolvedValue(null),
        recordCompletion: vi.fn().mockResolvedValue(undefined),
        saveCheckpoint,
      },
      triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("status unavailable");

  expect(saveCheckpoint.mock.calls).toStrictEqual([
    [
      {
        nextStep: "verify-vm",
        runYmd: "20260825",
        updatedAt: "2026-08-25T09:00:00.000Z",
        version: 1,
      },
    ],
  ]);
  const updateInvocation = runCommand.mock.invocationCallOrder[0];
  const checkpointInvocation = saveCheckpoint.mock.invocationCallOrder[0];
  expect(updateInvocation).toBeDefined();
  expect(checkpointInvocation).toBeDefined();
  if (updateInvocation === undefined || checkpointInvocation === undefined) {
    throw new Error("Expected update and checkpoint invocations");
  }
  expect(updateInvocation).toBeLessThan(checkpointInvocation);
});

it("keeps a readiness checkpoint and withholds completion when production attestation fails", async () => {
  const clearCheckpoint = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const recordCompletion = vi.fn().mockResolvedValue(undefined);

  await expect(
    runPcKeibaUpdateAndSync({
      appDir: "/repo/apps/local-postgresql",
      attestFinishPosition: vi.fn().mockRejectedValue(new Error("predictions incomplete")),
      bunExecutable: "/usr/local/bin/bun",
      log: vi.fn(),
      now: () => new Date("2026-08-25T09:00:00.000Z"),
      runCommand: vi.fn(),
      stateStore: {
        clearCheckpoint,
        loadCheckpoint: vi.fn().mockResolvedValue({
          nextStep: "readiness",
          runYmd: "20260825",
          updatedAt: "2026-08-25T08:00:00.000Z",
          version: 1,
        }),
        recordCompletion,
        saveCheckpoint: vi.fn().mockResolvedValue(undefined),
      },
      triggerRealtimeDiscovery: vi.fn().mockResolvedValue(undefined),
      vmName: "Windows 11",
    }),
  ).rejects.toThrow("predictions incomplete");
  expect(recordCompletion).not.toHaveBeenCalled();
  expect(clearCheckpoint).not.toHaveBeenCalled();
});
