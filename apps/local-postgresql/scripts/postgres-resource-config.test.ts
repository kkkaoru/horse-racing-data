import { expect, test } from "vitest";

import { formatShellResourceConfig, POSTGRES_RESOURCE_CONFIG } from "./postgres-resource-config";

test("uses the bounded PostgreSQL container resource budget", () => {
  expect(POSTGRES_RESOURCE_CONFIG).toStrictEqual({
    containerMemory: "14G",
    containerCpus: "8",
    sharedBuffers: "3GB",
    effectiveCacheSize: "10GB",
    workMem: "32MB",
    maintenanceWorkMem: "512MB",
    maxWorkerProcesses: "8",
    maxParallelWorkers: "6",
    maxParallelWorkersPerGather: "3",
    maxParallelMaintenanceWorkers: "2",
  });
});

test("prints the shell assignments consumed by start.sh", () => {
  expect(formatShellResourceConfig()).toBe(
    "POSTGRES_CONTAINER_MEMORY=14G\n" +
      "POSTGRES_CONTAINER_CPUS=8\n" +
      "POSTGRES_SHARED_BUFFERS=3GB\n" +
      "POSTGRES_EFFECTIVE_CACHE_SIZE=10GB\n" +
      "POSTGRES_WORK_MEM=32MB\n" +
      "POSTGRES_MAINTENANCE_WORK_MEM=512MB\n" +
      "POSTGRES_MAX_WORKER_PROCESSES=8\n" +
      "POSTGRES_MAX_PARALLEL_WORKERS=6\n" +
      "POSTGRES_MAX_PARALLEL_WORKERS_PER_GATHER=3\n" +
      "POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS=2",
  );
});
