// Run with bun. Prints shell assignments consumed by scripts/start.sh.

export const POSTGRES_RESOURCE_CONFIG = {
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
} as const;

export const formatShellResourceConfig = (): string =>
  [
    `POSTGRES_CONTAINER_MEMORY=${POSTGRES_RESOURCE_CONFIG.containerMemory}`,
    `POSTGRES_CONTAINER_CPUS=${POSTGRES_RESOURCE_CONFIG.containerCpus}`,
    `POSTGRES_SHARED_BUFFERS=${POSTGRES_RESOURCE_CONFIG.sharedBuffers}`,
    `POSTGRES_EFFECTIVE_CACHE_SIZE=${POSTGRES_RESOURCE_CONFIG.effectiveCacheSize}`,
    `POSTGRES_WORK_MEM=${POSTGRES_RESOURCE_CONFIG.workMem}`,
    `POSTGRES_MAINTENANCE_WORK_MEM=${POSTGRES_RESOURCE_CONFIG.maintenanceWorkMem}`,
    `POSTGRES_MAX_WORKER_PROCESSES=${POSTGRES_RESOURCE_CONFIG.maxWorkerProcesses}`,
    `POSTGRES_MAX_PARALLEL_WORKERS=${POSTGRES_RESOURCE_CONFIG.maxParallelWorkers}`,
    `POSTGRES_MAX_PARALLEL_WORKERS_PER_GATHER=${POSTGRES_RESOURCE_CONFIG.maxParallelWorkersPerGather}`,
    `POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS=${POSTGRES_RESOURCE_CONFIG.maxParallelMaintenanceWorkers}`,
  ].join("\n");

if (import.meta.main) {
  process.stdout.write(`${formatShellResourceConfig()}\n`);
}
