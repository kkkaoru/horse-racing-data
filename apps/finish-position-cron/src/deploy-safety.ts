// Run with bun. Pure deployment-safety checks shared by the local predeploy CLI.

const FINISH_POSITION_CONTAINER_PREFIX = "finish-position-cron-finishposition";

// Pause only queues that can start prediction work. Completion and control
// queues must keep running so accepted work can publish its result and stop
// its Container while the deployment waits for a clean drain.
export const DEPLOYMENT_DRAIN_QUEUES = [
  "finish-position-predict-queue",
  "finish-position-weight-rescore-queue",
] as const;

export interface ContainerApplicationSummary {
  id: string;
  name: string;
}

export interface ContainerInstanceSummary {
  name?: string;
  state: string;
}

export interface UnsafeContainerInstance extends ContainerInstanceSummary {
  applicationName: string;
}

export interface DeploymentRace {
  category: "ban-ei" | "jra" | "nar";
  keibajoCode: string;
  raceBango: string;
}

export interface DeploymentPredictionRequest extends DeploymentRace {
  force: boolean;
  runYmd: string;
}

export const shouldRequeueDeploymentPredictions = (value: string | undefined): boolean =>
  value === "1";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const parseContainerApplications = (value: unknown): ContainerApplicationSummary[] => {
  if (!Array.isArray(value)) throw new Error("Wrangler container list returned invalid JSON");
  return value.map((entry) => {
    if (!isRecord(entry) || typeof entry.id !== "string" || typeof entry.name !== "string") {
      throw new Error("Wrangler container list returned an invalid application");
    }
    return { id: entry.id, name: entry.name };
  });
};

export const parseContainerInstances = (value: unknown): ContainerInstanceSummary[] => {
  if (!Array.isArray(value)) throw new Error("Wrangler container instances returned invalid JSON");
  return value.map((entry) => {
    if (!isRecord(entry) || typeof entry.state !== "string") {
      throw new Error("Wrangler container instances returned an invalid instance");
    }
    return {
      ...(typeof entry.name === "string" ? { name: entry.name } : {}),
      state: entry.state,
    };
  });
};

export const parseDeploymentRaces = (value: unknown): DeploymentRace[] => {
  if (!Array.isArray(value) || value.length !== 1 || !isRecord(value[0])) {
    throw new Error("Wrangler D1 query returned invalid JSON");
  }
  const results = value[0].results;
  if (!Array.isArray(results)) throw new Error("Wrangler D1 query returned invalid results");
  return results.map((entry) => {
    if (!isRecord(entry)) throw new Error("Wrangler D1 query returned an invalid race");
    const source = entry.source;
    const keibajoCode = entry.keibajo_code;
    const raceBango = entry.race_bango;
    if (
      (source !== "jra" && source !== "nar" && source !== "ban-ei") ||
      typeof keibajoCode !== "string" ||
      typeof raceBango !== "string"
    ) {
      throw new Error("Wrangler D1 query returned an invalid race");
    }
    return { category: source, keibajoCode, raceBango };
  });
};

export const buildDeploymentPredictionRequest = (
  race: DeploymentRace,
  runYmd: string,
): DeploymentPredictionRequest => ({ ...race, force: true, runYmd });

export const finishPositionContainerApplications = (
  applications: readonly ContainerApplicationSummary[],
): ContainerApplicationSummary[] =>
  applications.filter((application) =>
    application.name.startsWith(FINISH_POSITION_CONTAINER_PREFIX),
  );

export const findUnsafeContainerInstances = (
  application: ContainerApplicationSummary,
  instances: readonly ContainerInstanceSummary[],
): UnsafeContainerInstance[] =>
  instances.flatMap((instance) =>
    instance.state === "inactive" ? [] : [{ ...instance, applicationName: application.name }],
  );
