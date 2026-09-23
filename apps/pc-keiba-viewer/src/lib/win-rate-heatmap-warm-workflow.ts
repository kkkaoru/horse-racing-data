// Run with bun (vitest) / Cloudflare Workers runtime.
// Win-rate heatmap cache warm orchestrated by Cloudflare Workflows.
//
// Heatmap warms used to share the detail-section queue. That queue backs up
// (18k+ messages, consumer concurrency 2) and heatmap messages were retried
// out of batches until max_retries dropped them, so most races of a day never
// stored a heatmap. One Workflow instance per (date, venue) now warms races
// sequentially, each race in its own durable step with retries.

import {
  buildDetailSectionApiPath,
  DETAIL_SECTION_CACHE_WARM_PARAM,
} from "./race-detail-section-cache";

export interface HeatmapWarmWorkflowParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumbers: string[];
  source: "jra" | "nar";
  year: string;
}

export interface HeatmapWarmRetryConfig {
  backoff: "constant" | "exponential" | "linear";
  delay: string;
  limit: number;
}

export interface HeatmapWarmStepConfig {
  retries: HeatmapWarmRetryConfig;
  timeout: string;
}

export interface HeatmapWarmStep {
  do<T>(name: string, config: HeatmapWarmStepConfig, callback: () => Promise<T>): Promise<T>;
}

export type HeatmapWarmFetch = (request: Request) => Promise<Response>;

export type HeatmapWarmRaceStatus = "failed" | "hit" | "stored";

export interface HeatmapWarmRaceResult {
  raceNumber: string;
  status: HeatmapWarmRaceStatus;
}

export interface RunHeatmapWarmWorkflowParams {
  fetchSelf: HeatmapWarmFetch;
  params: unknown;
  step: HeatmapWarmStep;
}

export interface HeatmapWarmRaceListItem {
  keibajoCode: string;
  raceBango: string;
  source: "jra" | "nar";
}

export interface HeatmapWarmDateParts {
  day: string;
  month: string;
  year: string;
}

export interface HeatmapWarmInstance {
  id: string;
  params: HeatmapWarmWorkflowParams;
}

export interface HeatmapWarmWorkflowBinding {
  createBatch(batch: HeatmapWarmInstance[]): Promise<unknown[]>;
}

export interface StartHeatmapWarmWorkflowsParams {
  date: HeatmapWarmDateParts;
  nowMs: number;
  races: readonly HeatmapWarmRaceListItem[];
  workflow: HeatmapWarmWorkflowBinding;
}

export interface StartHeatmapWarmWorkflowsResult {
  instanceIds: string[];
  raceCount: number;
}

interface VenueRaces {
  raceNumbers: string[];
  source: "jra" | "nar";
}

const INTERNAL_ORIGIN = "https://pc-keiba-viewer.local";
const HEATMAP_SECTION = "win-rate-heatmap";
const HEATMAP_CACHE_HEADER = "X-Win-Rate-Heatmap-Cache";
const CACHE_WARM_HEADER = "X-PC-Keiba-Cache-Warm";
const CACHE_WARM_HEADER_VALUE = "workflow";
const HEATMAP_STORED_HEADERS: ReadonlyArray<string> = ["HIT", "MISS-STORED"];
// A warm normally takes 10-20s, but an occasional self request hangs until
// the step timeout. Abort each request early. Keep per-race retries small:
// races run sequentially, so a race that keeps failing would otherwise hold
// the rest of its venue for ~20 minutes. The */15 sweep is the outer retry.
const HEATMAP_FETCH_TIMEOUT_MS = 120_000;
const HEATMAP_WARM_STEP_CONFIG: HeatmapWarmStepConfig = {
  retries: { backoff: "constant", delay: "10 seconds", limit: 1 },
  timeout: "3 minutes",
};
// Instance ids are deterministic per slot so duplicate triggers inside one
// slot are skipped by createBatch; a later slot re-sweeps after scratches.
const INSTANCE_SLOT_MS = 15 * 60 * 1000;
const MAX_INSTANCES_PER_BATCH = 100;
const DIGITS_2 = /^\d{2}$/u;
const DIGITS_4 = /^\d{4}$/u;
const KEIBAJO_CODE = /^[0-9A-Z]{2}$/u;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isDigits2 = (value: unknown): value is string =>
  typeof value === "string" && DIGITS_2.test(value);

export const isHeatmapWarmWorkflowParams = (value: unknown): value is HeatmapWarmWorkflowParams =>
  isRecord(value) &&
  typeof value.year === "string" &&
  DIGITS_4.test(value.year) &&
  isDigits2(value.month) &&
  isDigits2(value.day) &&
  typeof value.keibajoCode === "string" &&
  KEIBAJO_CODE.test(value.keibajoCode) &&
  (value.source === "jra" || value.source === "nar") &&
  Array.isArray(value.raceNumbers) &&
  value.raceNumbers.every(isDigits2);

const buildHeatmapUrl = (params: HeatmapWarmWorkflowParams, raceNumber: string): URL =>
  new URL(
    buildDetailSectionApiPath({
      day: params.day,
      keibajoCode: params.keibajoCode,
      month: params.month,
      raceNumber,
      section: HEATMAP_SECTION,
      year: params.year,
    }),
    INTERNAL_ORIGIN,
  );

// Only status and cache header matter. Heatmap payloads exceed the 1 MiB
// bounded drain, so cancel the body instead of reading it.
const discardResponseBody = async (response: Response): Promise<Response> => {
  await response.body?.cancel();
  return response;
};

const fetchHeatmap = (fetchSelf: HeatmapWarmFetch, url: URL): Promise<Response> =>
  fetchSelf(
    new Request(url, {
      headers: { [CACHE_WARM_HEADER]: CACHE_WARM_HEADER_VALUE },
      signal: AbortSignal.timeout(HEATMAP_FETCH_TIMEOUT_MS),
    }),
  ).then(discardResponseBody);

const isHeatmapHit = (response: Response): boolean =>
  response.ok && response.headers.get(HEATMAP_CACHE_HEADER) === "HIT";

const isHeatmapStored = (response: Response): boolean =>
  response.ok &&
  HEATMAP_STORED_HEADERS.some((value) => value === response.headers.get(HEATMAP_CACHE_HEADER));

export const warmHeatmapRace = async (
  fetchSelf: HeatmapWarmFetch,
  params: HeatmapWarmWorkflowParams,
  raceNumber: string,
): Promise<HeatmapWarmRaceStatus> => {
  const url = buildHeatmapUrl(params, raceNumber);
  if (isHeatmapHit(await fetchHeatmap(fetchSelf, url))) return "hit";
  url.searchParams.set(DETAIL_SECTION_CACHE_WARM_PARAM, "1");
  const warmed = await fetchHeatmap(fetchSelf, url);
  if (isHeatmapStored(warmed)) return "stored";
  throw new Error(
    `heatmap warm not stored: ${warmed.status} ${warmed.headers.get(HEATMAP_CACHE_HEADER) ?? "missing"} ${url.pathname}`,
  );
};

const runRaceStep = async (
  { fetchSelf, step }: Omit<RunHeatmapWarmWorkflowParams, "params">,
  params: HeatmapWarmWorkflowParams,
  raceNumber: string,
): Promise<HeatmapWarmRaceResult> => {
  try {
    const status = await step.do(
      `warm-${params.keibajoCode}-${raceNumber}`,
      HEATMAP_WARM_STEP_CONFIG,
      () => warmHeatmapRace(fetchSelf, params, raceNumber),
    );
    return { raceNumber, status };
  } catch (error) {
    console.error(
      "[pc-keiba-viewer] heatmap warm workflow step failed",
      error instanceof Error ? error.message : String(error),
    );
    return { raceNumber, status: "failed" };
  }
};

const runRacesSequentially = async (
  deps: Omit<RunHeatmapWarmWorkflowParams, "params">,
  params: HeatmapWarmWorkflowParams,
  raceNumbers: readonly string[],
): Promise<HeatmapWarmRaceResult[]> => {
  const [raceNumber, ...rest] = raceNumbers;
  if (raceNumber === undefined) return [];
  const result = await runRaceStep(deps, params, raceNumber);
  return [result, ...(await runRacesSequentially(deps, params, rest))];
};

export const runHeatmapWarmWorkflow = async ({
  fetchSelf,
  params,
  step,
}: RunHeatmapWarmWorkflowParams): Promise<HeatmapWarmRaceResult[]> => {
  if (!isHeatmapWarmWorkflowParams(params)) {
    throw new Error("invalid heatmap warm workflow params");
  }
  return runRacesSequentially({ fetchSelf, step }, params, params.raceNumbers);
};

const compareRaceNumbers = (left: string, right: string): number => left.localeCompare(right);

const groupRacesByVenue = (races: readonly HeatmapWarmRaceListItem[]): Map<string, VenueRaces> =>
  races.reduce((groups, race) => {
    groups.set(race.keibajoCode, {
      raceNumbers: [...(groups.get(race.keibajoCode)?.raceNumbers ?? []), race.raceBango],
      source: race.source,
    });
    return groups;
  }, new Map<string, VenueRaces>());

export const buildHeatmapWarmInstances = ({
  date,
  nowMs,
  races,
}: Omit<StartHeatmapWarmWorkflowsParams, "workflow">): HeatmapWarmInstance[] => {
  const slot = Math.floor(nowMs / INSTANCE_SLOT_MS).toString(36);
  return [...groupRacesByVenue(races)].map(([keibajoCode, venue]) => ({
    id: `heatmap-${date.year}${date.month}${date.day}-${keibajoCode}-${slot}`,
    params: {
      day: date.day,
      keibajoCode,
      month: date.month,
      raceNumbers: [...new Set(venue.raceNumbers)].toSorted(compareRaceNumbers),
      source: venue.source,
      year: date.year,
    },
  }));
};

const chunkInstances = (instances: readonly HeatmapWarmInstance[]): HeatmapWarmInstance[][] =>
  instances.length === 0
    ? []
    : [
        instances.slice(0, MAX_INSTANCES_PER_BATCH),
        ...chunkInstances(instances.slice(MAX_INSTANCES_PER_BATCH)),
      ];

export const startHeatmapWarmWorkflows = async (
  params: StartHeatmapWarmWorkflowsParams,
): Promise<StartHeatmapWarmWorkflowsResult> => {
  const instances = buildHeatmapWarmInstances(params);
  await Promise.all(chunkInstances(instances).map((chunk) => params.workflow.createBatch(chunk)));
  return {
    instanceIds: instances.map((instance) => instance.id),
    raceCount: params.races.length,
  };
};
