// Run with bun (vitest) / Cloudflare Workers runtime.
import { ANALYSIS_CELL_PARAM_NAMES } from "./past-race-cell-matching";
import {
  buildWinRateHeatmapCacheKey,
  serializeWinRateHeatmapCacheQuery,
} from "./win-rate-heatmap-cache";
import type { HeatmapWarmWorkflowParams } from "./win-rate-heatmap-warm-workflow";

export interface HeatmapExecutionRequest {
  dependencyRevision?: string;
  day: string;
  keibajoCode: string;
  month: string;
  query: string;
  raceNumber: string;
  source: "jra" | "nar";
  year: string;
}
export interface HeatmapExecutionOwner {
  ensure(request: HeatmapExecutionRequest): Promise<string>;
  claim(token: string): Promise<boolean>;
  settle(token: string, success: boolean): Promise<boolean>;
}
export interface HeatmapExecutionNamespace {
  getByName(name: string): HeatmapExecutionOwner;
}
export interface HeatmapExecutionState {
  dependencyRevision?: string;
  failures: number;
  phase: "queued" | "running" | "settled";
  retryAt: number;
  token: string;
  // Last transition time. Absent on states written before stale recovery existed.
  updatedAt?: number;
}
export interface HeatmapExecutionStorage {
  read(): unknown;
  write(state: HeatmapExecutionState): void;
}
export interface HeatmapExecutionWorkflow {
  createBatch(batch: { id: string; params: HeatmapWarmWorkflowParams }[]): Promise<unknown[]>;
}
const FLAGS: readonly string[] = [
  "Age",
  "Class",
  "Distance",
  "Frame",
  "RaceMonth",
  "MonthWindow",
  "NarOnly",
  "RaceNumber",
  "RaceSubtitle",
  "RaceName",
  "RaceTitle",
  "Sex",
  "Surface",
  "Track",
  "Turn",
  "Venue",
  "Weight",
];
const PREFIXES: readonly string[] = ["stats", "similarStats", "bloodlineStats", "analysisStats"];
const FLAG_NAMES: ReadonlySet<string> = new Set([
  ...PREFIXES.flatMap((prefix) => FLAGS.map((flag) => prefix + flag)),
  ...Object.values(ANALYSIS_CELL_PARAM_NAMES),
]);
const YEARS_NAMES: ReadonlySet<string> = new Set(PREFIXES.map((prefix) => prefix + "Years"));
const SCOPE_NAMES: ReadonlySet<string> = new Set([
  ...PREFIXES.map((prefix) => prefix + "SourceScope"),
  "resultsSourceScope",
  "source",
]);
// Includes the unbounded carried-weight presentation and fixed upstream cache protocol.
export const HEATMAP_ALGORITHM_REVISION = "qualified-frame-sql-v5";
const DEPENDENCY_REVISION: RegExp = /^qualified-frame-sql-v[1-9][0-9]*:gate-v1-[a-f0-9]{64}$/u;
const validDependencyRevision = (value: unknown): value is string =>
  typeof value === "string" && DEPENDENCY_REVISION.test(value);
const MAX_QUERY_LENGTH = 2048;
const MAX_FAILURES = 3;
const FAILURE_BACKOFF_MS = 30_000;
const REPEATED_FAILURE_BACKOFF_MS = 15 * 60_000;
// A queued/running owner whose Worker or Workflow died never settles; after this
// long without a transition it is re-queued instead of pinning the race forever.
const STALE_OWNER_MS = 10 * 60_000;

export const canonicalHeatmapQuery = (params: URLSearchParams): string => {
  if (params.toString().length > MAX_QUERY_LENGTH) throw new Error("Invalid heatmap query");
  const seen = new Set<string>();
  params.forEach((value, name) => {
    const valid =
      (FLAG_NAMES.has(name) && /^(0|1)$/u.test(value)) ||
      (YEARS_NAMES.has(name) && /^(all|[1-9]|10)$/u.test(value)) ||
      (SCOPE_NAMES.has(name) && /^(all|jra|nar)$/u.test(value));
    if (seen.has(name) || !valid) throw new Error("Invalid heatmap query");
    seen.add(name);
  });
  const query = new URLSearchParams(params);
  query.delete("source");
  query.sort();
  return query.toString();
};

export const heatmapExecutionKey = (request: HeatmapExecutionRequest): string =>
  buildWinRateHeatmapCacheKey({
    ...request,
    query: serializeWinRateHeatmapCacheQuery(new URLSearchParams(request.query)),
  });

const isState = (value: unknown): value is HeatmapExecutionState =>
  typeof value === "object" &&
  value !== null &&
  (!("dependencyRevision" in value) || validDependencyRevision(value.dependencyRevision)) &&
  "phase" in value &&
  ["queued", "running", "settled"].some((phase) => phase === value.phase) &&
  "token" in value &&
  typeof value.token === "string" &&
  value.token.length > 0 &&
  "failures" in value &&
  Number.isSafeInteger(value.failures) &&
  Number(value.failures) >= 0 &&
  "retryAt" in value &&
  typeof value.retryAt === "number" &&
  Number.isFinite(value.retryAt) &&
  (!("updatedAt" in value) ||
    (typeof value.updatedAt === "number" && Number.isFinite(value.updatedAt)));

const isStaleUnsettled = (state: HeatmapExecutionState, now: number): boolean =>
  state.phase !== "settled" &&
  (state.updatedAt === undefined || now - state.updatedAt >= STALE_OWNER_MS);

// Storage is synchronous SQLite. Every transition finishes before the first await.
// Only the server route that claimed this token may acknowledge settled work.
export class HeatmapExecutionCoordinator {
  constructor(
    private readonly storage: HeatmapExecutionStorage,
    private readonly workflow: HeatmapExecutionWorkflow,
  ) {}

  async ensure(request: HeatmapExecutionRequest): Promise<string> {
    if (canonicalHeatmapQuery(new URLSearchParams(request.query)) !== request.query)
      throw new Error("Noncanonical heatmap query");
    const previous = this.storage.read();
    if (previous !== undefined && !isState(previous)) return "unknown";
    const now = Date.now();
    if (previous !== undefined && previous.phase !== "settled" && !isStaleUnsettled(previous, now))
      return previous.phase;
    const stale = previous !== undefined && previous.phase !== "settled";
    const { dependencyRevision: suppliedRevision, ...workflowRequest } = request;
    const dependencyRevision =
      /^[A-Z][0-9A-Z]$/u.test(request.keibajoCode) && validDependencyRevision(suppliedRevision)
        ? suppliedRevision
        : undefined;
    const changed =
      previous !== undefined &&
      dependencyRevision !== undefined &&
      dependencyRevision !== previous.dependencyRevision;
    if (!changed && !stale && previous !== undefined && previous.retryAt > now) return "backoff";
    const token = crypto.randomUUID();
    const revision = dependencyRevision ?? previous?.dependencyRevision;
    this.storage.write({
      ...(revision === undefined ? {} : { dependencyRevision: revision }),
      failures: changed ? 0 : (previous?.failures ?? 0),
      phase: "queued",
      retryAt: 0,
      token,
      updatedAt: now,
    });
    // Ambiguous creation errors retain ownership, including retention holes.
    await this.workflow.createBatch([
      {
        id: `heatmap-${token}`,
        params: {
          ...workflowRequest,
          raceNumbers: [request.raceNumber],
          ownerToken: token,
        },
      },
    ]);
    return "queued";
  }

  claim(token: string): boolean {
    const state = this.storage.read();
    if (!isState(state) || state.phase !== "queued" || state.token !== token) return false;
    this.storage.write({ ...state, phase: "running", updatedAt: Date.now() });
    return true;
  }

  settle(token: string, success: boolean): boolean {
    const state = this.storage.read();
    if (!isState(state) || state.phase !== "running" || state.token !== token) return false;
    const failures = success ? 0 : Math.min(state.failures + 1, MAX_FAILURES);
    // Confirmed failures may recover after upstream publication; never release active owners.
    const backoff = failures >= MAX_FAILURES ? REPEATED_FAILURE_BACKOFF_MS : FAILURE_BACKOFF_MS;
    this.storage.write({
      ...state,
      failures,
      phase: "settled",
      retryAt: success ? 0 : Date.now() + backoff,
      updatedAt: Date.now(),
    });
    return true;
  }
}
