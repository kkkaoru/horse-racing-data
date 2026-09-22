// Ambient module declaration for the `cloudflare:workers` built-in.
// Provides the minimal `DurableObject` base class signature used by
// `paddock-room.ts` and `race-trend-room.ts`, and the `WorkflowEntrypoint`
// signature used by `heatmap-warm-workflow.ts`.
declare module "cloudflare:workers" {
  export abstract class DurableObject<Env = unknown> {
    protected ctx: PcKeibaDurableObjectState;
    protected env: Env;
    constructor(ctx: PcKeibaDurableObjectState, env: Env);
    fetch?(request: Request): Response | Promise<Response>;
  }

  export interface WorkflowEvent<Params> {
    instanceId: string;
    payload: Readonly<Params>;
    timestamp: Date;
  }

  export interface WorkflowStepRetryConfig {
    backoff?: "constant" | "exponential" | "linear";
    delay: number | string;
    limit: number;
  }

  export interface WorkflowStepConfig {
    retries?: WorkflowStepRetryConfig;
    timeout?: number | string;
  }

  export interface WorkflowStep {
    do<T>(name: string, config: WorkflowStepConfig, callback: () => Promise<T>): Promise<T>;
  }

  export abstract class WorkflowEntrypoint<Env = unknown, Params = unknown> {
    protected ctx: PcKeibaExecutionContext;
    protected env: Env;
    constructor(ctx: PcKeibaExecutionContext, env: Env);
    abstract run(event: WorkflowEvent<Params>, step: WorkflowStep): Promise<unknown>;
  }
}
