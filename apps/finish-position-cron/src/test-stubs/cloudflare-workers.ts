// Run with bun. Vitest stub for the `cloudflare:workers` built-in module so tests
// can import classes that extend DurableObject. The real implementation only
// exists in workerd; this stub satisfies the constructor signature used in tests.

export class WorkflowEntrypoint<Env = unknown, Params = unknown> {
  protected env: Env;

  constructor(_ctx: unknown, env: Env) {
    this.env = env;
  }

  run(_event: Readonly<{ payload: Params }>, _step: unknown): Promise<unknown> {
    return Promise.resolve(undefined);
  }
}

export class DurableObject<Env = unknown> {
  protected ctx: DurableObjectState;
  protected env: Env;

  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
  }
}
