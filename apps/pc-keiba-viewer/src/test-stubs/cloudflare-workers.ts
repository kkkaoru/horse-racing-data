// Vitest stub for the `cloudflare:workers` built-in module so tests can import
// classes that `extend DurableObject`. The real implementation only exists in
// workerd; this stub only needs to satisfy the constructor signature.
export class DurableObject<Env = unknown> {
  protected ctx: PcKeibaDurableObjectState;
  protected env: Env;

  constructor(ctx: PcKeibaDurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
  }
}
