// Ambient module declaration for the `cloudflare:workers` built-in.
// Provides the minimal `DurableObject` base class signature used by
// `paddock-room.ts` and `race-trend-room.ts`.
declare module "cloudflare:workers" {
  export abstract class DurableObject<Env = unknown> {
    protected ctx: PcKeibaDurableObjectState;
    protected env: Env;
    constructor(ctx: PcKeibaDurableObjectState, env: Env);
    fetch?(request: Request): Response | Promise<Response>;
  }
}
