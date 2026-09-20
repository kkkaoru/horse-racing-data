// Runs with bun via Vitest only; the real module is used in workerd integration tests.
export class WorkerEntrypoint<Environment = unknown> {
  constructor(
    protected readonly ctx: ExecutionContext,
    protected readonly env: Environment,
  ) {}
}
export class DurableObject<Environment = unknown> {
  constructor(
    protected readonly ctx: DurableObjectState,
    protected readonly env: Environment,
  ) {}
}
