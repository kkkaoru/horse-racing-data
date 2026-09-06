export class PermanentJobError extends Error {
  constructor(
    readonly safeStage: string,
    options?: ErrorOptions,
  ) {
    super(`Permanent sync failure at ${safeStage}`, options);
    this.name = "PermanentJobError";
  }
}

export const permanentFailure = (stage: string, cause: unknown): PermanentJobError =>
  cause instanceof PermanentJobError ? cause : new PermanentJobError(stage, { cause });

export class TransientJobError extends Error {
  constructor(
    readonly safeStage: string,
    options?: ErrorOptions,
  ) {
    super(`Transient sync failure at ${safeStage}`, options);
    this.name = "TransientJobError";
  }
}

export const transientFailure = (stage: string, cause: unknown): TransientJobError =>
  cause instanceof TransientJobError ? cause : new TransientJobError(stage, { cause });

export const isPermanentJobError = (error: unknown): error is PermanentJobError =>
  error instanceof PermanentJobError;

export const isTransientJobError = (error: unknown): error is TransientJobError =>
  error instanceof TransientJobError;
