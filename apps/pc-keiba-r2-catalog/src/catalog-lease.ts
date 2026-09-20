// Runs with bun; SQLite-backed Durable Object, one instance per Catalog table.
import { DurableObject } from "cloudflare:workers";

export interface CatalogLeaseToken {
  owner: string;
  fence: number;
  expiresAt: number;
}
interface LeaseState {
  owner: string | null;
  fence: number;
  expiresAt: number;
}
interface LeaseChange<T> {
  state: LeaseState;
  result: T;
}

const KEY: string = "catalog-lease/v1";
const MAX_TTL_MS: number = 15 * 60 * 1000;
const emptyState = (): LeaseState => ({ owner: null, fence: 0, expiresAt: 0 });
const validateOwner = (owner: string): void => {
  if (typeof owner !== "string" || owner.length === 0 || owner.length > 256)
    throw new Error("Invalid Catalog lease owner");
};
const validateTtl = (ttlMs: number): void => {
  if (!Number.isInteger(ttlMs) || ttlMs < 1000 || ttlMs > MAX_TTL_MS)
    throw new Error("Invalid Catalog lease lifetime");
};
const validateToken = (token: CatalogLeaseToken): void => {
  if (typeof token !== "object" || token === null) throw new Error("Invalid Catalog lease token");
  validateOwner(token.owner);
  if (
    !Number.isSafeInteger(token.fence) ||
    token.fence < 1 ||
    !Number.isSafeInteger(token.expiresAt) ||
    token.expiresAt < 0
  )
    throw new Error("Invalid Catalog lease token");
};
const matches = (state: LeaseState, token: CatalogLeaseToken): boolean =>
  state.owner === token.owner && state.fence === token.fence;

/**
 * A persisted fencing lease, NOT a claim of Catalog commit visibility.
 * Publishers must also fence their external commit and verify visibility before
 * publishing a read revision. Do not activate beside legacy D1-only writers.
 */
export class CatalogLeaseCoordinator extends DurableObject<unknown> {
  #change<T>(operation: (state: LeaseState, now: number) => LeaseChange<T>): T {
    return this.ctx.storage.transactionSync(() => {
      const state: LeaseState = this.ctx.storage.kv.get<LeaseState>(KEY) ?? emptyState();
      const update: LeaseChange<T> = operation(state, Date.now());
      if (update.state !== state) this.ctx.storage.kv.put(KEY, update.state);
      return update.result;
    });
  }

  acquire(owner: string, ttlMs: number): CatalogLeaseToken | null {
    validateOwner(owner);
    validateTtl(ttlMs);
    return this.#change<CatalogLeaseToken | null>((state, now) => {
      if (state.owner !== null && state.expiresAt > now) {
        if (state.owner !== owner) return { state, result: null };
        // Retry acknowledges the existing lease without silently extending it.
        return { state, result: { owner, fence: state.fence, expiresAt: state.expiresAt } };
      }
      const fence: number = state.fence + 1;
      if (!Number.isSafeInteger(fence)) throw new Error("Catalog lease fencing counter exhausted");
      const result: CatalogLeaseToken = { owner, fence, expiresAt: now + ttlMs };
      return { state: result, result };
    });
  }

  renew(token: CatalogLeaseToken, ttlMs: number): CatalogLeaseToken | null {
    validateToken(token);
    validateTtl(ttlMs);
    return this.#change<CatalogLeaseToken | null>((state, now) => {
      if (!matches(state, token) || state.expiresAt <= now) return { state, result: null };
      const result: CatalogLeaseToken = {
        owner: token.owner,
        fence: state.fence,
        expiresAt: Math.max(state.expiresAt, now + ttlMs),
      };
      return { state: result, result };
    });
  }

  release(token: CatalogLeaseToken): boolean {
    validateToken(token);
    return this.#change<boolean>((state) => {
      if (!matches(state, token)) return { state, result: false };
      return { state: { owner: null, fence: state.fence, expiresAt: 0 }, result: true };
    });
  }

  validate(token: CatalogLeaseToken): boolean {
    validateToken(token);
    return this.#change<boolean>((state, now) => ({
      state,
      result: matches(state, token) && state.expiresAt > now,
    }));
  }

  status(): CatalogLeaseToken | null {
    return this.#change<CatalogLeaseToken | null>((state, now) => ({
      state,
      result:
        state.owner !== null && state.expiresAt > now
          ? { owner: state.owner, fence: state.fence, expiresAt: state.expiresAt }
          : null,
    }));
  }
}
