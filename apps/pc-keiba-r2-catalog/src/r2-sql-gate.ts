// Runs with bun (Vitest) / Cloudflare Workers. Account-wide cap on concurrent heavy R2 SQL queries.
import { DurableObject } from "cloudflare:workers";
import type { Fetcher } from "./types";

// Measured against production R2 SQL on 2026-10-17 (same warm bloodline
// query): 0/8 runs hung one at a time, 1/8 at two concurrent, 2-4/8 at four
// and 4/16 at eight. Heavy aggregation queries are therefore admitted two at
// a time across every isolate; the rest wait in arrival order.
export const R2_SQL_GATE_MAX_ACTIVE: number = 2;
// A holder whose isolate died never releases; its slot is reclaimed after the
// longest R2 SQL attempt sequence (20s + 20s + 40s) plus margin.
export const R2_SQL_GATE_LEASE_MS: number = 90_000;
// A waiter that cannot get a slot in time proceeds without one instead of
// failing the request; the gate only shapes load, it never blocks forever.
export const R2_SQL_GATE_WAIT_MS: number = 60_000;
const GATE_NAME = "heavy-r2-sql";

export interface R2SqlGateStub {
  acquire(): Promise<string | null>;
  release(token: string): Promise<void>;
}

export interface R2SqlGateNamespace {
  getByName(name: string): R2SqlGateStub;
}

interface Waiter {
  resolve: (token: string | null) => void;
  timer: ReturnType<typeof setTimeout>;
}

// In-memory semaphore state. Losing it on eviction only resets the cap.
export class R2SqlGateState {
  private readonly leases = new Map<string, number>();
  private readonly waiters: Waiter[] = [];

  constructor(
    private readonly now: () => number = Date.now,
    private readonly newToken: () => string = () => crypto.randomUUID(),
  ) {}

  activeCount(): number {
    return this.leases.size;
  }

  acquire(): Promise<string | null> {
    this.expireLeases();
    if (this.leases.size < R2_SQL_GATE_MAX_ACTIVE) return Promise.resolve(this.grant());
    return new Promise((resolve) => {
      const waiter: Waiter = {
        resolve,
        timer: setTimeout(() => {
          this.waiters.splice(this.waiters.indexOf(waiter), 1);
          resolve(null);
        }, R2_SQL_GATE_WAIT_MS),
      };
      this.waiters.push(waiter);
    });
  }

  release(token: string): void {
    if (!this.leases.delete(token)) return;
    this.expireLeases();
    this.admitWaiters();
  }

  private grant(): string {
    const token = this.newToken();
    this.leases.set(token, this.now());
    return token;
  }

  private expireLeases(): void {
    const cutoff = this.now() - R2_SQL_GATE_LEASE_MS;
    for (const [token, grantedAt] of this.leases) {
      if (grantedAt <= cutoff) this.leases.delete(token);
    }
  }

  private admitWaiters(): void {
    while (this.leases.size < R2_SQL_GATE_MAX_ACTIVE) {
      const waiter = this.waiters.shift();
      if (waiter === undefined) return;
      clearTimeout(waiter.timer);
      waiter.resolve(this.grant());
    }
  }
}

export class R2SqlGate extends DurableObject {
  private readonly state = new R2SqlGateState();

  acquire(): Promise<string | null> {
    return this.state.acquire();
  }

  release(token: string): void {
    this.state.release(token);
  }
}

// Wraps a fetcher so each call holds one gate slot while R2 SQL computes.
// Without a gate binding (tests, local runs) the fetcher is returned unchanged.
export const gatedR2SqlFetcher =
  (fetchImpl: Fetcher, gate: R2SqlGateNamespace | undefined): Fetcher =>
  async (input, init) => {
    if (gate === undefined) return fetchImpl(input, init);
    const stub = gate.getByName(GATE_NAME);
    const token = await stub.acquire().catch(() => null);
    try {
      return await fetchImpl(input, init);
    } finally {
      if (token !== null) await stub.release(token).catch(() => undefined);
    }
  };
