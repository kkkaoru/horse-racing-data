// Run with bun. Durable Object coordinator for predict-run dedup and state tracking.
// Strong-consistency claim/complete: single DO instance serialises all calls via
// blockConcurrencyWhile.
// - Per-category run key:  run:{runYmd}:{category}
// - Per-race rescore key:  rescore:{runYmd}:{category}:{keibajo}:{race}
//   used by the per-race coordinator to avoid enqueueing the same race twice.
// - Focused full key: focused-full:{runYmd}:{category}:{keibajo}:{race}
//   used by the queue consumer to prevent redelivery from starting a duplicate
//   container pipeline while the original detached build is still in flight.

import { DurableObject } from "cloudflare:workers";
import type { Env } from "./types";

const STORAGE_KEY_PREFIX = "run";
const RESCORE_KEY_PREFIX = "rescore";
const FOCUSED_FULL_KEY_PREFIX = "focused-full";
const CLAIM_PATH = "/claim";
const COMPLETE_PATH = "/complete";
const STATE_PATH = "/state";
const CLAIM_RACE_PATH = "/claim-race";
const CLAIM_FOCUSED_FULL_RACE_PATH = "/claim-focused-full-race";
const COMPLETE_FOCUSED_FULL_RACE_PATH = "/complete-focused-full-race";
const HTTP_OK = 200;
const HTTP_METHOD_NOT_ALLOWED = 405;
const HTTP_NOT_FOUND = 404;

interface RunRecord {
  status: string;
  timestamp: number;
  racesPredicted?: number;
  completedAt?: number;
}

interface ClaimResult {
  proceed: boolean;
  state?: string;
}

interface CompleteParams {
  runYmd: string;
  category: string;
  status: string;
  racesPredicted: number;
}

interface ClaimRaceParams {
  runYmd: string;
  category: string;
  keibajoCode: string;
  raceBango: string;
}

interface ClaimFocusedFullRaceParams extends ClaimRaceParams {
  staleAfterMs: number;
  force?: boolean;
}

interface CompleteFocusedFullRaceParams extends ClaimRaceParams {
  status: string;
}

const buildKey = (runYmd: string, category: string): string =>
  `${STORAGE_KEY_PREFIX}:${runYmd}:${category}`;

const buildRaceKey = (params: ClaimRaceParams): string =>
  `${RESCORE_KEY_PREFIX}:${params.runYmd}:${params.category}:${params.keibajoCode}:${params.raceBango}`;

const buildFocusedFullRaceKey = (params: ClaimRaceParams): string =>
  `${FOCUSED_FULL_KEY_PREFIX}:${params.runYmd}:${params.category}:${params.keibajoCode}:${params.raceBango}`;

const TERMINAL_STATUSES = new Set(["success"]);

export class PredictRunCoordinator extends DurableObject<Env> {
  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
  }

  async claim(runYmd: string, category: string): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const existing = await this.ctx.storage.get<RunRecord>(buildKey(runYmd, category));
      if (existing && TERMINAL_STATUSES.has(existing.status)) {
        return { proceed: false, state: existing.status };
      }
      await this.ctx.storage.put<RunRecord>(buildKey(runYmd, category), {
        status: "started",
        timestamp: Date.now(),
      });
      return { proceed: true };
    });
  }

  async complete(params: CompleteParams): Promise<void> {
    await this.ctx.storage.put<RunRecord>(buildKey(params.runYmd, params.category), {
      completedAt: Date.now(),
      racesPredicted: params.racesPredicted,
      status: params.status,
      timestamp: Date.now(),
    });
  }

  async getState(runYmd: string, category: string): Promise<RunRecord | undefined> {
    return this.ctx.storage.get<RunRecord>(buildKey(runYmd, category));
  }

  // Strong-consistency per-race claim used by the per-race coordinator. The
  // first caller for a (runYmd, category, keibajo, race) gets proceed:true and
  // the key is marked enqueued; subsequent callers get proceed:false so a race
  // is enqueued for rescore at most once per day. blockConcurrencyWhile
  // serialises the read-check-write so two cron ticks cannot both proceed.
  async claimRace(params: ClaimRaceParams): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const key = buildRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      if (existing !== undefined) {
        return { proceed: false, state: existing.status };
      }
      await this.ctx.storage.put<RunRecord>(key, {
        status: "enqueued",
        timestamp: Date.now(),
      });
      return { proceed: true };
    });
  }

  // Poll = every redelivery of a focused-full skipDedup message while the
  // race is still claimed and not yet stale (see queue-consumer.ts
  // claimFocusedFullOrRetry, called on every delivery attempt). Each such
  // poll is itself evidence that the queue is actively watching this race,
  // so the claim's heartbeat is refreshed here rather than left at its
  // original claim-time value. Without this refresh, staleAfterMs measured
  // "time since the pipeline was launched" instead of "time since we last
  // confirmed it was still worth waiting for" -- so shrinking staleAfterMs
  // to react quickly to a genuinely dead pipeline would also falsely
  // reclaim (and duplicate-launch) a legitimately slow one. With the
  // refresh, a live poll cadence keeps staleAfterMs from ever tripping for
  // work that is still being watched; it only trips once redeliveries stop
  // landing (retries exhausted -> dead-letter queue, see dlq-consumer.ts),
  // which is the actual "nobody is polling this anymore" signal.
  async claimFocusedFullRace(params: ClaimFocusedFullRaceParams): Promise<ClaimResult> {
    return this.ctx.blockConcurrencyWhile(async () => {
      const key = buildFocusedFullRaceKey(params);
      const existing = await this.ctx.storage.get<RunRecord>(key);
      const now = Date.now();
      if (existing !== undefined) {
        const isTerminal = TERMINAL_STATUSES.has(existing.status);
        if (isTerminal && params.force !== true) {
          return { proceed: false, state: existing.status };
        }
        const ageMs = now - existing.timestamp;
        if (!isTerminal && existing.status === "started" && ageMs < params.staleAfterMs) {
          await this.ctx.storage.put<RunRecord>(key, { status: existing.status, timestamp: now });
          return { proceed: false, state: existing.status };
        }
      }
      await this.ctx.storage.put<RunRecord>(key, {
        status: "started",
        timestamp: now,
      });
      return { proceed: true };
    });
  }

  async completeFocusedFullRace(params: CompleteFocusedFullRaceParams): Promise<void> {
    await this.ctx.storage.put<RunRecord>(buildFocusedFullRaceKey(params), {
      completedAt: Date.now(),
      status: params.status,
      timestamp: Date.now(),
    });
  }

  private async handleClaim(request: Request): Promise<Response> {
    const body = (await request.json()) as { runYmd: string; category: string };
    const result = await this.claim(body.runYmd, body.category);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleComplete(request: Request): Promise<Response> {
    const body = (await request.json()) as CompleteParams;
    await this.complete(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  private async handleState(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const runYmd = url.searchParams.get("runYmd") ?? "";
    const category = url.searchParams.get("category") ?? "";
    const result = await this.getState(runYmd, category);
    return Response.json({ state: result ?? null }, { status: HTTP_OK });
  }

  private async handleClaimRace(request: Request): Promise<Response> {
    const body = (await request.json()) as ClaimRaceParams;
    const result = await this.claimRace(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleClaimFocusedFullRace(request: Request): Promise<Response> {
    const body = (await request.json()) as ClaimFocusedFullRaceParams;
    const result = await this.claimFocusedFullRace(body);
    return Response.json(result, { status: HTTP_OK });
  }

  private async handleCompleteFocusedFullRace(request: Request): Promise<Response> {
    const body = (await request.json()) as CompleteFocusedFullRaceParams;
    await this.completeFocusedFullRace(body);
    return Response.json({ ok: true }, { status: HTTP_OK });
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathMethodKey = `${request.method}:${url.pathname}`;
    const handlers = new Map<string, (req: Request) => Promise<Response>>([
      [`POST:${CLAIM_PATH}`, (req) => this.handleClaim(req)],
      [`POST:${COMPLETE_PATH}`, (req) => this.handleComplete(req)],
      [`GET:${STATE_PATH}`, (req) => this.handleState(req)],
      [`POST:${CLAIM_RACE_PATH}`, (req) => this.handleClaimRace(req)],
      [`POST:${CLAIM_FOCUSED_FULL_RACE_PATH}`, (req) => this.handleClaimFocusedFullRace(req)],
      [`POST:${COMPLETE_FOCUSED_FULL_RACE_PATH}`, (req) => this.handleCompleteFocusedFullRace(req)],
    ]);
    const handler = handlers.get(pathMethodKey);
    if (handler) {
      return handler(request);
    }
    const knownPaths = new Set([
      CLAIM_PATH,
      COMPLETE_PATH,
      STATE_PATH,
      CLAIM_RACE_PATH,
      CLAIM_FOCUSED_FULL_RACE_PATH,
      COMPLETE_FOCUSED_FULL_RACE_PATH,
    ]);
    return knownPaths.has(url.pathname)
      ? Response.json({ error: "Method not allowed" }, { status: HTTP_METHOD_NOT_ALLOWED })
      : Response.json({ error: "Not found" }, { status: HTTP_NOT_FOUND });
  }
}
