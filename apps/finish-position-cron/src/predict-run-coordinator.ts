// Run with bun. Durable Object coordinator for predict-run dedup and state tracking.
// Strong-consistency claim/complete: single DO instance serialises all calls via
// blockConcurrencyWhile. Key format: run:{runYmd}:{category}

import { DurableObject } from "cloudflare:workers";
import type { Env } from "./types";

const STORAGE_KEY_PREFIX = "run";
const CLAIM_PATH = "/claim";
const COMPLETE_PATH = "/complete";
const STATE_PATH = "/state";
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

const buildKey = (runYmd: string, category: string): string =>
  `${STORAGE_KEY_PREFIX}:${runYmd}:${category}`;

const TERMINAL_STATUSES = new Set(["started", "success"]);

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

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathMethodKey = `${request.method}:${url.pathname}`;
    const handlers = new Map<string, (req: Request) => Promise<Response>>([
      [`POST:${CLAIM_PATH}`, (req) => this.handleClaim(req)],
      [`POST:${COMPLETE_PATH}`, (req) => this.handleComplete(req)],
      [`GET:${STATE_PATH}`, (req) => this.handleState(req)],
    ]);
    const handler = handlers.get(pathMethodKey);
    if (handler) {
      return handler(request);
    }
    const knownPaths = new Set([CLAIM_PATH, COMPLETE_PATH, STATE_PATH]);
    return knownPaths.has(url.pathname)
      ? Response.json({ error: "Method not allowed" }, { status: HTTP_METHOD_NOT_ALLOWED })
      : Response.json({ error: "Not found" }, { status: HTTP_NOT_FOUND });
  }
}
