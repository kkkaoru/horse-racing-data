// Run with bun. Shared safe wrapper around `@opennextjs/cloudflare`'s
// `getCloudflareContext` so server components and server-only libs degrade
// gracefully when the local dev wrangler proxy cannot resolve a remote
// binding (e.g. when the Cloudflare API call to
// `/accounts/<id>/workers/subdomain/edge-preview` fails because the user is
// not authenticated locally). Production behaviour is identical because the
// global cloudflare context is always present inside the worker. A plain
// production Node server (`next start`) must use only that synchronous global
// lookup: the asynchronous mode would start a local Wrangler platform proxy
// from the production config and expose unusable internal Durable Object stubs.

import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

export interface SafeCloudflareRuntime {
  ctx: PcKeibaExecutionContext | null;
  env: CloudflareEnv | null;
}

const EMPTY_RUNTIME: SafeCloudflareRuntime = { ctx: null, env: null };

const getRuntimeContext = async () => {
  try {
    return getCloudflareContext<Record<string, unknown>, PcKeibaExecutionContext>({ async: false });
  } catch (error: unknown) {
    if (process.env.NODE_ENV === "production") {
      throw error;
    }
    return getCloudflareContext<Record<string, unknown>, PcKeibaExecutionContext>({ async: true });
  }
};

export const safeGetCloudflareRuntime = async (): Promise<SafeCloudflareRuntime> => {
  try {
    const context = await getRuntimeContext();
    return { ctx: context.ctx ?? null, env: context.env ?? null };
  } catch {
    return EMPTY_RUNTIME;
  }
};

export const safeGetCloudflareEnv = async (): Promise<CloudflareEnv | null> =>
  (await safeGetCloudflareRuntime()).env;

export const safeGetCloudflareExecutionContext =
  async (): Promise<PcKeibaExecutionContext | null> => (await safeGetCloudflareRuntime()).ctx;
