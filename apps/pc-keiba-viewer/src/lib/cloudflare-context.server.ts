// Run with bun. Shared safe wrapper around `@opennextjs/cloudflare`'s
// `getCloudflareContext` so server components and server-only libs degrade
// gracefully when the local dev wrangler proxy cannot resolve a remote
// binding (e.g. when the Cloudflare API call to
// `/accounts/<id>/workers/subdomain/edge-preview` fails because the user is
// not authenticated locally). Production behaviour is identical because the
// global cloudflare context is always present inside the worker.

import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

export interface SafeCloudflareRuntime {
  ctx: PcKeibaExecutionContext | null;
  env: CloudflareEnv | null;
}

const EMPTY_RUNTIME: SafeCloudflareRuntime = { ctx: null, env: null };

export const safeGetCloudflareRuntime = async (): Promise<SafeCloudflareRuntime> => {
  try {
    const context = await getCloudflareContext<Record<string, unknown>, PcKeibaExecutionContext>({
      async: true,
    });
    return { ctx: context.ctx ?? null, env: context.env ?? null };
  } catch {
    return EMPTY_RUNTIME;
  }
};

export const safeGetCloudflareEnv = async (): Promise<CloudflareEnv | null> =>
  (await safeGetCloudflareRuntime()).env;

export const safeGetCloudflareExecutionContext =
  async (): Promise<PcKeibaExecutionContext | null> => (await safeGetCloudflareRuntime()).ctx;
