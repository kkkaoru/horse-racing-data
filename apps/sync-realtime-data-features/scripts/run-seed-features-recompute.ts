// Run with: bun run apps/sync-realtime-data-features/scripts/run-seed-features-recompute.ts
// Local CLI wrapper that bypasses the macOS libc resolver by using Bun.dns with
// the c-ares backend and rewriting fetch() targets to a literal IPv4 with TLS
// SNI / Host headers preserved. Same pattern as the odds migration wrapper at
// apps/sync-realtime-data-hot/scripts/run-migrate-odds-recent.ts.

import { buildDefaultConfig, seedFeaturesRecompute } from "./seed-features-recompute";

const hostToIp = new Map<string, string>();

const resolveHost = async (host: string): Promise<string> => {
  const cached = hostToIp.get(host);
  if (cached) {
    return cached;
  }
  const results = await Bun.dns.lookup(host, { backend: "c-ares" });
  const ipv4 = results.find((entry) => entry.family === 4);
  if (!ipv4) {
    throw new Error(`no IPv4 record for ${host}`);
  }
  hostToIp.set(host, ipv4.address);
  return ipv4.address;
};

const resolvingFetch: typeof fetch = async (input, init) => {
  const url = new URL(
    typeof input === "string" ? input : input instanceof URL ? input.href : input.url,
  );
  const originalHost = url.hostname;
  const ip = await resolveHost(originalHost);
  url.hostname = ip;
  const headers = new Headers(init?.headers);
  headers.set("host", originalHost);
  return fetch(url.toString(), {
    ...init,
    headers,
    tls: { serverName: originalHost },
  });
};

const config = buildDefaultConfig(new Date());
const result = await seedFeaturesRecompute({ ...config, fetchImpl: resolvingFetch });
console.log(`seed-features-recompute done: ${JSON.stringify(result)}`);
