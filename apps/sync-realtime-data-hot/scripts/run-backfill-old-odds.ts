// Run with: bun run apps/sync-realtime-data-hot/scripts/run-backfill-old-odds.ts
// Phase B-3 wrapper that mirrors run-migrate-odds-recent: pulls IPv4 from
// Bun.dns (c-ares backend) and rewrites fetch() so libc-level resolver stalls
// or stale router NXDOMAIN entries do not prevent the nightly backfill from
// running.

import { backfillOldOdds, buildDefaultConfig } from "./backfill-old-odds";

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
  const url = new URL(typeof input === "string" ? input : input.toString());
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

const config = await buildDefaultConfig(new Date(), resolvingFetch);
const result = await backfillOldOdds({ ...config, fetchImpl: resolvingFetch });
console.log(
  `backfill-old-odds: stopped=${result.stoppedReason}, inserted=${result.totalInserted}, lastId=${result.finalSinceId}`,
);
