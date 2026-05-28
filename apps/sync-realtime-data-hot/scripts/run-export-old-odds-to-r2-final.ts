// Run with: bun run apps/sync-realtime-data-hot/scripts/run-export-old-odds-to-r2-final.ts
// Phase F final-backup CLI wrapper. Mirrors run-migrate-odds-recent / run-
// backfill-old-odds: pulls IPv4 from Bun.dns (c-ares backend) and rewrites
// fetch() so libc-level resolver stalls or stale router NXDOMAIN entries do
// not block the final-backup archive script.

import { buildDefaultConfig, exportOldOddsToR2Final } from "./export-old-odds-to-r2-final";

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

const config = await buildDefaultConfig(resolvingFetch);
const result = await exportOldOddsToR2Final({ ...config, fetchImpl: resolvingFetch });
console.log(
  `export-old-odds-to-r2-final: stopped=${result.stoppedReason}, rows=${result.totalRows}, groups=${result.totalGroups}, lastId=${result.finalSinceId}`,
);
