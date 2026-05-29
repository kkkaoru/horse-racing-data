// Run with: CONFIRM_DELETE=1 bun run apps/sync-realtime-data/scripts/run-delete-daily-race-entries-chunk.ts
// Phase F nightly delete CLI wrapper. Mirrors sync-realtime-data-hot's
// run-delete-odds-snapshots-chunk: pulls IPv4 from Bun.dns (c-ares backend)
// and rewrites fetch() so libc-level resolver stalls or stale router NXDOMAIN
// entries do not block the nightly chunk-delete script.

import {
  buildDefaultConfig,
  deleteDailyRaceEntriesChunked,
} from "./delete-daily-race-entries-chunk";

const IPV4_FAMILY = 4;

export const createResolvingFetch =
  (baseFetch: typeof fetch, lookupHost: (host: string) => Promise<string>): typeof fetch =>
  async (input, init) => {
    const url = new URL(
      typeof input === "string" ? input : input instanceof URL ? input.href : input.url,
    );
    const originalHost = url.hostname;
    const ip = await lookupHost(originalHost);
    url.hostname = ip;
    const headers = new Headers(init?.headers);
    headers.set("host", originalHost);
    return baseFetch(url.toString(), {
      ...init,
      headers,
      tls: { serverName: originalHost },
    });
  };

export const createCachedHostLookup = (
  lookupImpl: (host: string) => Promise<readonly { address: string; family: number }[]>,
): ((host: string) => Promise<string>) => {
  const hostToIp = new Map<string, string>();
  return async (host: string): Promise<string> => {
    const cached = hostToIp.get(host);
    if (cached) {
      return cached;
    }
    const results = await lookupImpl(host);
    const ipv4 = results.find((entry) => entry.family === IPV4_FAMILY);
    if (!ipv4) {
      throw new Error(`no IPv4 record for ${host}`);
    }
    hostToIp.set(host, ipv4.address);
    return ipv4.address;
  };
};

const bunCAresLookup = (host: string): Promise<readonly { address: string; family: number }[]> =>
  Bun.dns.lookup(host, { backend: "c-ares" });

/* v8 ignore start */
if (import.meta.main) {
  const resolvingFetch = createResolvingFetch(fetch, createCachedHostLookup(bunCAresLookup));
  const config = buildDefaultConfig(new Date(), resolvingFetch);
  const result = await deleteDailyRaceEntriesChunked(config);
  console.log(
    `delete-daily-race-entries-chunk: stopped=${result.stoppedReason}, deleted=${result.totalDeleted}, lastRowid=${result.finalSinceRowid}`,
  );
}
/* v8 ignore stop */
