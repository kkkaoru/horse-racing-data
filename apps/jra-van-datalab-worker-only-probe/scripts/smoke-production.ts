// Run with bun. Credential-safe production smoke test without private local oracle files.
const baseUrl =
  process.env.JRA_VAN_WORKER_BASE_URL ??
  "https://jra-van-datalab-worker-only-probe.kaoru.workers.dev";
const token = process.env.JRA_VAN_WORKER_API_TOKEN;
if (!token) throw new Error("Worker API token is required");

const authorization = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
const compatibilityResponse = await fetch(`${baseUrl}/compatibility`);
const compatibility = (await compatibilityResponse.json()) as {
  attestation: { coreVersion: string; verified: boolean };
  deploymentCompatibility: boolean;
  fullCompatibility: boolean;
  rustCoreVersion: string;
};
if (
  !compatibilityResponse.ok ||
  !compatibility.attestation.verified ||
  compatibility.attestation.coreVersion !== "0500-private-core-v3.3" ||
  !compatibility.deploymentCompatibility ||
  compatibility.fullCompatibility ||
  compatibility.rustCoreVersion !== "0500-private-core-v3.3"
)
  throw new Error("Production compatibility attestation failed");

// JRA-VAN only keeps realtime data for recent days, so a pinned historical key
// eventually returns an error envelope. Probe the last week (JST) and require
// at least one day with a successful, non-empty realtime response.
const REALTIME_LOOKBACK_DAYS = 8;
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const realtimeKeys = Array.from({ length: REALTIME_LOOKBACK_DAYS }, (_, daysAgo) =>
  new Date(Date.now() + JST_OFFSET_MS - daysAgo * DAY_MS)
    .toISOString()
    .slice(0, 10)
    .replaceAll("-", ""),
);
const realtimeResults = await Promise.all(
  realtimeKeys.map(async (key) => {
    const response = await fetch(`${baseUrl}/realtime`, {
      body: JSON.stringify({ dataSpec: "0B14", key }),
      headers: authorization,
      method: "POST",
    });
    const body = (await response.json()) as { records?: string[]; status?: number };
    return { key, ok: response.ok && body.status === 0, records: body.records?.length ?? 0 };
  }),
);
const realtime = realtimeResults.find((result) => result.ok && result.records > 0);
if (realtime === undefined)
  throw new Error(
    `Production realtime smoke test failed results=${JSON.stringify(realtimeResults)}`,
  );

const unauthorized = await fetch(`${baseUrl}/realtime`, {
  body: JSON.stringify({ dataSpec: "0B14", key: "20260830" }),
  headers: { "Content-Type": "application/json" },
  method: "POST",
});
if (unauthorized.status !== 401) throw new Error("Production authentication boundary failed");

console.log(
  JSON.stringify({
    coreVersion: compatibility.rustCoreVersion,
    deploymentCompatibility: true,
    realtimeKey: realtime.key,
    realtimeRecords: realtime.records,
    unauthorizedStatus: unauthorized.status,
    verified: true,
  }),
);
