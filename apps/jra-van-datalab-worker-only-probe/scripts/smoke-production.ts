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
  compatibility.attestation.coreVersion !== "0500-private-core-v2" ||
  !compatibility.deploymentCompatibility ||
  compatibility.fullCompatibility ||
  compatibility.rustCoreVersion !== "0500-private-core-v2"
)
  throw new Error("Production compatibility attestation failed");

const realtimeResponse = await fetch(`${baseUrl}/realtime`, {
  body: JSON.stringify({ dataSpec: "0B14", key: "20260830" }),
  headers: authorization,
  method: "POST",
});
const realtime = (await realtimeResponse.json()) as { records?: string[]; status?: number };
if (!realtimeResponse.ok || realtime.status !== 0 || realtime.records?.length !== 22)
  throw new Error("Production realtime smoke test failed");

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
    realtimeRecords: realtime.records.length,
    unauthorizedStatus: unauthorized.status,
    verified: true,
  }),
);
