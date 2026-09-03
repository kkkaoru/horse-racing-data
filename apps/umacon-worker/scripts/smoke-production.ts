// Run with bun. Verify the deployed Worker without requiring local oracle artifacts.

const baseUrl: string = process.env.UMMACON_WORKER_URL ?? "https://umacon-worker.kaoru.workers.dev";
const token: string | undefined = process.env.UMMACON_WORKER_API_TOKEN;
if (!token) throw new Error("UMMACON_WORKER_API_TOKEN is required");

const health: Response = await fetch(`${baseUrl}/health`);
if (!health.ok) throw new Error(`Health check failed (${health.status})`);

const unauthorized: Response = await fetch(`${baseUrl}/compatibility`);
if (unauthorized.status !== 401) throw new Error("Compatibility endpoint is not protected");

const headers: HeadersInit = { Authorization: `Bearer ${token}` };
const compatibility: Response = await fetch(`${baseUrl}/compatibility`, { headers });
if (!compatibility.ok) throw new Error(`Compatibility check failed (${compatibility.status})`);
const compatibilityBody: unknown = await compatibility.json();
if (
  typeof compatibilityBody !== "object" ||
  compatibilityBody === null ||
  !("deploymentCompatibility" in compatibilityBody) ||
  compatibilityBody.deploymentCompatibility !== true ||
  !("attestation" in compatibilityBody) ||
  typeof compatibilityBody.attestation !== "object" ||
  compatibilityBody.attestation === null ||
  !("verified" in compatibilityBody.attestation) ||
  compatibilityBody.attestation.verified !== true
)
  throw new Error("Compatibility response is not attested");

const acquisition: Response = await fetch(`${baseUrl}/acquire/stream`, {
  body: JSON.stringify({ dataSpec: "RACE", fromTime: "20260902000000", option: 1 }),
  headers: { ...headers, "Content-Type": "application/json" },
  method: "POST",
});
if (!acquisition.ok || acquisition.body === null) {
  const stage: string = acquisition.headers.get("X-NV-Link-Failure-Stage") ?? "unknown";
  const upstream: string = acquisition.headers.get("X-NV-Link-Upstream-Status") ?? "network";
  const socket: string = acquisition.headers.get("X-NV-Link-Socket-Stage") ?? "none";
  const socketBytes: string = acquisition.headers.get("X-NV-Link-Socket-Bytes") ?? "unknown";
  throw new Error(
    `Acquisition failed (${acquisition.status}, stage=${stage}, upstream=${upstream}, socket=${socket}, socketBytes=${socketBytes})`,
  );
}

const reader: ReadableStreamDefaultReader<Uint8Array> = acquisition.body.getReader();
const decoder: TextDecoder = new TextDecoder();
let pending: string = "";
let files: number = 0;
let records: number = 0;
let foundRecord: boolean = false;
while (!foundRecord) {
  const next = await reader.read();
  if (next.done) break;
  pending += decoder.decode(next.value, { stream: true });
  const lines: string[] = pending.split("\n");
  pending = lines.pop() ?? "";
  for (const line of lines) {
    if (!line) continue;
    const event: unknown = JSON.parse(line);
    if (typeof event !== "object" || event === null || !("event" in event)) continue;
    if (event.event === "file") files += 1;
    if (
      event.event === "record" &&
      "data" in event &&
      typeof event.data === "string" &&
      atob(event.data).length > 2
    ) {
      records += 1;
      foundRecord = true;
      break;
    }
  }
}
await reader.cancel();
if (!foundRecord) throw new Error("Production acquisition returned no records");
console.log(JSON.stringify({ attestation: "verified", files, health: "ok", records }));
