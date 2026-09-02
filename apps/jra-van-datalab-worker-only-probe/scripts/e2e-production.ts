// Run with bun. Credential-safe E2E against the deployed Worker and official COM cache oracle.

import { decodeJvFile } from "../src/jvfile";

interface CourseResponse {
  encoding: "base64";
  explanation: string;
  image: string;
  path: string;
}

interface MovieResponse {
  encoding: "base64";
  keys: string[];
  status: number;
}

interface RealtimeResponse {
  encoding: "base64";
  records: string[];
  status: number;
}

interface StreamEvent {
  data?: string;
  decodedBytes?: number;
  event: "open" | "file" | "record" | "close";
  fileBytes?: number;
  files?: number;
  filename?: string;
  readCount?: number;
  records?: number;
}

const BASE_URL: string =
  process.env.JRA_VAN_WORKER_BASE_URL ??
  "https://jra-van-datalab-worker-only-probe.kaoru.workers.dev";
const token = process.env.JRA_VAN_WORKER_API_TOKEN;
if (!token) throw new Error("JRA_VAN_WORKER_API_TOKEN is required");

const oraclePath = new URL(
  "../../jra-van-datalab-wine-demo/.native-cache/prefix/drive_c/JVData/cache/JGDW2026083020260829112816.jvd",
  import.meta.url,
);
const oracleFile = new Uint8Array(await Bun.file(oraclePath).arrayBuffer());
const oracleDecoded = await decodeJvFile(oracleFile);
const requestBody = JSON.stringify({
  dataSpec: "RACE",
  from: "20260829000000",
  to: "20260830235959",
});

const response = await fetch(`${BASE_URL}/acquire/stream`, {
  body: requestBody,
  headers: {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  },
  method: "POST",
});
if (!response.ok) throw new Error(`Worker E2E failed with HTTP ${response.status}`);
if (response.headers.get("Content-Type") !== "application/x-ndjson")
  throw new Error("Worker E2E returned an unexpected content type");

const events = (await response.text())
  .trimEnd()
  .split("\n")
  .map((line) => JSON.parse(line) as StreamEvent);
const open = events[0];
const file = events.find(({ event }) => event === "file");
const close = events.at(-1);
const recordEvents = events.filter(({ event }) => event === "record");
if (open?.event !== "open" || open.readCount !== 1) throw new Error("JVOpen E2E mismatch");
if (
  file?.fileBytes !== oracleFile.length ||
  file.decodedBytes !== oracleDecoded.length ||
  file.filename !== "JGDW2026083020260829112816.jvd"
)
  throw new Error("JV file metadata does not match the official COM oracle");
if (close?.event !== "close" || close.files !== 1 || close.records !== recordEvents.length)
  throw new Error("JVClose E2E mismatch");

const decodedRecords = recordEvents.map(({ data }) => {
  if (data === undefined) throw new Error("Record event has no payload");
  return Uint8Array.from(atob(data), (value) => value.charCodeAt(0));
});
const streamed = new Uint8Array(decodedRecords.reduce((sum, record) => sum + record.length, 0));
let offset = 0;
for (const record of decodedRecords) {
  streamed.set(record, offset);
  offset += record.length;
}
if (!Bun.deepEquals(streamed, oracleDecoded))
  throw new Error("Streamed JV-Data differs from the official COM oracle");

const realtimeOraclePath = new URL(
  "../../jra-van-datalab-cloudflare-demo/.migration/official-rt-decoded.bin",
  import.meta.url,
);
const realtimeOracle = new Uint8Array(await Bun.file(realtimeOraclePath).arrayBuffer());
const realtimeResponse = await fetch(`${BASE_URL}/realtime`, {
  body: JSON.stringify({ dataSpec: "0B14", key: "20260830" }),
  headers: {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  },
  method: "POST",
});
if (!realtimeResponse.ok)
  throw new Error(`Worker realtime E2E failed with HTTP ${realtimeResponse.status}`);
const realtime = (await realtimeResponse.json()) as RealtimeResponse;
const realtimeRecords = realtime.records.map((data) =>
  Uint8Array.from(atob(data), (value) => value.charCodeAt(0)),
);
const realtimeDecoded = new Uint8Array(
  realtimeRecords.reduce((sum, record) => sum + record.length, 0),
);
offset = 0;
for (const record of realtimeRecords) {
  realtimeDecoded.set(record, offset);
  offset += record.length;
}
if (realtime.status !== 0 || !Bun.deepEquals(realtimeDecoded, realtimeOracle))
  throw new Error("Worker realtime data differs from the official JVRTOpen oracle");

const courseOraclePath = new URL(
  "../../jra-van-datalab-cloudflare-demo/.migration/official-course-record.bin",
  import.meta.url,
);
const courseImageOraclePath = new URL(
  "../../jra-van-datalab-cloudflare-demo/.migration/official-course.gif",
  import.meta.url,
);
const courseOracle = new Uint8Array(await Bun.file(courseOraclePath).arrayBuffer());
const courseImageOracle = new Uint8Array(await Bun.file(courseImageOraclePath).arrayBuffer());
const courseResponse = await fetch(`${BASE_URL}/course`, {
  body: JSON.stringify({ key: "9999999905240011" }),
  headers: {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  },
  method: "POST",
});
if (!courseResponse.ok)
  throw new Error(`Worker course E2E failed with HTTP ${courseResponse.status}`);
const course = (await courseResponse.json()) as CourseResponse;
const courseExplanation = Uint8Array.from(atob(course.explanation), (value) => value.charCodeAt(0));
const courseImage = Uint8Array.from(atob(course.image), (value) => value.charCodeAt(0));
const coursePath = new TextDecoder("ascii").decode(courseOracle.subarray(0, 192)).trimEnd();
if (
  course.path !== coursePath ||
  !Bun.deepEquals(courseExplanation, courseOracle.slice(192, -2)) ||
  !Bun.deepEquals(courseImage, courseImageOracle)
)
  throw new Error("Worker course data differs from the official JVCourseFile oracle");

const movieOraclePath = new URL(
  "../../jra-van-datalab-cloudflare-demo/.migration/official-movie-list.bin",
  import.meta.url,
);
const movieOracle = new TextDecoder("ascii")
  .decode(new Uint8Array(await Bun.file(movieOraclePath).arrayBuffer()))
  .split("\r\n")
  .slice(2)
  .filter((key) => key.length > 0);
const movieResponse = await fetch(`${BASE_URL}/movies`, {
  body: JSON.stringify({ movieType: "11", searchKey: "20260830" }),
  headers: {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  },
  method: "POST",
});
if (!movieResponse.ok)
  throw new Error(`Worker movie-list E2E failed with HTTP ${movieResponse.status}`);
const movies = (await movieResponse.json()) as MovieResponse;
const movieKeys = movies.keys.map((key) => atob(key));
if (movies.status !== 0 || !Bun.deepEquals(movieKeys, movieOracle))
  throw new Error("Worker movie list differs from the official JVMVOpen oracle");

const compatibilityResponse = await fetch(`${BASE_URL}/compatibility`);
const compatibility = (await compatibilityResponse.json()) as {
  attestation: { coreVersion: string; verified: boolean };
  deployedMethods: unknown[];
  deploymentCompatibility: boolean;
  events: unknown[];
  fullCompatibility: boolean;
  implementation: string;
  methods: unknown[];
  properties: unknown[];
  rustCoreVersion: string;
};
if (
  !compatibilityResponse.ok ||
  compatibility.attestation.coreVersion !== "0500-private-core-v2" ||
  !compatibility.attestation.verified ||
  !compatibility.deploymentCompatibility ||
  compatibility.deployedMethods.length !== 18 ||
  compatibility.fullCompatibility ||
  compatibility.implementation !== "rust-wasm-static-module" ||
  compatibility.methods.length !== 27 ||
  compatibility.properties.length !== 9 ||
  compatibility.events.length !== 7 ||
  compatibility.rustCoreVersion !== "0500-private-core-v2"
)
  throw new Error("Deployed compatibility ledger does not match the official API surface");

const unauthorized = await fetch(`${BASE_URL}/acquire/stream`, {
  body: requestBody,
  headers: { "Content-Type": "application/json" },
  method: "POST",
});
if (unauthorized.status !== 401) throw new Error("Worker E2E authentication boundary failed");

console.log(
  JSON.stringify({
    courseImageBytes: courseImage.length,
    courseOfficialOracleMatch: true,
    decodedBytes: streamed.length,
    fileBytes: oracleFile.length,
    deploymentCompatibility: compatibility.deploymentCompatibility,
    files: close.files,
    fullCompatibility: compatibility.fullCompatibility,
    officialApiMethodsClassified: compatibility.methods.length,
    movieListOfficialOracleMatch: true,
    movieListRecords: movieKeys.length,
    officialOracleMatch: true,
    realtimeBytes: realtimeDecoded.length,
    realtimeOfficialOracleMatch: true,
    realtimeRecords: realtimeRecords.length,
    records: close.records,
    rustCoreVersion: compatibility.rustCoreVersion,
    status: response.status,
    unauthorizedStatus: unauthorized.status,
    wasmAttestation: compatibility.attestation.verified,
  }),
);
