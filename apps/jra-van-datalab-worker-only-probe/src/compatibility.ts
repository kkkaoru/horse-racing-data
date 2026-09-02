// Run with bun. Exhaustive JV-Link 4.9.0.1/5.0.0 API compatibility ledger.

export type CompatibilityLevel =
  | "implemented"
  | "worker-equivalent"
  | "host-ui-only"
  | "protocol-research-required";

export interface CompatibilityEntry {
  dispid?: number;
  level: CompatibilityLevel;
  name: string;
  note: string;
}

export const JVLINK_PROPERTIES: readonly CompatibilityEntry[] = [
  { level: "worker-equivalent", name: "m_saveflag", note: "Select streaming or persistence." },
  { level: "worker-equivalent", name: "m_savepath", note: "Maps to an R2 object prefix." },
  { level: "worker-equivalent", name: "m_servicekey", note: "Stored as a Worker secret." },
  { level: "implemented", name: "m_JVLinkVersion", note: "Wire compatibility version 0500." },
  {
    level: "implemented",
    name: "m_TotalReadFilesize",
    note: "Derived from all file-list FS values.",
  },
  {
    level: "implemented",
    name: "m_CurrentReadFilesize",
    note: "Emitted by each NDJSON file event.",
  },
  {
    level: "implemented",
    name: "m_CurrentFileTimeStamp",
    note: "Available from file-list metadata and filename.",
  },
  { level: "host-ui-only", name: "ParentHWnd", note: "Workers have no desktop window." },
  { level: "implemented", name: "m_payflag", note: "Decoded by the bootstrap response." },
] as const;

export const JVLINK_METHODS: readonly CompatibilityEntry[] = [
  {
    dispid: 4,
    level: "implemented",
    name: "JVInit",
    note: "Creates fresh authenticated session data.",
  },
  {
    dispid: 6,
    level: "host-ui-only",
    name: "JVSetUIProperties",
    note: "Configuration is supplied with secrets and deployment settings.",
  },
  {
    dispid: 13,
    level: "worker-equivalent",
    name: "JVSetServiceKey",
    note: "Wrangler secret provisioning replaces registry mutation.",
  },
  {
    dispid: 15,
    level: "worker-equivalent",
    name: "JVSetSaveFlag",
    note: "The endpoint selects streaming or persistence.",
  },
  {
    dispid: 1,
    level: "worker-equivalent",
    name: "JVSetSavePath",
    note: "An R2 prefix replaces a local filesystem path.",
  },
  {
    dispid: 34,
    level: "worker-equivalent",
    name: "JVSetPayFlag",
    note: "The bootstrap pay flag controls Worker response behavior without a desktop dialog.",
  },
  {
    dispid: 7,
    level: "implemented",
    name: "JVOpen",
    note: "Stored-data bootstrap, complete file list, and all ordered files.",
  },
  {
    dispid: 10,
    level: "implemented",
    name: "JVRTOpen",
    note: "APPL=0003 authorization and GateServlet records match the official SDK oracle.",
  },
  {
    dispid: 8,
    level: "worker-equivalent",
    name: "JVStatus",
    note: "Streaming file events report deterministic completed-file progress.",
  },
  {
    dispid: 9,
    level: "implemented",
    name: "JVRead",
    note: "Base64 NDJSON records preserve exact CP932 bytes and file boundaries.",
  },
  {
    dispid: 22,
    level: "implemented",
    name: "JVGets",
    note: "Base64 NDJSON records are lossless byte arrays.",
  },
  {
    dispid: 19,
    level: "worker-equivalent",
    name: "JVSkip",
    note: "Consumers skip the remainder of the current filename in the stream.",
  },
  {
    dispid: 11,
    level: "worker-equivalent",
    name: "JVCancel",
    note: "Canceling the response stream cancels acquisition.",
  },
  {
    dispid: 5,
    level: "worker-equivalent",
    name: "JVClose",
    note: "The close event terminates the stateless session.",
  },
  {
    dispid: 12,
    level: "worker-equivalent",
    name: "JVFiledelete",
    note: "Streaming has no local cache; persisted objects use R2 deletion.",
  },
  {
    dispid: 25,
    level: "protocol-research-required",
    name: "JVFukuFile",
    note: "Content/image oracle coverage is pending.",
  },
  {
    dispid: 26,
    level: "protocol-research-required",
    name: "JVFuku",
    note: "Content/image oracle coverage is pending.",
  },
  {
    dispid: 24,
    level: "protocol-research-required",
    name: "JVMVCheck",
    note: "Racing Viewer authorization contract is separately permissioned.",
  },
  {
    dispid: 36,
    level: "protocol-research-required",
    name: "JVMVCheckWithType",
    note: "Racing Viewer authorization contract is separately permissioned.",
  },
  {
    dispid: 23,
    level: "host-ui-only",
    name: "JVMVPlay",
    note: "The SDK method launches a desktop browser.",
  },
  {
    dispid: 29,
    level: "host-ui-only",
    name: "JVMVPlayWithType",
    note: "The SDK method launches a desktop browser.",
  },
  {
    dispid: 27,
    level: "implemented",
    name: "JVMVOpen",
    note: "APPL=0006 and 0B8C movie-list records match the authorized official oracle.",
  },
  {
    dispid: 28,
    level: "implemented",
    name: "JVMVRead",
    note: "Returns every ordered eighteen-byte movie key from the 0B8C response.",
  },
  {
    dispid: 30,
    level: "implemented",
    name: "JVCourseFile",
    note: "Returns the exact GIF and CP932 explanation from the 0B72 oracle contract.",
  },
  {
    dispid: 31,
    level: "implemented",
    name: "JVCourseFile2",
    note: "Returns the exact GIF from the 0B72 oracle contract.",
  },
  {
    dispid: 33,
    level: "protocol-research-required",
    name: "JVWatchEvent",
    note: "Long-lived event transport oracle coverage is pending.",
  },
  {
    dispid: 35,
    level: "protocol-research-required",
    name: "JVWatchEventClose",
    note: "Depends on JVWatchEvent coverage.",
  },
] as const;

export const JVLINK_EVENTS: readonly string[] = [
  "JVEvtPay",
  "JVEvtJockeyChange",
  "JVEvtWeather",
  "JVEvtCourseChange",
  "JVEvtAvoid",
  "JVEvtTimeChange",
  "JVEvtWeight",
] as const;

export const JVLINK_DEPLOYED_METHODS: readonly string[] = [
  "JVInit",
  "JVSetServiceKey",
  "JVSetSaveFlag",
  "JVSetSavePath",
  "JVSetPayFlag",
  "JVOpen",
  "JVRTOpen",
  "JVStatus",
  "JVRead",
  "JVGets",
  "JVSkip",
  "JVCancel",
  "JVClose",
  "JVFiledelete",
  "JVMVOpen",
  "JVMVRead",
  "JVCourseFile",
  "JVCourseFile2",
] as const;

const METHODS_BY_NAME = new Map(JVLINK_METHODS.map((entry) => [entry.name, entry]));
export const JVLINK_DEPLOYMENT_COMPATIBILITY: boolean = JVLINK_DEPLOYED_METHODS.every((name) => {
  const level = METHODS_BY_NAME.get(name)!.level;
  return level === "implemented" || level === "worker-equivalent";
});

export const JVLINK_FULL_COMPATIBILITY: boolean = [...JVLINK_PROPERTIES, ...JVLINK_METHODS].every(
  ({ level }) => level !== "protocol-research-required",
);
