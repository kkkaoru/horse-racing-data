// Run with: bun
// Iceberg snapshot construction utilities

import type { SnapshotJson, SnapshotSummary } from "../catalog/metadata-types.ts";

const SNAPSHOT_ID_BYTE_LENGTH = 8;
const SAFE_INT_HIGH_MASK = 0x001fffff;
const UINT32_MULTIPLIER = 0x100000000;

const generateSnapshotId = (): string => {
  const bytes = new Uint8Array(SNAPSHOT_ID_BYTE_LENGTH);
  crypto.getRandomValues(bytes);
  const view = new DataView(bytes.buffer);
  const high = view.getUint32(0) & SAFE_INT_HIGH_MASK;
  const low = view.getUint32(4);
  return String(high * UINT32_MULTIPLIER + low);
};

interface BuildSnapshotArgs {
  readonly snapshotId: string;
  readonly parentSnapshotId?: string;
  readonly sequenceNumber: number;
  readonly manifestListLocation: string;
  readonly schemaId: number;
  readonly deletedRowCount: number;
}

const buildDeleteSnapshot = (args: BuildSnapshotArgs): SnapshotJson => {
  const summary: SnapshotSummary = {
    operation: "delete",
    "deleted-data-files": "0",
    "added-delete-files": "1",
    "deleted-records": String(args.deletedRowCount),
  };

  const snapshot: SnapshotJson = {
    "snapshot-id": args.snapshotId,
    "sequence-number": args.sequenceNumber,
    "timestamp-ms": Date.now(),
    summary,
    "manifest-list": args.manifestListLocation,
    "schema-id": args.schemaId,
  };

  if (args.parentSnapshotId !== undefined) {
    return { ...snapshot, "parent-snapshot-id": args.parentSnapshotId };
  }

  return snapshot;
};

export { generateSnapshotId, buildDeleteSnapshot };
export type { BuildSnapshotArgs };
