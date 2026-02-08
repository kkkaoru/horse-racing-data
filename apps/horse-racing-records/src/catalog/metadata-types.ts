// Run with: bun
// Iceberg table metadata type definitions

interface IcebergSchema {
  readonly type: string;
  readonly "schema-id": number;
  readonly fields: ReadonlyArray<IcebergSchemaField>;
}

interface IcebergSchemaField {
  readonly id: number;
  readonly name: string;
  readonly required: boolean;
  readonly type: string | IcebergNestedType;
}

interface IcebergNestedType {
  readonly type: string;
  readonly "element-id"?: number;
  readonly element?: string | IcebergNestedType;
  readonly "element-required"?: boolean;
  readonly fields?: ReadonlyArray<IcebergSchemaField>;
}

interface PartitionSpec {
  readonly "spec-id": number;
  readonly fields: ReadonlyArray<PartitionField>;
}

interface PartitionField {
  readonly name: string;
  readonly transform: string;
  readonly "source-id": number;
  readonly "field-id": number;
}

interface SortOrder {
  readonly "order-id": number;
  readonly fields: ReadonlyArray<unknown>;
}

interface SnapshotJson {
  readonly "snapshot-id": string;
  readonly "parent-snapshot-id"?: string;
  readonly "sequence-number": number;
  readonly "timestamp-ms": number;
  readonly summary: SnapshotSummary;
  readonly "manifest-list": string;
  readonly "schema-id": number;
}

interface SnapshotSummary {
  readonly operation: string;
  readonly [key: string]: string;
}

interface TableMetadata {
  readonly "format-version": number;
  readonly "table-uuid": string;
  readonly location: string;
  readonly "last-sequence-number": number;
  readonly "last-updated-ms": number;
  readonly "last-column-id": number;
  readonly "current-schema-id": number;
  readonly schemas: ReadonlyArray<IcebergSchema>;
  readonly "default-spec-id": number;
  readonly "partition-specs": ReadonlyArray<PartitionSpec>;
  readonly "last-partition-id": number;
  readonly "default-sort-order-id": number;
  readonly "sort-orders": ReadonlyArray<SortOrder>;
  readonly "current-snapshot-id"?: string;
  readonly snapshots?: ReadonlyArray<SnapshotJson>;
  readonly "snapshot-log"?: ReadonlyArray<SnapshotLogEntry>;
  readonly properties?: Record<string, string>;
}

interface SnapshotLogEntry {
  readonly "timestamp-ms": number;
  readonly "snapshot-id": string;
}

interface LoadTableResponse {
  readonly "metadata-location": string;
  readonly metadata: TableMetadata;
}

interface AssertRefSnapshotIdRequirement {
  readonly type: "assert-ref-snapshot-id";
  readonly ref: string;
  readonly "snapshot-id": string;
}

type TableRequirement = AssertRefSnapshotIdRequirement;

interface AddSnapshotUpdate {
  readonly action: "add-snapshot";
  readonly snapshot: SnapshotJson;
}

interface SetSnapshotRefUpdate {
  readonly action: "set-snapshot-ref";
  readonly "ref-name": string;
  readonly type: string;
  readonly "snapshot-id": string;
}

type TableUpdate = AddSnapshotUpdate | SetSnapshotRefUpdate;

interface CommitTableRequest {
  readonly requirements: ReadonlyArray<TableRequirement>;
  readonly updates: ReadonlyArray<TableUpdate>;
}

export type {
  IcebergSchema,
  IcebergSchemaField,
  IcebergNestedType,
  PartitionSpec,
  PartitionField,
  SortOrder,
  SnapshotJson,
  SnapshotSummary,
  TableMetadata,
  SnapshotLogEntry,
  LoadTableResponse,
  AssertRefSnapshotIdRequirement,
  TableRequirement,
  AddSnapshotUpdate,
  SetSnapshotRefUpdate,
  TableUpdate,
  CommitTableRequest,
};
