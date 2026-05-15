// Run with: imported from build-finish-position-features.ts (bun runtime)

export type FeatureCategory = "all" | "ban-ei" | "jra" | "nar";

export type FeatureTarget = "local" | "neon";

export interface BuildOptions {
  category: FeatureCategory;
  dryRun: boolean;
  featureSchemaVersion: string;
  fromDate: string;
  target: FeatureTarget;
  toDate: string;
}

export interface InsertBatchRow {
  insertedCount: number;
  category: FeatureCategory;
  fromDate: string;
  toDate: string;
}
