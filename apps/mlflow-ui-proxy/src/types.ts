// Run with bun.
export interface MlflowSyncResult {
  exitCode: number;
  stderr: string;
  stdout: string;
}

export interface MlflowContainerStub {
  fetch(request: Request): Promise<Response>;
  syncProductionPreview(dateFrom: string, dateTo: string): Promise<MlflowSyncResult>;
}

export interface MlflowContainerNamespace {
  getByName(name: string): MlflowContainerStub;
}

export interface Env {
  MLFLOW_CONTAINER: MlflowContainerNamespace;
  MLFLOW_UI_USERNAME: string;
  MLFLOW_UI_PASSWORD: string;
  HORSE_RACING_MLFLOW_BACKEND_URI: string;
  NEON_PRIMARY_URL: string;
  R2_ACCESS_KEY_ID: string;
  R2_ACCOUNT_ID: string;
  R2_SECRET_ACCESS_KEY: string;
  HORSE_RACING_MLFLOW_R2_BUCKET: string;
  HORSE_RACING_MLFLOW_R2_PREFIX: string;
}
