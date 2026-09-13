// Run with bun. Durable, fail-open change detection before starting MLflow.
export interface SyncCheckpoint {
  fingerprint: string;
  syncedAt: number;
}

export interface SyncDay {
  date: string;
  fingerprint: string;
}

export interface SyncCheckpointStore {
  get(key: string): Promise<SyncCheckpoint | undefined>;
  put(key: string, value: SyncCheckpoint): Promise<void>;
}

export interface SyncChangedDaysInput {
  dateFrom: string;
  dateTo: string;
  now: number;
  probe: () => Promise<SyncDay[]>;
  store: SyncCheckpointStore;
  sync: (from: string, to: string) => Promise<void>;
}

export const RECONCILE_INTERVAL_MS: number = 60 * 60 * 1000;
const CHECKPOINT_PREFIX: string = "preview-source-v1:";

const needsSync = (checkpoint: SyncCheckpoint | undefined, day: SyncDay, now: number): boolean =>
  checkpoint === undefined ||
  checkpoint.fingerprint !== day.fingerprint ||
  now < checkpoint.syncedAt ||
  now - checkpoint.syncedAt >= RECONCILE_INTERVAL_MS;

export const syncChangedDays = async (input: SyncChangedDaysInput): Promise<void> => {
  const days = await input.probe().catch((error: unknown) => {
    console.warn("[mlflow-sync] source probe failed; reconciling full window", String(error));
    return null;
  });
  if (days === null) {
    await input.sync(input.dateFrom, input.dateTo);
    return;
  }
  // Serial execution bounds memory and avoids simultaneous MLflow backend writes.
  await days.reduce(async (previous, day) => {
    await previous;
    const key = `${CHECKPOINT_PREFIX}${day.date}`;
    const checkpoint = await input.store.get(key);
    if (!needsSync(checkpoint, day, input.now)) {
      console.log(`[mlflow-sync] unchanged date=${day.date}; Container not started`);
      return;
    }
    await input.sync(day.date, day.date);
    await input.store.put(key, { fingerprint: day.fingerprint, syncedAt: input.now });
  }, Promise.resolve());
};
