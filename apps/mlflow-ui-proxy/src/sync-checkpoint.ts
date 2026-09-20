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

interface SyncRange {
  first: SyncDay;
  last: SyncDay;
  days: SyncDay[];
}

export const RECONCILE_INTERVAL_MS: number = 60 * 60 * 1000;
const CHECKPOINT_PREFIX: string = "preview-source-v1:";
const DAY_MS: number = 24 * 60 * 60 * 1000;

const needsSync = (checkpoint: SyncCheckpoint | undefined, day: SyncDay, now: number): boolean =>
  checkpoint === undefined ||
  checkpoint.fingerprint !== day.fingerprint ||
  now < checkpoint.syncedAt ||
  now - checkpoint.syncedAt >= RECONCILE_INTERVAL_MS;

const dateTimestamp = (date: string): number =>
  Date.UTC(Number(date.slice(0, 4)), Number(date.slice(4, 6)) - 1, Number(date.slice(6, 8)));

const appendRange = (ranges: SyncRange[], day: SyncDay): SyncRange[] => {
  const last: SyncRange | undefined = ranges.at(-1);
  if (last !== undefined && dateTimestamp(day.date) - dateTimestamp(last.last.date) === DAY_MS) {
    last.last = day;
    last.days.push(day);
  } else {
    ranges.push({ first: day, last: day, days: [day] });
  }
  return ranges;
};

const findChangedDays = (days: SyncDay[], input: SyncChangedDaysInput): Promise<SyncDay[]> =>
  days.reduce(async (previous, day) => {
    const changed: SyncDay[] = await previous;
    const checkpoint: SyncCheckpoint | undefined = await input.store.get(
      `${CHECKPOINT_PREFIX}${day.date}`,
    );
    if (needsSync(checkpoint, day, input.now)) changed.push(day);
    else console.log(`[mlflow-sync] unchanged date=${day.date}; Container not started`);
    return changed;
  }, Promise.resolve<SyncDay[]>([]));

const syncRange = async (range: SyncRange, input: SyncChangedDaysInput): Promise<void> => {
  await input.sync(range.first.date, range.last.date);
  // A partially failed CLI must not certify any date in its range as synced.
  await range.days.reduce(async (previous, day) => {
    await previous;
    await input.store.put(`${CHECKPOINT_PREFIX}${day.date}`, {
      fingerprint: day.fingerprint,
      syncedAt: input.now,
    });
  }, Promise.resolve());
};

export const syncChangedDays = async (input: SyncChangedDaysInput): Promise<void> => {
  const days = await input.probe().catch((error: unknown) => {
    console.warn("[mlflow-sync] source probe failed; reconciling full window", String(error));
    return null;
  });
  if (days === null) {
    await input.sync(input.dateFrom, input.dateTo);
    return;
  }
  const changed: SyncDay[] = await findChangedDays(days, input);
  const ranges: SyncRange[] = changed.reduce<SyncRange[]>(appendRange, []);
  // Reuse Python imports/client connections across adjacent dates, never gaps.
  // Serial ranges still bound memory and avoid simultaneous backend writes.
  await ranges.reduce(async (previous, range) => {
    await previous;
    await syncRange(range, input);
  }, Promise.resolve());
};
