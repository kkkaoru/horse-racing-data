import type { PremiumPaddockBulletin } from "./premium-race";

export interface TimestampedPremiumPaddockBulletin extends PremiumPaddockBulletin {
  fetchedAt: string;
}

export interface PremiumPaddockRunnerSignal {
  commentAvailable: boolean;
  evaluationAvailable: boolean;
  groupKey: "favorite" | "value" | null;
  selected: boolean;
  snapshotFetchedAt: string;
}

const parseTimestamp = (value: string, label: string): number => {
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    throw new RangeError(`${label} must be an ISO timestamp`);
  }
  return timestamp;
};

/**
 * Builds inference-safe paddock signals without interpreting free text.
 *
 * A complete, strictly pre-start payload makes absence from the bulletin a
 * meaningful `selected=false`. Incomplete payloads fail closed to `null`, so a
 * missing premium fetch is never confused with a negative paddock assessment.
 */
export const buildPremiumPaddockRunnerSignals = (
  expectedHorseNumbers: readonly string[],
  bulletins: readonly TimestampedPremiumPaddockBulletin[],
  scheduledStart: string,
  snapshotComplete: boolean,
): Readonly<Record<string, PremiumPaddockRunnerSignal | null>> => {
  const start = parseTimestamp(scheduledStart, "scheduledStart");
  const expected = new Set(expectedHorseNumbers);
  if (expected.size !== expectedHorseNumbers.length) {
    throw new RangeError("expectedHorseNumbers must be unique");
  }
  if (!snapshotComplete) {
    return Object.fromEntries(expectedHorseNumbers.map((horseNumber) => [horseNumber, null]));
  }

  const latestByHorse = new Map<
    string,
    { bulletin: TimestampedPremiumPaddockBulletin; fetchedAt: number }
  >();
  let latestSnapshot: { fetchedAt: string; timestamp: number } | null = null;
  for (const bulletin of bulletins) {
    const fetchedAt = parseTimestamp(bulletin.fetchedAt, "bulletin.fetchedAt");
    if (fetchedAt >= start) {
      continue;
    }
    if (latestSnapshot === null || fetchedAt > latestSnapshot.timestamp) {
      latestSnapshot = { fetchedAt: bulletin.fetchedAt, timestamp: fetchedAt };
    }
    if (!expected.has(bulletin.horseNumber)) {
      continue;
    }
    const current = latestByHorse.get(bulletin.horseNumber);
    if (current === undefined || fetchedAt > current.fetchedAt) {
      latestByHorse.set(bulletin.horseNumber, { bulletin, fetchedAt });
    }
  }
  if (latestSnapshot === null) {
    return Object.fromEntries(expectedHorseNumbers.map((horseNumber) => [horseNumber, null]));
  }

  return Object.fromEntries(
    expectedHorseNumbers.map((horseNumber) => {
      const selected = latestByHorse.get(horseNumber)?.bulletin;
      return [
        horseNumber,
        {
          commentAvailable: selected?.commentText?.trim() !== "" && selected?.commentText != null,
          evaluationAvailable:
            selected?.evaluationText?.trim() !== "" && selected?.evaluationText != null,
          groupKey: selected?.groupKey ?? null,
          selected: selected !== undefined,
          snapshotFetchedAt: latestSnapshot.fetchedAt,
        },
      ];
    }),
  );
};
