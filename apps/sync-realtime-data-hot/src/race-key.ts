export type RealtimeSource = "jra" | "nar";

const RACE_KEY_PATTERN = /^(jra|nar):(\d{4}):(\d{4}):[0-9A-Z]{2}:\d{2}$/u;

export const extractYyyymmddFromRaceKey = (raceKey: string): string | null => {
  const match = RACE_KEY_PATTERN.exec(raceKey);
  return match ? `${match[2]}${match[3]}` : null;
};

export const buildRealtimeRaceKey = (
  source: RealtimeSource,
  year: string,
  monthDay: string,
  keibajoCode: string,
  raceNumber: string,
): string => `${source}:${year}:${monthDay}:${keibajoCode}:${raceNumber.padStart(2, "0")}`;

export const raceKeyFromRealtimePath = (pathname: string): string | null => {
  const match = pathname.match(
    /^\/api\/(jra|nar)\/races\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})\/realtime$/u,
  );
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5] || !match[6]) {
    return null;
  }
  return buildRealtimeRaceKey(
    match[1] as RealtimeSource,
    match[2],
    `${match[3]}${match[4]}`,
    match[5],
    match[6],
  );
};
