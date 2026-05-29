export const LOCAL_KEIBAJO_TO_NAR_BABA_CODE = {
  "30": "36",
  "35": "10",
  "36": "11",
  "42": "18",
  "43": "19",
  "44": "20",
  "45": "21",
  "46": "22",
  "47": "23",
  "48": "24",
  "50": "27",
  "51": "28",
  "54": "31",
  "55": "32",
  "83": "03",
} as const satisfies Record<string, string>;

export const NAR_BABA_CODE_TO_LOCAL_KEIBAJO = Object.fromEntries(
  Object.entries(LOCAL_KEIBAJO_TO_NAR_BABA_CODE).map(([keibajoCode, babaCode]) => [
    babaCode,
    keibajoCode,
  ]),
) as Record<string, string>;

export const buildNarRaceKey = (
  year: string,
  monthDay: string,
  keibajoCode: string,
  raceNumber: string,
): string => `nar:${year}:${monthDay}:${keibajoCode}:${raceNumber.padStart(2, "0")}`;

export const parseNarRaceKey = (
  raceKey: string,
): {
  keibajoCode: string;
  monthDay: string;
  raceNumber: string;
  year: string;
} | null => {
  const match = raceKey.match(/^nar:(\d{4}):(\d{4}):([0-9A-Z]{2}):(\d{2})$/u);
  if (!match?.[1] || !match[2] || !match[3] || !match[4]) {
    return null;
  }
  return {
    keibajoCode: match[3],
    monthDay: match[2],
    raceNumber: match[4],
    year: match[1],
  };
};
