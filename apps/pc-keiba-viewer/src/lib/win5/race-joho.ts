import type { Win5RaceLeg } from "./types";

export interface ParsedRaceJoho {
  keibajoCode: string;
  kaisaiKai: string;
  kaisaiNichime: string;
  raceBango: string;
}

export const parseRaceJoho = (value: string | null | undefined): ParsedRaceJoho | null => {
  const cleaned = (value ?? "").trim();
  if (!/^\d{8}$/u.test(cleaned)) {
    return null;
  }
  return {
    keibajoCode: cleaned.slice(0, 2),
    kaisaiKai: cleaned.slice(2, 4),
    kaisaiNichime: cleaned.slice(4, 6),
    raceBango: cleaned.slice(6, 8),
  };
};

export const buildRaceJoho = (leg: Pick<Win5RaceLeg, "keibajoCode" | "kaisaiKai" | "kaisaiNichime" | "raceBango">): string =>
  `${leg.keibajoCode}${leg.kaisaiKai}${leg.kaisaiNichime}${leg.raceBango.padStart(2, "0")}`;

export const buildWin5LegsFromRaceJoho = (
  raceJohoValues: ReadonlyArray<string | null | undefined>,
): Win5RaceLeg[] =>
  raceJohoValues
    .map((value, index) => {
      const parsed = parseRaceJoho(value);
      if (parsed === null) {
        return null;
      }
      return {
        legIndex: index + 1,
        ...parsed,
        raceBango: parsed.raceBango.replace(/^0+/u, "") || parsed.raceBango,
      };
    })
    .filter((leg): leg is Win5RaceLeg => leg !== null);
