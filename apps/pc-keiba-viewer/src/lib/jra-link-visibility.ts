const JRA_RESULT_LINK_DELAY_DAYS = 5;
const JST_OFFSET = "+09:00";
const MS_PER_DAY = 24 * 60 * 60 * 1000;

const getRaceDateStartMs = (year: string, month: string, day: string): number =>
  new Date(
    `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}T00:00:00${JST_OFFSET}`,
  ).getTime();

export const isJraResultLinkAvailable = (
  year: string,
  month: string,
  day: string,
  now: number = Date.now(),
): boolean => {
  const raceDateStartMs = getRaceDateStartMs(year, month, day);
  if (Number.isNaN(raceDateStartMs)) {
    return false;
  }
  return now >= raceDateStartMs + JRA_RESULT_LINK_DELAY_DAYS * MS_PER_DAY;
};
