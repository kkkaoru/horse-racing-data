const toJstDateParts = (
  date: Date,
): { day: string; hour: string; minute: string; month: string; year: string } => {
  const parts = new Intl.DateTimeFormat("ja-JP", {
    day: "2-digit",
    hour: "2-digit",
    hourCycle: "h23",
    minute: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  }).formatToParts(date);
  const get = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((part) => part.type === type)?.value ?? "";
  return {
    day: get("day"),
    hour: get("hour"),
    minute: get("minute"),
    month: get("month"),
    year: get("year"),
  };
};

export const getDefaultRaceStartFilterTime = (
  year: string,
  month: string,
  day: string,
  now = new Date(),
): string => {
  const jstNow = toJstDateParts(now);
  if (jstNow.year !== year || jstNow.month !== month || jstNow.day !== day) {
    return "";
  }
  return `${jstNow.hour}:${jstNow.minute}`;
};
