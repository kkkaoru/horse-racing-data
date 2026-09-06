import type { Provider } from "./types";

export const CRON_NV_DAILY = "0 16 * * *";
export const CRON_NV_MONITOR = "0 17 * * *";
export const CRON_JV_DAILY = "0 11 * * *";
export const CRON_JV_MONITOR = "0 12 * * *";
export const DEFAULT_LOOKBACK_DAYS = 2;
export const BOOTSTRAP_LOOKBACK_DAYS = 7;

export interface ScheduledAction {
  action: "monitor" | "run";
  provider: Provider;
}

const ACTIONS: Readonly<Record<string, ScheduledAction>> = Object.freeze({
  [CRON_NV_DAILY]: { action: "run", provider: "nv" },
  [CRON_NV_MONITOR]: { action: "monitor", provider: "nv" },
  [CRON_JV_DAILY]: { action: "run", provider: "jv" },
  [CRON_JV_MONITOR]: { action: "monitor", provider: "jv" },
});

export const scheduledAction = (cron: string): ScheduledAction | undefined => ACTIONS[cron];

export const jstDate = (timestamp: number): string => jstTimestamp(timestamp).slice(0, 8);

export const jstTimestamp = (timestamp: number): string => {
  const date = new Date(timestamp + 9 * 60 * 60 * 1000);
  return date.toISOString().slice(0, 19).replaceAll(/[-:T]/g, "");
};

const shiftYmd = (ymd: string, days: number): string => {
  const year = Number(ymd.slice(0, 4));
  const month = Number(ymd.slice(4, 6));
  const day = Number(ymd.slice(6, 8));
  const shifted = new Date(Date.UTC(year, month - 1, day + days));
  return shifted.toISOString().slice(0, 10).replaceAll("-", "");
};

export interface AcquisitionWindow {
  fromTime: string;
  toTime: string | null;
}

export const defaultAcquisitionWindow = (
  provider: Provider,
  runDate: string,
  lookbackDays: number,
): AcquisitionWindow => ({
  fromTime: `${shiftYmd(runDate, -lookbackDays)}000000`,
  toTime: provider === "jv" ? `${runDate}235959` : null,
});

export const acquisitionBody = (
  provider: Provider,
  fromTime: string,
  toTime: string | null,
): Readonly<Record<string, number | string>> => {
  if (provider === "nv") return { dataSpec: "RACE", fromTime, option: 1 };
  if (toTime === null) throw new Error("JV acquisition requires an end time");
  return { dataSpec: "RACE", from: fromTime, to: toTime };
};
