import { isJstPollingWindow } from "../time";

export const shouldRunOddsCron = (now: Date): boolean => isJstPollingWindow(now);
