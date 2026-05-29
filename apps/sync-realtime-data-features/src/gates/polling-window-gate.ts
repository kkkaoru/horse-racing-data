// Run with bun. Gate 1: skip cron tick outside JST polling window.

import { isJstPollingWindow } from "../time";

export const shouldRunFeaturesCron = (now: Date): boolean => isJstPollingWindow(now);
