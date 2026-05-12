"use client";

import { memo, useEffect, useState } from "react";

const formatRemainingDuration = (remainingSeconds: number): string => {
  if (remainingSeconds <= 0) {
    return "発走済み";
  }

  const hours = Math.floor(remainingSeconds / 3600);
  const minutes = Math.floor((remainingSeconds % 3600) / 60);
  const seconds = remainingSeconds % 60;

  if (hours > 0) {
    return `${hours}時間${minutes}分${seconds}秒`;
  }

  return `${minutes}分${seconds}秒`;
};

export const RaceStartCountdown = memo(function RaceStartCountdown({
  startsAt,
}: {
  startsAt: string | null;
}) {
  const [remainingSeconds, setRemainingSeconds] = useState<number | null>(null);

  useEffect(() => {
    if (startsAt === null) {
      setRemainingSeconds(null);
      return undefined;
    }

    const startTime = new Date(startsAt).getTime();
    if (!Number.isFinite(startTime)) {
      setRemainingSeconds(null);
      return undefined;
    }

    const updateRemainingSeconds = () => {
      setRemainingSeconds(Math.max(0, Math.ceil((startTime - Date.now()) / 1000)));
    };

    updateRemainingSeconds();
    const timer = window.setInterval(updateRemainingSeconds, 1000);
    return () => {
      window.clearInterval(timer);
    };
  }, [startsAt]);

  if (remainingSeconds === null) {
    return null;
  }

  const durationLabel = formatRemainingDuration(remainingSeconds);
  const countdownLabel = durationLabel === "発走済み" ? durationLabel : `発走まで${durationLabel}`;

  return (
    <span
      className="race-global-summary-countdown"
      aria-label={countdownLabel}
      title={countdownLabel}
    >
      {durationLabel === "発走済み" ? null : (
        <b className="race-global-summary-countdown-prefix">発走まで</b>
      )}
      {durationLabel}
    </span>
  );
});
