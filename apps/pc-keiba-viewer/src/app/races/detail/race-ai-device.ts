"use client";

export const isLikelyMobileBrowser = (): boolean => {
  if (typeof navigator === "undefined") {
    return false;
  }
  const userAgent = navigator.userAgent;
  return (
    /Android|iPad|iPhone|iPod/iu.test(userAgent) ||
    (/Macintosh/iu.test(userAgent) && navigator.maxTouchPoints > 1)
  );
};
