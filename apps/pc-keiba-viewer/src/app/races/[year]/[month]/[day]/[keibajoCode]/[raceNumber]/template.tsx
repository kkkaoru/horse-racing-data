"use client";

import { useEffect, useState, type ReactNode } from "react";

export default function RaceDetailTemplate({ children }: { children: ReactNode }) {
  const [ready, setReady] = useState(false);

  useEffect(() => {
    setReady(true);
  }, []);

  return (
    <div className={ready ? "race-page-transition ready" : "race-page-transition"}>{children}</div>
  );
}
