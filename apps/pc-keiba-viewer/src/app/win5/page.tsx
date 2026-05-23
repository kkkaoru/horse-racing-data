import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { getWin5DaySummaries, getWin5Years } from "../../lib/win5-queries.server";
import { Win5TodaySection } from "./win5-today-section";
import { Win5Calendar } from "./win5-calendar";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "WIN5",
};

export default async function Win5Page() {
  const years = await getWin5Years();
  const selectedYear = years[0]?.year;
  if (!selectedYear) {
    notFound();
  }

  const days = await getWin5DaySummaries(selectedYear);

  return (
    <>
      <section className="page-shell">
        <div className="page-title-row">
          <div>
            <p className="eyebrow">WIN5</p>
            <h1>WIN5 予想</h1>
          </div>
          <div className="summary-metrics">
            <Link href={`/win5/${selectedYear}`}>年別一覧</Link>
          </div>
        </div>
      </section>
      <Win5TodaySection />
      <Win5Calendar days={days} selectedYear={selectedYear} years={years} />
    </>
  );
}
