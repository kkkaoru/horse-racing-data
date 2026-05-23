import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { formatDisplayDate, formatKeibajo, formatRaceNumber } from "../../../../../lib/format";
import { getWin5Prediction, getWin5Schedule } from "../../../../../lib/win5-queries.server";
import { Win5PredictionPanel } from "../../../win5-prediction-panel";

export const dynamic = "force-dynamic";

interface Win5DatePageProps {
  params: Promise<{
    year: string;
    month: string;
    day: string;
  }>;
}

const isValidDateParams = (year: string, month: string, day: string): boolean =>
  /^\d{4}$/.test(year) && /^\d{2}$/.test(month) && /^\d{2}$/.test(day);

export async function generateMetadata({ params }: Win5DatePageProps): Promise<Metadata> {
  const { day, month, year } = await params;
  return {
    title: isValidDateParams(year, month, day)
      ? `${formatDisplayDate(year, `${month}${day}`)} WIN5`
      : "WIN5",
  };
}

export default async function Win5DatePage({ params }: Win5DatePageProps) {
  const { year, month, day } = await params;
  if (!isValidDateParams(year, month, day)) {
    notFound();
  }

  const kaisaiTsukihi = `${month}${day}`;
  const [schedule, prediction] = await Promise.all([
    getWin5Schedule(year, kaisaiTsukihi),
    getWin5Prediction(year, kaisaiTsukihi),
  ]);

  if (!schedule || !prediction) {
    notFound();
  }

  return (
    <section className="page-shell">
      <div className="breadcrumbs">
        <Link href="/win5">WIN5</Link>
        <Link href={`/win5/${year}`}>{year}年</Link>
        <Link href={`/win5/${year}/${month}`}>{Number(month)}月</Link>
        <span>{Number(day)}日</span>
      </div>

      <div className="page-title-row">
        <div>
          <p className="eyebrow">WIN5</p>
          <h1>{formatDisplayDate(year, kaisaiTsukihi)}</h1>
        </div>
        <div className="summary-metrics">
          <span>{schedule.legs.length} レース</span>
          {schedule.saleDeadline ? <span>締切 {schedule.saleDeadline}</span> : null}
        </div>
      </div>

      <div aria-label="WIN5対象レース" className="win5-race-overview">
        {schedule.legs.map((leg) => (
          <Link
            className="win5-race-chip"
            href={`/races/${year}/${month}/${day}/${leg.keibajoCode}/${leg.raceBango.padStart(2, "0")}`}
            key={leg.legIndex}
          >
            <strong>
              第{leg.legIndex}R {leg.keibajoName ?? formatKeibajo(leg.keibajoCode)}
              {formatRaceNumber(leg.raceBango)}
            </strong>
            {leg.startTime ? <span>{leg.startTime}</span> : null}
          </Link>
        ))}
      </div>

      <Win5PredictionPanel
        day={day}
        month={month}
        prediction={prediction}
        year={year}
      />
    </section>
  );
}
