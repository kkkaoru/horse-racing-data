import Link from "next/link";
import { notFound } from "next/navigation";

import { getRacesByDate } from "../../../../../db/queries";
import { formatDate, formatDisplayDate } from "../../../../../lib/format";
import { RaceDateFilter } from "./race-date-filter";

export const dynamic = "force-dynamic";

interface RaceDatePageProps {
  params: Promise<{
    year: string;
    month: string;
    day: string;
  }>;
}

const isValidDateParams = (year: string, month: string, day: string): boolean =>
  /^\d{4}$/.test(year) && /^\d{2}$/.test(month) && /^\d{2}$/.test(day);

export default async function RaceDatePage({ params }: RaceDatePageProps) {
  const { year, month, day } = await params;
  if (!isValidDateParams(year, month, day)) {
    notFound();
  }

  const races = await getRacesByDate(year, month, day);

  return (
    <section className="page-shell">
      <div className="breadcrumbs">
        <Link href="/races">開催日一覧</Link>
        <span>{year}年</span>
        <span>{Number(month)}月</span>
        <span>{Number(day)}日</span>
      </div>

      <div className="page-title-row">
        <div>
          <p className="eyebrow">{formatDate(year, `${month}${day}`)}</p>
          <h1>{formatDisplayDate(year, `${month}${day}`)}</h1>
        </div>
        <div className="summary-metrics" aria-label="summary">
          <span>{races.length} レース</span>
          <span>発走時刻 ASC</span>
        </div>
      </div>

      {races.length === 0 ? (
        <p className="empty-state">この日のレースは見つかりませんでした。</p>
      ) : (
        <RaceDateFilter day={day} month={month} races={races} year={year} />
      )}
    </section>
  );
}
