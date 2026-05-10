import Link from "next/link";
import { notFound } from "next/navigation";

import { getRaceDaySummaries } from "../../../../db/queries";
import { formatCount } from "../../../../lib/format";

export const dynamic = "force-dynamic";

interface RaceMonthPageProps {
  params: Promise<{
    month: string;
    year: string;
  }>;
}

const isValidMonthParams = (year: string, month: string): boolean =>
  /^\d{4}$/.test(year) && /^\d{2}$/.test(month);

export default async function RaceMonthPage({ params }: RaceMonthPageProps) {
  const { month, year } = await params;
  if (!isValidMonthParams(year, month)) {
    notFound();
  }

  const days = (await getRaceDaySummaries(year)).filter((day) => day.month === month);
  const totalRaces = days.reduce((sum, day) => sum + day.jraCount + day.narCount, 0);

  return (
    <section className="page-shell">
      <div className="breadcrumbs">
        <Link href="/races">開催日一覧</Link>
        <Link href={`/races/${year}`}>{year}年</Link>
        <span>{Number(month)}月</span>
      </div>

      <div className="page-title-row">
        <div>
          <p className="eyebrow">Race calendar</p>
          <h1>
            {year}年 {Number(month)}月 開催日一覧
          </h1>
        </div>
        <div className="summary-metrics" aria-label="summary">
          <span>{formatCount(days.length)} 日</span>
          <span>{formatCount(totalRaces)} レース</span>
        </div>
      </div>

      {days.length === 0 ? (
        <p className="empty-state">この月のレースは見つかりませんでした。</p>
      ) : (
        <div className="day-grid month-page-day-grid">
          {days.map((day) => (
            <Link
              className="day-link"
              href={`/races/${year}/${month}/${day.day}`}
              key={`${year}-${month}-${day.day}`}
            >
              <span className="day-number">{Number(day.day)}日</span>
              <span className="day-counts">
                JRA {day.jraCount} / NAR {day.narCount}
              </span>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}
