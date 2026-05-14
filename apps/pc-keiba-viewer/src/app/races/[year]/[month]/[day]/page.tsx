import Link from "next/link";
import { notFound } from "next/navigation";

import { getRacesByDate } from "../../../../../db/queries";
import { formatDate, formatDisplayDate, formatKeibajo } from "../../../../../lib/format";
import { getDefaultRaceStartFilterTime } from "./race-date-defaults";
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
  const venues = [...new Set(races.map((race) => race.keibajoCode))]
    .map((keibajoCode) => ({
      keibajoCode,
      name: formatKeibajo(keibajoCode),
      raceCount: races.filter((race) => race.keibajoCode === keibajoCode).length,
    }))
    .toSorted((left, right) => left.name.localeCompare(right.name, "ja"));

  return (
    <section className="page-shell">
      <div className="breadcrumbs">
        <Link href="/races">開催日一覧</Link>
        <Link href={`/races/${year}`}>{year}年</Link>
        <Link href={`/races/${year}/${month}`}>{Number(month)}月</Link>
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
        <>
          <nav className="year-nav venue-nav" aria-label="venue navigation">
            {venues.map((venue) => (
              <Link
                href={`/races/${year}/${month}/${day}/${venue.keibajoCode}`}
                key={venue.keibajoCode}
              >
                <strong>{venue.name}</strong>
                <span>{venue.raceCount}R</span>
              </Link>
            ))}
          </nav>
          <RaceDateFilter
            day={day}
            defaultStartTime={getDefaultRaceStartFilterTime(year, month, day)}
            month={month}
            races={races}
            year={year}
          />
        </>
      )}
    </section>
  );
}
