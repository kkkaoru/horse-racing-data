import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { getRacesByDate } from "../../../../../../db/queries";
import { formatDate, formatDisplayDate, formatKeibajo } from "../../../../../../lib/format";
import { getDefaultRaceStartFilterTime } from "../race-date-defaults";
import { RaceDateFilter } from "../race-date-filter";

export const dynamic = "force-dynamic";

interface RaceVenuePageProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    year: string;
  }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

const isValidVenueParams = (
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
): boolean =>
  /^\d{4}$/.test(year) &&
  /^\d{2}$/.test(month) &&
  /^\d{2}$/.test(day) &&
  /^[0-9A-Z]{2}$/.test(keibajoCode);

export async function generateMetadata({ params }: RaceVenuePageProps): Promise<Metadata> {
  const { day, keibajoCode, month, year } = await params;
  return {
    title: isValidVenueParams(year, month, day, keibajoCode)
      ? `${formatDisplayDate(year, `${month}${day}`)} ${formatKeibajo(keibajoCode)}`
      : "レース一覧",
  };
}

export default async function RaceVenuePage({ params, searchParams }: RaceVenuePageProps) {
  const { day, keibajoCode, month, year } = await params;
  if (!isValidVenueParams(year, month, day, keibajoCode)) {
    notFound();
  }

  const races = (await getRacesByDate(year, month, day)).filter(
    (race) => race.keibajoCode === keibajoCode,
  );
  const initialSearchParams = await searchParams;
  if (races.length === 0) {
    notFound();
  }

  return (
    <section className="page-shell">
      <div className="breadcrumbs">
        <Link href="/races">開催日一覧</Link>
        <Link href={`/races/${year}`}>{year}年</Link>
        <Link href={`/races/${year}/${month}`}>{Number(month)}月</Link>
        <Link href={`/races/${year}/${month}/${day}`}>
          {formatDisplayDate(year, `${month}${day}`)}
        </Link>
        <span>{formatKeibajo(keibajoCode)}</span>
      </div>

      <div className="page-title-row">
        <div>
          <p className="eyebrow">{formatDate(year, `${month}${day}`)}</p>
          <h1>
            {formatDisplayDate(year, `${month}${day}`)} {formatKeibajo(keibajoCode)}
          </h1>
        </div>
        <div className="summary-metrics" aria-label="summary">
          <span>{races.length} レース</span>
          <span>発走時刻 ASC</span>
        </div>
      </div>

      <RaceDateFilter
        day={day}
        defaultStartTime={getDefaultRaceStartFilterTime(year, month, day)}
        fixedVenueCode={keibajoCode}
        initialSearchParams={initialSearchParams}
        month={month}
        races={races}
        year={year}
      />
    </section>
  );
}
