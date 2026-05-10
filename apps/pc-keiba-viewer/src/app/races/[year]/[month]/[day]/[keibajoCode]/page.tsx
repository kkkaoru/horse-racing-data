import Link from "next/link";
import { notFound } from "next/navigation";

import { getRacesByDate } from "../../../../../../db/queries";
import { SOURCE_LABELS } from "../../../../../../lib/codes";
import {
  cleanText,
  formatDate,
  formatDisplayDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTime,
  formatTrack,
} from "../../../../../../lib/format";
import { getRaceTags } from "../../../../../../lib/race-classification";

export const dynamic = "force-dynamic";

interface RaceVenuePageProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    year: string;
  }>;
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

export default async function RaceVenuePage({ params }: RaceVenuePageProps) {
  const { day, keibajoCode, month, year } = await params;
  if (!isValidVenueParams(year, month, day, keibajoCode)) {
    notFound();
  }

  const races = (await getRacesByDate(year, month, day)).filter(
    (race) => race.keibajoCode === keibajoCode,
  );
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

      <div className="race-list">
        {races.map((race) => {
          const tags = getRaceTags(race);

          return (
            <Link
              className="race-row"
              href={`/races/${year}/${month}/${day}/${race.keibajoCode}/${race.raceBango}`}
              key={`${race.source}-${race.keibajoCode}-${race.raceBango}`}
            >
              <span className="race-time">{formatTime(race.hassoJikoku)}</span>
              <span className="race-main">
                <strong>
                  {SOURCE_LABELS[race.source]} {formatRaceNumber(race.raceBango)}
                </strong>
                <span>{cleanText(race.kyosomeiHondai, "一般競走")}</span>
                {tags.length > 0 ? (
                  <span className="tag-list">
                    {tags.map((raceTag) => (
                      <span className="race-tag" key={raceTag}>
                        {raceTag}
                      </span>
                    ))}
                  </span>
                ) : null}
              </span>
              <span className="race-meta">
                {formatTrack(race.trackCode)} {formatDistance(race.kyori)}
              </span>
            </Link>
          );
        })}
      </div>
    </section>
  );
}
