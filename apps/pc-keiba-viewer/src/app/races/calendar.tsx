import Link from "next/link";

import { formatCount } from "../../lib/format";
import type { RaceDaySummary, RaceYearSummary } from "../../lib/race-types";

interface RaceCalendarProps {
  days: RaceDaySummary[];
  selectedYear: string;
  years: RaceYearSummary[];
}

export function RaceCalendar({ days, selectedYear, years }: RaceCalendarProps) {
  const monthGroups = Map.groupBy(days, (day) => day.month);
  const totalRaces = days.reduce((sum, day) => sum + day.jraCount + day.narCount, 0);

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Race calendar</p>
          <h1>{selectedYear}年 開催日一覧</h1>
        </div>
        <div className="summary-metrics" aria-label="summary">
          <span>{formatCount(days.length)} 日</span>
          <span>{formatCount(totalRaces)} レース</span>
        </div>
      </div>

      <nav className="year-nav" aria-label="year navigation">
        {years.map((year) => (
          <Link
            aria-current={year.year === selectedYear ? "page" : undefined}
            href={`/races/${year.year}`}
            key={year.year}
          >
            <strong>{year.year}</strong>
            <span>
              {formatCount(year.dayCount)}日 / {formatCount(year.raceCount)}R
            </span>
          </Link>
        ))}
      </nav>

      <section className="year-section">
        <div className="section-heading">
          <h2>{selectedYear}年</h2>
          <span>月別開催日</span>
        </div>

        <div className="month-grid">
          {Array.from(monthGroups.entries()).map(([month, monthDays]) => (
            <section className="month-panel" key={`${selectedYear}-${month}`}>
              <h3>
                <Link href={`/races/${selectedYear}/${month}`}>{Number(month)}月</Link>
              </h3>
              <div className="day-grid">
                {monthDays.map((day) => (
                  <Link
                    className="day-link"
                    href={`/races/${selectedYear}/${month}/${day.day}`}
                    key={`${selectedYear}-${month}-${day.day}`}
                  >
                    <span className="day-number">{Number(day.day)}日</span>
                    <span className="day-counts">
                      JRA {day.jraCount} / NAR {day.narCount}
                    </span>
                  </Link>
                ))}
              </div>
            </section>
          ))}
        </div>
      </section>
    </section>
  );
}
