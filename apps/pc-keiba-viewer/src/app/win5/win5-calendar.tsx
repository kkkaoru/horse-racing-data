import Link from "next/link";

import { formatCount, formatDisplayDate } from "../../lib/format";
import type { Win5DaySummary, Win5YearSummary } from "../../lib/win5/types";

interface Win5CalendarProps {
  days: Win5DaySummary[];
  selectedYear: string;
  years: Win5YearSummary[];
}

export function Win5Calendar({ days, selectedYear, years }: Win5CalendarProps) {
  const monthGroups = Map.groupBy(days, (day) => day.month);

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">WIN5</p>
          <h1>{selectedYear}年 WIN5一覧</h1>
        </div>
        <div className="summary-metrics" aria-label="summary">
          <span>{formatCount(days.length)} 日</span>
        </div>
      </div>

      <nav className="year-nav" aria-label="year navigation">
        {years.map((year) => (
          <Link
            aria-current={year.year === selectedYear ? "page" : undefined}
            href={`/win5/${year.year}`}
            key={year.year}
          >
            <strong>{year.year}</strong>
            <span>{formatCount(year.dayCount)}日</span>
          </Link>
        ))}
      </nav>

      <section className="year-section">
        <div className="section-heading">
          <h2>{selectedYear}年</h2>
          <span>月別 WIN5 開催日</span>
        </div>
        <div className="month-grid">
          {Array.from(monthGroups.entries()).map(([month, monthDays]) => (
            <section className="month-panel" key={`${selectedYear}-${month}`}>
              <h3>
                <Link href={`/win5/${selectedYear}/${month}`}>{Number(month)}月</Link>
              </h3>
              <div className="day-grid">
                {monthDays.map((day) => (
                  <Link
                    className="day-link"
                    href={`/win5/${selectedYear}/${month}/${day.day}`}
                    key={day.kaisaiTsukihi}
                  >
                    <strong>{Number(day.day)}日</strong>
                    <span>{day.legCount}R</span>
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

interface Win5MonthListProps {
  days: Win5DaySummary[];
  month: string;
  year: string;
}

export function Win5MonthList({ days, month, year }: Win5MonthListProps) {
  return (
    <section className="page-shell">
      <div className="breadcrumbs">
        <Link href="/win5">WIN5</Link>
        <Link href={`/win5/${year}`}>{year}年</Link>
        <span>{Number(month)}月</span>
      </div>
      <div className="page-title-row">
        <div>
          <p className="eyebrow">WIN5</p>
          <h1>
            {year}年{Number(month)}月
          </h1>
        </div>
        <div className="summary-metrics">
          <span>{formatCount(days.length)} 日</span>
        </div>
      </div>
      <div className="day-grid">
        {days.map((day) => (
          <Link
            className="day-link"
            href={`/win5/${year}/${month}/${day.day}`}
            key={day.kaisaiTsukihi}
          >
            <strong>{formatDisplayDate(year, day.kaisaiTsukihi)}</strong>
            <span>{day.legCount}レース</span>
          </Link>
        ))}
      </div>
    </section>
  );
}
