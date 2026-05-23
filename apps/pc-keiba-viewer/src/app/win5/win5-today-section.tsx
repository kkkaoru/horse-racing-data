import Link from "next/link";

import { formatDisplayDate } from "../../lib/format";
import { getTodayWin5DateParts, getWin5Prediction } from "../../lib/win5-queries.server";
import { Win5PredictionPanel } from "./win5-prediction-panel";

export async function Win5TodaySection() {
  const { year, month, day } = getTodayWin5DateParts();
  const kaisaiTsukihi = `${month}${day}`;
  const prediction = await getWin5Prediction(year, kaisaiTsukihi);
  if (!prediction) {
    return (
      <section className="win5-home-section page-shell">
        <h2>本日の WIN5 ({formatDisplayDate(year, kaisaiTsukihi)})</h2>
        <p className="empty-state">本日の WIN5 予想はまだありません。</p>
      </section>
    );
  }

  return (
    <section className="win5-home-section page-shell">
      <div className="section-heading">
        <h2>本日の WIN5 ({formatDisplayDate(year, kaisaiTsukihi)})</h2>
        <Link href={`/win5/${year}/${month}/${day}`}>日付ページへ</Link>
      </div>
      <Win5PredictionPanel day={day} month={month} prediction={prediction} year={year} />
    </section>
  );
}
