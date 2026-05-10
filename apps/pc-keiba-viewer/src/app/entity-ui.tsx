import Link from "next/link";

import {
  cleanText,
  formatDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTrack,
} from "../lib/format";
import type {
  EntityDetailSummary,
  EntityListQuery,
  EntityRaceResult,
  HorseListRow,
  PersonListRow,
} from "../lib/race-types";

const sourceLabel = (source: EntityListQuery["source"]): string => {
  if (source === "jra") {
    return "JRA";
  }
  if (source === "nar") {
    return "NAR";
  }
  return "全て";
};

export const parseEntityListQuery = (
  searchParams: Record<string, string | string[] | undefined>,
): EntityListQuery => {
  const getValue = (key: string, fallback: string) => {
    const value = searchParams[key];
    return typeof value === "string" ? value : fallback;
  };
  const source = getValue("source", "all");
  return {
    dateFrom: getValue("dateFrom", ""),
    dateTo: getValue("dateTo", ""),
    distanceMax: getValue("distanceMax", ""),
    distanceMin: getValue("distanceMin", ""),
    keibajoCode: getValue("keibajoCode", "").trim(),
    order: getValue("order", "latest"),
    q: getValue("q", "").trim(),
    rank: getValue("rank", "all"),
    source: source === "jra" || source === "nar" ? source : "all",
    surface: getValue("surface", "all"),
  };
};

interface EntityFilterFormProps {
  action: string;
  query: EntityListQuery;
  searchPlaceholder: string;
}

export function EntityFilterForm({ action, query, searchPlaceholder }: EntityFilterFormProps) {
  return (
    <form action={action} className="entity-filter-panel">
      <label>
        <span>検索</span>
        <input defaultValue={query.q} name="q" placeholder={searchPlaceholder} />
      </label>
      <label>
        <span>対象</span>
        <select defaultValue={query.source} name="source">
          <option value="all">全て</option>
          <option value="jra">JRA</option>
          <option value="nar">NAR</option>
        </select>
      </label>
      <label>
        <span>並び替え</span>
        <select defaultValue={query.order} name="order">
          <option value="latest">最新出走</option>
          <option value="starts">出走数</option>
          <option value="winRate">勝率</option>
          <option value="showRate">複勝率</option>
          <option value="name">名前</option>
          <option value="rank">着順</option>
          <option value="odds">単勝</option>
          <option value="time">タイム</option>
        </select>
      </label>
      <button type="submit">絞り込み</button>
    </form>
  );
}

export function EntityDetailFilterForm({
  action,
  query,
  searchPlaceholder,
}: EntityFilterFormProps) {
  return (
    <form action={action} className="entity-filter-panel entity-detail-filter-panel">
      <label>
        <span>検索</span>
        <input defaultValue={query.q} name="q" placeholder={searchPlaceholder} />
      </label>
      <label>
        <span>対象</span>
        <select defaultValue={query.source} name="source">
          <option value="all">全て</option>
          <option value="jra">JRA</option>
          <option value="nar">NAR</option>
        </select>
      </label>
      <label>
        <span>着順</span>
        <select defaultValue={query.rank} name="rank">
          <option value="all">全て</option>
          <option value="win">1着</option>
          <option value="top2">連対</option>
          <option value="top3">複勝圏</option>
          <option value="out">4着以下</option>
          <option value="upcoming">出走予定</option>
        </select>
      </label>
      <label>
        <span>馬場</span>
        <select defaultValue={query.surface} name="surface">
          <option value="all">全て</option>
          <option value="turf">芝</option>
          <option value="dirt">ダート</option>
          <option value="obstacle">障害</option>
        </select>
      </label>
      <label>
        <span>競馬場コード</span>
        <input
          defaultValue={query.keibajoCode}
          inputMode="numeric"
          name="keibajoCode"
          placeholder="05"
        />
      </label>
      <label>
        <span>距離 min</span>
        <input
          defaultValue={query.distanceMin}
          inputMode="numeric"
          name="distanceMin"
          placeholder="1200"
        />
      </label>
      <label>
        <span>距離 max</span>
        <input
          defaultValue={query.distanceMax}
          inputMode="numeric"
          name="distanceMax"
          placeholder="2000"
        />
      </label>
      <label>
        <span>日付 from</span>
        <input defaultValue={query.dateFrom} name="dateFrom" type="date" />
      </label>
      <label>
        <span>日付 to</span>
        <input defaultValue={query.dateTo} name="dateTo" type="date" />
      </label>
      <label>
        <span>並び替え</span>
        <select defaultValue={query.order} name="order">
          <option value="latest">最新出走</option>
          <option value="rank">着順</option>
          <option value="odds">単勝</option>
          <option value="time">タイム</option>
        </select>
      </label>
      <button type="submit">絞り込み</button>
    </form>
  );
}

const formatRate = (value: number): string => `${value.toFixed(1)}%`;

export function HorseListTable({ rows }: { rows: HorseListRow[] }) {
  if (rows.length === 0) {
    return <p className="empty-state">条件に一致する馬はありません。</p>;
  }
  return (
    <div className="entity-table-wrap">
      <table className="entity-table">
        <thead>
          <tr>
            <th>馬名</th>
            <th>対象</th>
            <th>出走</th>
            <th>勝利</th>
            <th>勝率</th>
            <th>複勝率</th>
            <th>最新</th>
            <th>最新レース</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.kettoTorokuBango}>
              <td className="entity-name-cell">
                <Link href={`/horses/${encodeURIComponent(row.kettoTorokuBango)}`}>
                  {cleanText(row.bamei)}
                </Link>
              </td>
              <td>{sourceLabel(row.primarySource)}</td>
              <td>{row.starts.toLocaleString("ja-JP")}</td>
              <td>{row.winCount.toLocaleString("ja-JP")}</td>
              <td>{formatRate(row.winRate)}</td>
              <td>{formatRate(row.showRate)}</td>
              <td>{formatDate(row.latestDate.slice(0, 4), row.latestDate.slice(4, 8))}</td>
              <td>{cleanText(row.latestRaceName)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function PersonListTable({
  basePath,
  rows,
}: {
  basePath: "/jockeys" | "/trainers";
  rows: PersonListRow[];
}) {
  if (rows.length === 0) {
    return <p className="empty-state">条件に一致するデータはありません。</p>;
  }
  return (
    <div className="entity-table-wrap">
      <table className="entity-table">
        <thead>
          <tr>
            <th>名前</th>
            <th>対象</th>
            <th>出走</th>
            <th>勝利</th>
            <th>勝率</th>
            <th>複勝率</th>
            <th>最新</th>
            <th>最新レース</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.name}>
              <td className="entity-name-cell">
                <Link href={`${basePath}/${encodeURIComponent(row.name)}`}>{row.name}</Link>
              </td>
              <td>{sourceLabel(row.primarySource)}</td>
              <td>{row.starts.toLocaleString("ja-JP")}</td>
              <td>{row.winCount.toLocaleString("ja-JP")}</td>
              <td>{formatRate(row.winRate)}</td>
              <td>{formatRate(row.showRate)}</td>
              <td>{formatDate(row.latestDate.slice(0, 4), row.latestDate.slice(4, 8))}</td>
              <td>{cleanText(row.latestRaceName)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function EntitySummary({ summary }: { summary: EntityDetailSummary }) {
  return (
    <section className="entity-summary-grid">
      <div>
        <span>出走</span>
        <strong>{summary.starts.toLocaleString("ja-JP")}</strong>
      </div>
      <div>
        <span>勝率</span>
        <strong>{formatRate(summary.winRate)}</strong>
      </div>
      <div>
        <span>連対率</span>
        <strong>{formatRate(summary.quinellaRate)}</strong>
      </div>
      <div>
        <span>複勝率</span>
        <strong>{formatRate(summary.showRate)}</strong>
      </div>
      <div>
        <span>平均人気</span>
        <strong>{summary.averagePopularity?.toFixed(1) ?? "-"}</strong>
      </div>
      <div>
        <span>平均単勝</span>
        <strong>{summary.averageOdds?.toFixed(1) ?? "-"}</strong>
      </div>
    </section>
  );
}

const formatOdds = (value: string | null | undefined): string => {
  const parsed = Number(cleanText(value, ""));
  return Number.isFinite(parsed) && parsed > 0 ? (parsed / 10).toFixed(1) : "-";
};

const formatRank = (value: string | null | undefined): string => {
  const parsed = Number(cleanText(value, ""));
  return Number.isFinite(parsed) && parsed > 0 ? String(parsed) : "-";
};

const getEntityResultRowClassName = (row: EntityRaceResult): string | undefined => {
  if (row.isUpcoming) {
    return "entity-result-row-upcoming";
  }
  if (row.rank === "01") {
    return "entity-result-row-win";
  }
  if (row.rank === "02") {
    return "entity-result-row-place";
  }
  if (row.rank === "03") {
    return "entity-result-row-show";
  }
  return undefined;
};

export function EntityRaceResultsTable({ rows }: { rows: EntityRaceResult[] }) {
  if (rows.length === 0) {
    return <p className="empty-state">条件に一致する成績はありません。</p>;
  }
  return (
    <div className="entity-table-wrap">
      <table className="entity-table entity-results-table">
        <thead>
          <tr>
            <th>日付</th>
            <th>競馬場</th>
            <th>R</th>
            <th>馬名</th>
            <th>騎手</th>
            <th>調教師</th>
            <th>距離</th>
            <th>コース</th>
            <th>着順</th>
            <th>人気</th>
            <th>単勝</th>
            <th>レース</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              className={getEntityResultRowClassName(row)}
              key={[
                row.source,
                row.kaisaiNen,
                row.kaisaiTsukihi,
                row.keibajoCode,
                row.raceBango,
                row.horseNumber,
                row.horseName,
              ].join("-")}
            >
              <td>{formatDate(row.kaisaiNen, row.kaisaiTsukihi)}</td>
              <td>{formatKeibajo(row.keibajoCode)}</td>
              <td>{formatRaceNumber(row.raceBango)}</td>
              <td>{row.horseName}</td>
              <td>{row.jockeyName}</td>
              <td>{row.trainerName}</td>
              <td>{formatDistance(row.kyori)}</td>
              <td>{formatTrack(row.trackCode)}</td>
              <td>{formatRank(row.rank)}</td>
              <td>{formatRank(row.popularity)}</td>
              <td>{formatOdds(row.winOdds)}</td>
              <td className="entity-name-cell">
                <Link
                  href={`/races/${row.kaisaiNen}/${row.kaisaiTsukihi.slice(0, 2)}/${row.kaisaiTsukihi.slice(2, 4)}/${row.keibajoCode}/${row.raceBango}`}
                >
                  {row.raceName}
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
