import Image from "next/image";
import Link from "next/link";
import { notFound } from "next/navigation";

import {
  getRaceCourseInfo,
  getRaceDetail,
  getRaceRunners,
  getRacesByDate,
} from "../../../db/queries";
import { SOURCE_LABELS, type RaceSource } from "../../../lib/codes";
import { formatCourseParagraphs, getCourseFacts, getCourseImagePath } from "../../../lib/course";
import {
  cleanText,
  formatBaba,
  formatDate,
  formatDisplayDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTime,
  formatTrack,
  formatWeather,
  getTrackSurfaceLabel,
} from "../../../lib/format";
import { getRaceTags, getWeightLabel } from "../../../lib/race-classification";
import type { RaceDetail } from "../../../lib/race-types";
import { isBanEiKeibajoCode } from "../../../lib/runner-format";
import { LazyDetailSections } from "./lazy-detail-sections";
import { RealtimeRaceSection } from "./realtime-race-section";
import { RunnersTable } from "./runners-table";

export const dynamic = "force-dynamic";

interface RaceDetailViewProps {
  day: string;
  initialRace?: RaceDetail;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  searchParams: Record<string, string | string[] | undefined>;
  source: RaceSource;
  year: string;
}

const getRaceDetailPath = (race: {
  source: RaceSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}): string =>
  `/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(0, 2)}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}`;

const getAdjacentRaceLabel = (race: {
  hassoJikoku: string | null;
  kyori: string | null;
  raceBango: string;
  trackCode: string | null;
}): string =>
  [
    formatRaceNumber(race.raceBango),
    formatTime(race.hassoJikoku),
    getTrackSurfaceLabel(race.trackCode) ?? formatTrack(race.trackCode),
    formatDistance(race.kyori),
  ].join(" / ");

export async function RaceDetailView({
  day,
  initialRace,
  keibajoCode,
  month,
  raceNumber,
  source: raceSource,
  year,
}: RaceDetailViewProps) {
  const race =
    initialRace ?? (await getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber));

  if (!race) {
    notFound();
  }

  const raceName = cleanText(race.kyosomeiHondai, "一般競走");
  const raceTags = getRaceTags(race);
  const [courseInfo, runners, raceDayRaces] = await Promise.all([
    getRaceCourseInfo(keibajoCode, race.kyori, race.trackCode),
    getRaceRunners(raceSource, year, month, day, keibajoCode, raceNumber),
    getRacesByDate(year, month, day),
  ]);
  const sameVenueRaces = raceDayRaces
    .filter((item) => item.source === raceSource && item.keibajoCode === keibajoCode)
    .toSorted((left, right) => Number(left.raceBango) - Number(right.raceBango));
  const currentRaceIndex = sameVenueRaces.findIndex((item) => item.raceBango === raceNumber);
  const previousRace = currentRaceIndex > 0 ? sameVenueRaces[currentRaceIndex - 1] : null;
  const nextRace =
    currentRaceIndex >= 0 && currentRaceIndex < sameVenueRaces.length - 1
      ? sameVenueRaces[currentRaceIndex + 1]
      : null;
  const courseText = cleanText(courseInfo?.courseSetsumei, "");
  const courseFacts = getCourseFacts(courseText, race.kyori, race.trackCode);
  const courseParagraphs = courseText
    ? formatCourseParagraphs(courseText)
    : ["このコースの説明データは見つかりませんでした。"];
  const courseImagePath = getCourseImagePath(keibajoCode, race.trackCode, race.kyori);
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";
  return (
    <section className="page-shell">
      <div className="race-global-summary" aria-label="race summary in global header">
        <div>
          <span>{formatTime(race.hassoJikoku)}発走</span>
          <span>{formatKeibajo(keibajoCode)}</span>
          <span>{formatRaceNumber(raceNumber)}</span>
          <span>{getTrackSurfaceLabel(race.trackCode) ?? formatTrack(race.trackCode)}</span>
          <span>{formatDistance(race.kyori)}</span>
        </div>
      </div>
      {previousRace ? (
        <Link
          aria-label={`前のレース ${getAdjacentRaceLabel(previousRace)}`}
          className="race-side-nav race-side-nav-prev"
          href={getRaceDetailPath(previousRace)}
        >
          <span className="race-nav-icon" aria-hidden="true" />
        </Link>
      ) : null}
      {nextRace ? (
        <Link
          aria-label={`次のレース ${getAdjacentRaceLabel(nextRace)}`}
          className="race-side-nav race-side-nav-next"
          href={getRaceDetailPath(nextRace)}
        >
          <span className="race-nav-icon" aria-hidden="true" />
        </Link>
      ) : null}
      <details className="race-detail-list-menu">
        <summary aria-label="一覧ページへのリンクを開閉">
          <span aria-hidden="true" />
        </summary>
        <nav aria-label="race list navigation">
          <Link href="/races">開催日一覧</Link>
          <Link href={`/races/${year}`}>{year}年の一覧</Link>
          <Link href={`/races/${year}/${month}`}>{Number(month)}月の一覧</Link>
          <Link href={`/races/${year}/${month}/${day}`}>
            {formatDisplayDate(year, `${month}${day}`)}の一覧
          </Link>
          <Link href={`/races/${year}/${month}/${day}/${keibajoCode}`}>
            {formatKeibajo(keibajoCode)}の一覧
          </Link>
        </nav>
      </details>
      {previousRace || nextRace ? (
        <nav className="race-mobile-nav" aria-label="same venue race navigation">
          {previousRace ? (
            <Link
              aria-label={`前のレース ${getAdjacentRaceLabel(previousRace)}`}
              className="race-mobile-nav-prev"
              href={getRaceDetailPath(previousRace)}
            >
              <span className="race-nav-icon" aria-hidden="true" />
            </Link>
          ) : (
            <span aria-hidden="true" />
          )}
          {nextRace ? (
            <Link
              aria-label={`次のレース ${getAdjacentRaceLabel(nextRace)}`}
              className="race-mobile-nav-next"
              href={getRaceDetailPath(nextRace)}
            >
              <span className="race-nav-icon" aria-hidden="true" />
            </Link>
          ) : (
            <span aria-hidden="true" />
          )}
        </nav>
      ) : null}

      <div className="detail-hero">
        <div>
          <p className="eyebrow">
            {SOURCE_LABELS[raceSource]} / {formatDate(year, `${month}${day}`)}
          </p>
          <h1>{raceName}</h1>
          <p className="sub-title">
            {formatKeibajo(keibajoCode)} {formatRaceNumber(raceNumber)}{" "}
            {formatTime(race.hassoJikoku)}発走
          </p>
          {raceTags.length > 0 ? (
            <div className="hero-tags">
              {raceTags.map((tag) => (
                <span className="race-tag prominent" key={tag}>
                  {tag}
                </span>
              ))}
            </div>
          ) : null}
        </div>
        <div className="race-badge">
          <span>{formatTrack(race.trackCode)}</span>
          <strong>{formatDistance(race.kyori)}</strong>
        </div>
      </div>

      <section className="detail-grid" aria-label="race details">
        <div className="detail-cell">
          <span>副題</span>
          <strong>{cleanText(race.kyosomeiFukudai)}</strong>
        </div>
        <div className="detail-cell">
          <span>括弧内名称</span>
          <strong>{cleanText(race.kyosomeiKakkonai)}</strong>
        </div>
        <div className="detail-cell">
          <span>条件</span>
          <strong>
            {raceTags.length > 0 ? raceTags.join(" / ") : cleanText(race.kyosoJokenMeisho)}
          </strong>
        </div>
        <div className="detail-cell">
          <span>グレード</span>
          <strong>{cleanText(race.gradeCode)}</strong>
        </div>
        <div className="detail-cell">
          <span>競走記号</span>
          <strong>{cleanText(race.kyosoKigoCode)}</strong>
        </div>
        <div className="detail-cell">
          <span>重量種別</span>
          <strong>{getWeightLabel(race.juryoShubetsuCode)}</strong>
        </div>
        <div className="detail-cell">
          <span>出走頭数</span>
          <strong>{cleanText(race.shussoTosu)} 頭</strong>
        </div>
        <div className="detail-cell">
          <span>登録頭数</span>
          <strong>{cleanText(race.torokuTosu)} 頭</strong>
        </div>
        <div className="detail-cell">
          <span>天候</span>
          <strong>{formatWeather(race.tenkoCode)}</strong>
        </div>
        <div className="detail-cell">
          <span>芝馬場</span>
          <strong>{formatBaba(race.babajotaiCodeShiba)}</strong>
        </div>
        <div className="detail-cell">
          <span>ダート馬場</span>
          <strong>{formatBaba(race.babajotaiCodeDirt)}</strong>
        </div>
      </section>

      <section className="course-section">
        <div className="section-heading compact">
          <h2>コース情報</h2>
          <span>
            {formatKeibajo(keibajoCode)} {formatTrack(race.trackCode)} {formatDistance(race.kyori)}
          </span>
        </div>
        <div className="course-panel">
          <div className="course-summary">
            <span>{formatTrack(race.trackCode)}</span>
            <strong>{formatDistance(race.kyori)}</strong>
            <span>
              改修日{" "}
              {courseInfo
                ? formatDate(
                    courseInfo.courseKaishuNengappi.slice(0, 4),
                    courseInfo.courseKaishuNengappi.slice(4, 8),
                  )
                : "-"}
            </span>
          </div>
          {courseFacts.length > 0 ? (
            <dl className="course-facts">
              {courseFacts.map((fact) => (
                <div key={fact.label}>
                  <dt>{fact.label}</dt>
                  <dd>{fact.value}</dd>
                </div>
              ))}
            </dl>
          ) : null}
          {courseImagePath ? (
            <figure className="course-image">
              <Image
                src={courseImagePath}
                alt={`${formatKeibajo(keibajoCode)} ${formatTrack(race.trackCode)} ${formatDistance(race.kyori)} コース図`}
                width={900}
                height={480}
                sizes="(max-width: 720px) 100vw, 900px"
              />
            </figure>
          ) : null}
          <details className="course-description">
            <summary>コース説明を表示</summary>
            <div>
              {courseParagraphs.map((paragraph) => (
                <p key={paragraph}>{paragraph}</p>
              ))}
            </div>
          </details>
        </div>
      </section>

      <section className="runners-section">
        <div className="section-heading compact">
          <h2>出走馬</h2>
          <span>{runners.length} 頭</span>
        </div>
        {runners.length === 0 ? (
          <p className="empty-state">出走馬情報はまだありません。</p>
        ) : (
          <RunnersTable
            decodeHexHorseWeight={raceSource === "nar" && isBanEiKeibajoCode(keibajoCode)}
            initialRealtimePayload={null}
            realtimeRequest={{
              apiBaseUrl: realtimeApiBaseUrl,
              day,
              keibajoCode,
              month,
              raceNumber,
              source: raceSource,
              year,
            }}
            runners={runners}
          />
        )}
      </section>

      <RealtimeRaceSection
        apiBaseUrl={realtimeApiBaseUrl}
        day={day}
        initialPayload={null}
        keibajoCode={keibajoCode}
        month={month}
        raceNumber={raceNumber}
        runners={runners}
        source={raceSource}
        year={year}
      />

      <LazyDetailSections
        day={day}
        keibajoCode={keibajoCode}
        month={month}
        raceNumber={raceNumber}
        source={raceSource}
        year={year}
      />
    </section>
  );
}
