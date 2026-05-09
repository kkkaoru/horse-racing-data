import Image from "next/image";
import Link from "next/link";
import { notFound } from "next/navigation";

import {
  getBloodlineStats,
  getHorseRaceResults,
  getRaceCourseInfo,
  getRaceDetail,
  getRaceRunners,
  getRaceTrainings,
  getSimilarRaceStats,
} from "../../../../../../../../../db/queries";
import { SOURCE_LABELS, type RaceSource } from "../../../../../../../../../lib/codes";
import {
  formatCourseParagraphs,
  getCourseFacts,
  getCourseImagePath,
} from "../../../../../../../../../lib/course";
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
  getTrackTurnLabel,
} from "../../../../../../../../../lib/format";
import {
  getAgeLabel,
  getConditionLabel,
  getRaceSymbolLabel,
  getRaceTags,
  getWeightLabel,
} from "../../../../../../../../../lib/race-classification";
import { BloodlineStatsTable } from "../../../../../../bloodline-stats-table";
import { HorseRaceResultsTable } from "../../../../../../horse-race-results-table";
import { RunnersTable } from "../../../../../../runners-table";
import { SimilarRaceStatsTable } from "../../../../../../similar-race-stats-table";
import { TrainingTable } from "../../../../../../training-table";

export const dynamic = "force-dynamic";

interface RaceDetailPageProps {
  params: Promise<{
    source: string;
    year: string;
    month: string;
    day: string;
    keibajoCode: string;
    raceNumber: string;
  }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

const isRaceSource = (source: string): source is RaceSource => source === "jra" || source === "nar";

const isValidParams = (
  source: string,
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
): boolean =>
  isRaceSource(source) &&
  /^\d{4}$/.test(year) &&
  /^\d{2}$/.test(month) &&
  /^\d{2}$/.test(day) &&
  /^[0-9A-Z]{2}$/.test(keibajoCode) &&
  /^\d{2}$/.test(raceNumber);

const getFirstSearchParam = (value: string | string[] | undefined): string | undefined =>
  Array.isArray(value) ? value[0] : value;

const getFlag = (value: string | string[] | undefined): boolean =>
  getFirstSearchParam(value) !== "0";

const getOptionalFlag = (value: string | string[] | undefined): boolean =>
  getFirstSearchParam(value) === "1";

const getDefaultFlag = (value: string | string[] | undefined, defaultValue: boolean): boolean => {
  const firstValue = getFirstSearchParam(value);
  if (firstValue === undefined) {
    return defaultValue;
  }
  return firstValue !== "0";
};

const LISTED_OR_HIGHER_GRADE_CODES = new Set(["A", "B", "C", "D", "F", "G", "H", "L", "S"]);

const getStatsYears = (
  value: string | string[] | undefined,
  defaultYears: number | null,
): number | null => {
  const firstValue = getFirstSearchParam(value);
  if (firstValue === "all" || (firstValue === undefined && defaultYears === null)) {
    return null;
  }

  const parsed = Number(firstValue ?? String(defaultYears));
  return Number.isInteger(parsed) && parsed >= 1 && parsed <= 10 ? parsed : defaultYears;
};

const cleanConditionText = (value: string | null | undefined): string =>
  cleanText(value, "").replace(/\s+/g, " ").replace(/　+/g, " ").trim();

const getLocalConditionLabel = (value: string | null | undefined): string => {
  const cleaned = cleanConditionText(value);
  const normalized = cleaned
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, (char) => String.fromCharCode(char.charCodeAt(0) - 0xfee0))
    .replace(/[－ー―‐]/g, "-");
  if (/OP/.test(normalized)) {
    const opMatch = cleaned.match(/[ＯO]Ｐ|OP/i);
    return opMatch?.[0] ?? "OP";
  }
  return cleaned.split(" ")[0] ?? "";
};

const getClassConditionLabel = (race: Awaited<ReturnType<typeof getRaceDetail>>): string | null => {
  if (!race) {
    return null;
  }
  if (race.source === "nar" && cleanText(race.kyosoJokenCode, "") === "000") {
    return getLocalConditionLabel(race.kyosoJokenMeisho) || null;
  }
  const label = getConditionLabel(race.kyosoJokenCode);
  return label === "-" ? null : label;
};

const getRaceNameFilterLabels = (
  race: Awaited<ReturnType<typeof getRaceDetail>>,
): { subtitle: string | null; title: string | null } => {
  if (!race) {
    return { subtitle: null, title: null };
  }

  const tags = getRaceTags(race).join(" ");
  const grade = cleanText(race.gradeCode, "");
  const condition = cleanConditionText(race.kyosoJokenMeisho);
  const hasNamedClass =
    grade.length > 0 || /G[1-3]|Jpn[1-3]|リステッド|OP|ＯＰ|オープン/.test(`${tags} ${condition}`);

  if (!hasNamedClass) {
    return { subtitle: null, title: null };
  }

  const title = cleanText(race.kyosomeiHondai, "");
  const subtitle = cleanText(race.kyosomeiFukudai, "") || cleanText(race.kyosomeiKakkonai, "");
  return {
    subtitle: subtitle || null,
    title: title || null,
  };
};

const isBanEi = (race: Awaited<ReturnType<typeof getRaceDetail>>): boolean =>
  race?.source === "nar" && ["81", "82", "83", "84"].includes(race.keibajoCode);

const isListedOrHigher = (race: Awaited<ReturnType<typeof getRaceDetail>>): boolean =>
  race ? LISTED_OR_HIGHER_GRADE_CODES.has(cleanText(race.gradeCode, "")) : false;

export default async function RaceDetailPage({ params, searchParams }: RaceDetailPageProps) {
  const { source, year, month, day, keibajoCode, raceNumber } = await params;
  const query = await searchParams;
  if (!isValidParams(source, year, month, day, keibajoCode, raceNumber)) {
    notFound();
  }

  const raceSource = isRaceSource(source) ? source : notFound();
  const race = await getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber);

  if (!race) {
    notFound();
  }

  const raceName = cleanText(race.kyosomeiHondai, "一般競走");
  const raceTags = getRaceTags(race);
  const banEiRace = isBanEi(race);
  const classConditionLabel = getClassConditionLabel(race);
  const raceNameFilterLabels = getRaceNameFilterLabels(race);
  const raceSymbolLabel = getRaceSymbolLabel(race.kyosoKigoCode);
  const defaultStatsYears = isListedOrHigher(race) ? 10 : 5;
  const statsSettings = {
    classConditionName: classConditionLabel,
    includeAge: getFlag(query.statsAge ?? query.statsClass),
    includeClass: getDefaultFlag(query.statsClass, Boolean(classConditionLabel)),
    includeDistance: banEiRace ? false : getFlag(query.statsDistance),
    includeFrame: getOptionalFlag(query.statsFrame),
    includeRaceNumber: getOptionalFlag(query.statsRaceNumber),
    includeRaceSubtitle: getDefaultFlag(
      query.statsRaceSubtitle ?? query.statsRaceName,
      Boolean(raceNameFilterLabels.subtitle),
    ),
    includeRaceTitle: getDefaultFlag(
      query.statsRaceTitle ?? query.statsRaceName,
      Boolean(raceNameFilterLabels.title),
    ),
    includeSex: getFlag(query.statsSex),
    includeSurface: banEiRace ? false : getFlag(query.statsSurface ?? query.statsTrack),
    includeTurn: banEiRace ? false : getFlag(query.statsTurn ?? query.statsTrack),
    includeVenue: banEiRace ? false : getFlag(query.statsVenue),
    years: getStatsYears(query.statsYears, defaultStatsYears),
  };
  const bloodlineStatsSettings = {
    ...statsSettings,
    years: getStatsYears(query.statsYears, null),
  };
  const statsConditionLabels = {
    age: getAgeLabel(race.kyosoShubetsuCode),
    class: classConditionLabel,
    distance: banEiRace ? null : formatDistance(race.kyori),
    frame: "枠番号",
    raceNumber: formatRaceNumber(race.raceBango),
    raceSubtitle: raceNameFilterLabels.subtitle,
    raceTitle: raceNameFilterLabels.title,
    sex: raceSymbolLabel.startsWith("競走記号") ? null : raceSymbolLabel,
    surface: banEiRace ? null : getTrackSurfaceLabel(race.trackCode),
    turn: banEiRace ? null : getTrackTurnLabel(race.trackCode),
    venue: banEiRace ? null : formatKeibajo(keibajoCode),
  };
  const [courseInfo, runners, raceResults, trainings, bloodlineStats, similarStats] =
    await Promise.all([
      getRaceCourseInfo(keibajoCode, race.kyori, race.trackCode),
      getRaceRunners(raceSource, year, month, day, keibajoCode, raceNumber),
      getHorseRaceResults(raceSource, year, month, day, keibajoCode, raceNumber),
      getRaceTrainings(raceSource, year, month, day, keibajoCode, raceNumber),
      getBloodlineStats(race, bloodlineStatsSettings),
      getSimilarRaceStats(race, statsSettings),
    ]);
  const courseText = cleanText(courseInfo?.courseSetsumei, "");
  const courseFacts = getCourseFacts(courseText, race.kyori, race.trackCode);
  const courseParagraphs = courseText
    ? formatCourseParagraphs(courseText)
    : ["このコースの説明データは見つかりませんでした。"];
  const courseImagePath = getCourseImagePath(keibajoCode, race.trackCode, race.kyori);

  return (
    <section className="page-shell">
      <div className="breadcrumbs">
        <Link href="/races">開催日一覧</Link>
        <Link href={`/races/${year}/${month}/${day}`}>
          {formatDisplayDate(year, `${month}${day}`)}
        </Link>
        <span>
          {formatKeibajo(keibajoCode)} {formatRaceNumber(raceNumber)}
        </span>
      </div>

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
          <h2>走るコース</h2>
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
          <RunnersTable runners={runners} />
        )}
      </section>

      <section className="race-results-section">
        <div className="section-heading compact">
          <h2>競走成績</h2>
          <span>{raceResults.length} 件</span>
        </div>
        <HorseRaceResultsTable
          currentDistance={race.kyori}
          currentKeibajoCode={race.keibajoCode}
          results={raceResults}
          runners={runners}
        />
      </section>

      <section className="training-section">
        <div className="section-heading compact">
          <h2>調教・追い切り</h2>
          <span>{trainings.length} 件</span>
        </div>
        <TrainingTable sourceLabel={SOURCE_LABELS[raceSource]} trainings={trainings} />
      </section>

      <section className="similar-stats-section">
        <div className="section-heading compact">
          <h2>血統成績</h2>
          <span>
            {bloodlineStatsSettings.years === null
              ? "全期間"
              : `過去${bloodlineStatsSettings.years}年`}
          </span>
        </div>
        <BloodlineStatsTable
          conditionLabels={statsConditionLabels}
          rows={bloodlineStats}
          runners={runners}
          settings={bloodlineStatsSettings}
        />
      </section>

      <section className="similar-stats-section">
        <div className="section-heading compact">
          <h2>同条件成績</h2>
          <span>{statsSettings.years === null ? "全期間" : `過去${statsSettings.years}年`}</span>
        </div>
        <SimilarRaceStatsTable
          conditionLabels={statsConditionLabels}
          rows={similarStats}
          settings={statsSettings}
        />
      </section>
    </section>
  );
}
