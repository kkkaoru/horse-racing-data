import Image from "next/image";
import Link from "next/link";
import { notFound } from "next/navigation";

import {
  getBloodlineStats,
  getFinishPositionStats,
  getFrameStats,
  getHorseRaceResults,
  getPayoutStats,
  getRaceCourseInfo,
  getRaceDetail,
  getRaceRunners,
  getRaceTimeStats,
  getRaceTrainings,
  getRacesByDate,
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
  getGradeLabel,
  getRaceSymbolLabel,
  getRaceTags,
  getWeightLabel,
} from "../../../../../../../../../lib/race-classification";
import type {
  FinishPositionStatsRow,
  FrameStatsRow,
  PayoutStatsRow,
  RaceTimeStats,
  SimilarRaceStatsSettings,
} from "../../../../../../../../../lib/race-types";
import { BloodlineStatsTable } from "../../../../../../bloodline-stats-table";
import { HorseRaceResultsTable } from "../../../../../../horse-race-results-table";
import { RaceConditionAnalysisSection } from "../../../../../../race-condition-analysis-section";
import { RealtimeRaceSection } from "../../../../../../realtime-race-section";
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

const hasSearchParam = (
  query: Record<string, string | string[] | undefined>,
  names: string[],
): boolean => names.some((name) => getFirstSearchParam(query[name]) !== undefined);

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

const isJraG1ToG3 = (race: Awaited<ReturnType<typeof getRaceDetail>>): boolean =>
  race?.source === "jra" && ["A", "B", "C"].includes(cleanText(race.gradeCode, ""));

const getStatsClassConditionLabel = (
  race: Awaited<ReturnType<typeof getRaceDetail>>,
): string | null => {
  if (race?.source === "jra" && isListedOrHigher(race)) {
    const label = getGradeLabel(race.gradeCode);
    return label === "-" ? null : label;
  }
  return getClassConditionLabel(race);
};

const getRaceDetailPath = (race: {
  source: RaceSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}): string =>
  `/races/detail/${race.source}/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(0, 2)}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}`;

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

const CONDITION_ANALYSIS_OVERRIDE_PARAMS = [
  "statsAge",
  "statsClass",
  "statsDistance",
  "statsFrame",
  "statsMonthWindow",
  "statsRaceMonth",
  "statsRaceName",
  "statsRaceNumber",
  "statsRaceSubtitle",
  "statsRaceTitle",
  "statsRunnerCount",
  "statsSex",
  "statsSurface",
  "statsTrack",
  "statsTurn",
  "statsVenue",
  "statsWeight",
];

const CONDITION_ANALYSIS_RELAX_KEYS = [
  "includeMonthWindow",
  "includeRaceTitle",
  "includeRaceSubtitle",
  "includeAge",
  "includeClass",
  "includeSex",
  "includeWeight",
  "includeSurface",
  "includeTurn",
  "includeDistance",
  "includeRunnerCount",
  "includeFrame",
  "includeRaceNumber",
] as const;

type ConditionAnalysisStats = [
  RaceTimeStats,
  PayoutStatsRow[],
  FinishPositionStatsRow[],
  FrameStatsRow[],
];

const hasConditionAnalysisRows = (stats: ConditionAnalysisStats): boolean => {
  const [timeStats, payoutRows, finishRows, frameRows] = stats;
  return (
    timeStats.raceCount > 0 ||
    payoutRows.some((row) => row.count > 0) ||
    finishRows.some((row) => row.count > 0) ||
    frameRows.some((row) => row.count > 0)
  );
};

const getConditionAnalysisSettingCandidates = <T extends SimilarRaceStatsSettings>(
  settings: T,
): T[] => {
  const candidates = [settings];
  const relaxedSettings = { ...settings };

  for (const key of CONDITION_ANALYSIS_RELAX_KEYS) {
    if (!relaxedSettings[key]) {
      continue;
    }
    relaxedSettings[key] = false;
    candidates.push({ ...relaxedSettings });
  }

  return candidates;
};

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
  const statsClassConditionLabel = getStatsClassConditionLabel(race);
  const raceNameFilterLabels = getRaceNameFilterLabels(race);
  const raceSymbolLabel = getRaceSymbolLabel(race.kyosoKigoCode);
  const defaultStatsYears = isJraG1ToG3(race) ? null : isListedOrHigher(race) ? 10 : 5;
  const defaultStatsIncludeAge = !getAgeLabel(race.kyosoShubetsuCode).includes("4歳以上");
  const defaultSimilarStatsIncludeSex = raceSymbolLabel !== "牝馬限定";
  const statsSettings = {
    classConditionName: statsClassConditionLabel,
    includeAge: getDefaultFlag(query.statsAge ?? query.statsClass, defaultStatsIncludeAge),
    includeClass: getDefaultFlag(query.statsClass, Boolean(statsClassConditionLabel)),
    includeDistance: banEiRace ? false : getFlag(query.statsDistance),
    includeFrame: getOptionalFlag(query.statsFrame),
    includeMonthWindow: getOptionalFlag(query.statsRaceMonth ?? query.statsMonthWindow),
    includeRaceNumber: getOptionalFlag(query.statsRaceNumber),
    includeRaceSubtitle: getDefaultFlag(
      query.statsRaceSubtitle ?? query.statsRaceName,
      Boolean(raceNameFilterLabels.subtitle),
    ),
    includeRaceTitle: getDefaultFlag(
      query.statsRaceTitle ?? query.statsRaceName,
      Boolean(raceNameFilterLabels.title),
    ),
    includeRunnerCount: false,
    includeSex: getDefaultFlag(query.statsSex, defaultSimilarStatsIncludeSex),
    includeSurface: banEiRace ? false : getFlag(query.statsSurface ?? query.statsTrack),
    includeTurn: banEiRace ? false : getFlag(query.statsTurn ?? query.statsTrack),
    includeVenue: banEiRace ? false : getFlag(query.statsVenue),
    includeWeight: getFlag(query.statsWeight),
    runnerCount: null,
    years: getStatsYears(query.statsYears, defaultStatsYears),
  };
  const bloodlineStatsSettings = {
    ...statsSettings,
    includeRunnerCount: false,
    includeSex: getFlag(query.statsSex),
    runnerCount: null,
    years: getStatsYears(query.statsYears, null),
  };
  const statsConditionLabels = {
    age: getAgeLabel(race.kyosoShubetsuCode),
    class: statsClassConditionLabel,
    distance: banEiRace ? null : formatDistance(race.kyori),
    frame: "枠番号",
    monthWindow: "開催月±1か月",
    raceNumber: formatRaceNumber(race.raceBango),
    raceSubtitle: raceNameFilterLabels.subtitle,
    raceTitle: raceNameFilterLabels.title,
    runnerCount: null,
    sex: raceSymbolLabel.startsWith("競走記号") ? null : raceSymbolLabel,
    surface: banEiRace ? null : getTrackSurfaceLabel(race.trackCode),
    turn: banEiRace ? null : getTrackTurnLabel(race.trackCode),
    venue: banEiRace ? null : formatKeibajo(keibajoCode),
    weight: getWeightLabel(race.juryoShubetsuCode),
  };
  const [courseInfo, runners, raceResults, trainings, raceDayRaces] = await Promise.all([
    getRaceCourseInfo(keibajoCode, race.kyori, race.trackCode),
    getRaceRunners(raceSource, year, month, day, keibajoCode, raceNumber),
    getHorseRaceResults(raceSource, year, month, day, keibajoCode, raceNumber),
    getRaceTrainings(raceSource, year, month, day, keibajoCode, raceNumber),
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
  const parsedRaceRunnerCount = Number(cleanText(race.shussoTosu, "").replace(/[^0-9]/g, ""));
  const currentRunnerCount =
    runners.length > 0
      ? runners.length
      : Number.isFinite(parsedRaceRunnerCount) && parsedRaceRunnerCount > 0
        ? parsedRaceRunnerCount
        : null;
  let conditionAnalysisSettings = {
    ...statsSettings,
    includeRunnerCount: getDefaultFlag(query.statsRunnerCount, currentRunnerCount !== null),
    runnerCount: currentRunnerCount,
    years: getStatsYears(query.statsYears, null),
  };
  const conditionAnalysisLabels = {
    ...statsConditionLabels,
    runnerCount: currentRunnerCount === null ? null : `${currentRunnerCount}頭`,
  };
  const getConditionAnalysisStats = async (settings: typeof conditionAnalysisSettings) =>
    Promise.all([
      getRaceTimeStats(race, settings),
      getPayoutStats(race, settings),
      getFinishPositionStats(race, settings),
      getFrameStats(race, settings),
    ]) satisfies Promise<ConditionAnalysisStats>;
  let conditionAnalysisStats = await getConditionAnalysisStats(conditionAnalysisSettings);
  if (
    !hasSearchParam(query, CONDITION_ANALYSIS_OVERRIDE_PARAMS) &&
    !hasConditionAnalysisRows(conditionAnalysisStats)
  ) {
    const candidates = getConditionAnalysisSettingCandidates(conditionAnalysisSettings).slice(1);
    const candidateStats = await Promise.all(candidates.map(getConditionAnalysisStats));
    const matchedIndex = candidateStats.findIndex(hasConditionAnalysisRows);
    const matchedSettings = candidates[matchedIndex];
    const matchedStats = candidateStats[matchedIndex];
    if (matchedSettings && matchedStats) {
      conditionAnalysisSettings = matchedSettings;
      conditionAnalysisStats = matchedStats;
    }
  }
  const [raceTimeStats, payoutStats, finishPositionStats, frameStats] = conditionAnalysisStats;
  const [bloodlineStats, similarStats] = await Promise.all([
    getBloodlineStats(race, bloodlineStatsSettings),
    getSimilarRaceStats(race, statsSettings),
  ]);
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
      <div className="breadcrumbs">
        <Link href="/races">開催日一覧</Link>
        <Link href={`/races/${year}/${month}/${day}`}>
          {formatDisplayDate(year, `${month}${day}`)}
        </Link>
        <span>
          {formatKeibajo(keibajoCode)} {formatRaceNumber(raceNumber)}
        </span>
      </div>
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
          <RunnersTable runners={runners} />
        )}
      </section>

      <RealtimeRaceSection
        apiBaseUrl={realtimeApiBaseUrl}
        day={day}
        keibajoCode={keibajoCode}
        month={month}
        raceNumber={raceNumber}
        runners={runners}
        source={raceSource}
        year={year}
      />

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
          <h2>同条件レース分析</h2>
          <span>
            {conditionAnalysisSettings.years === null
              ? "全期間"
              : `過去${conditionAnalysisSettings.years}年`}
          </span>
        </div>
        <RaceConditionAnalysisSection
          conditionLabels={conditionAnalysisLabels}
          frameStats={frameStats}
          finishPositionStats={finishPositionStats}
          payoutStats={payoutStats}
          raceTimeStats={raceTimeStats}
          settings={conditionAnalysisSettings}
        />
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
