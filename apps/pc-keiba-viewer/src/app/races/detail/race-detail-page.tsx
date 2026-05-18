import type { RealtimeRacePayload } from "horse-racing-realtime/types";
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
import { isJraResultLinkAvailable } from "../../../lib/jra-link-visibility";
import { buildJraRaceEntryUrl, buildJraRaceResultUrl } from "../../../lib/jra-url";
import {
  getGradeLabel,
  getRaceSymbolDetailLabel,
  getRaceTags,
  getWeightLabel,
} from "../../../lib/race-classification";
import { isCornerPacePredictionSupported } from "../../../lib/race-pace-prediction";
import type { RaceDetail } from "../../../lib/race-types";
import {
  formatCarriedWeight,
  formatHorseWeight,
  formatRunnerNumber,
  formatRunnerValue,
  formatSexAge,
  isBanEiKeibajoCode,
} from "../../../lib/runner-format";
import { AiJsonExportSection } from "./ai-json-export-section";
import {
  LazyDetailSections,
  LazyFinishPredictionSection,
  LazyRacePacePredictionSection,
} from "./lazy-detail-sections";
import { PaddockSection } from "./paddock-section";
import { RaceAiAssistant } from "./race-ai-assistant";
import { RaceShareControls } from "./race-share-controls";
import { RaceStartCountdown } from "./race-start-countdown";
import { RaceTrendSection } from "./race-trend-section";
import { RealtimeRaceProvider, type RealtimeRaceRequest } from "./realtime-client";
import { RealtimeRaceSection } from "./realtime-race-section";
import { RunnersTable } from "./runners-table";
import { RunningStyleRaceSection } from "./running-style-race-section";
import { TrackConditionSection } from "./track-condition-section";

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

const addDaysToIsoDate = (year: string, month: string, day: string, days: number): string => {
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
  date.setUTCDate(date.getUTCDate() + days);
  return [
    String(date.getUTCFullYear()).padStart(4, "0"),
    String(date.getUTCMonth() + 1).padStart(2, "0"),
    String(date.getUTCDate()).padStart(2, "0"),
  ].join("-");
};

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

const hasDetailValue = (value: string | null | undefined): boolean => {
  const normalized = cleanText(value, "").replace(/\s+/g, "").replace(/　+/g, "");
  return (
    normalized !== "" && normalized !== "-" && normalized !== "未設定" && !/^0+$/.test(normalized)
  );
};

const getRaceStartsAt = (
  year: string,
  month: string,
  day: string,
  hassoJikoku: string | null,
): string | null => {
  const normalizedTime = cleanText(hassoJikoku, "").padStart(4, "0");
  if (!/^\d{4}$/.test(normalizedTime)) {
    return null;
  }

  const hour = normalizedTime.slice(0, 2);
  const minute = normalizedTime.slice(2, 4);
  return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}T${hour}:${minute}:00+09:00`;
};

const formatStoredOddsForExport = (value: string | null | undefined): string => {
  const cleaned = cleanText(value, "");
  if (!cleaned || cleaned === "0000") {
    return "-";
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? (parsed / 10).toFixed(1) : "-";
};

const isRealtimeRacePayload = (value: unknown): value is RealtimeRacePayload =>
  typeof value === "object" &&
  value !== null &&
  "raceKey" in value &&
  typeof value.raceKey === "string";

const fetchInitialRealtimePayload = async ({
  apiBaseUrl,
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: {
  apiBaseUrl: string;
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}): Promise<RealtimeRacePayload | null> => {
  const realtimeUrl = buildRealtimeRaceUrl({
    apiBaseUrl,
    day,
    keibajoCode,
    month,
    raceNumber,
    source,
    year,
  });
  if (!realtimeUrl) {
    return null;
  }
  try {
    const response = await fetch(realtimeUrl, { cache: "no-store" });
    if (!response.ok) {
      return null;
    }
    const data: unknown = await response.json();
    return isRealtimeRacePayload(data) ? data : null;
  } catch {
    return null;
  }
};

const buildRealtimeRaceUrl = ({
  apiBaseUrl,
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: RealtimeRaceRequest): string | null => {
  if (source !== "nar" && source !== "jra") {
    return null;
  }
  if (!apiBaseUrl) {
    return null;
  }
  return `${apiBaseUrl.replace(/\/$/u, "")}/api/${source}/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/realtime`;
};

const DetailCell = ({
  label,
  suffix = "",
  value,
}: {
  label: string;
  suffix?: string;
  value: string | null | undefined;
}) => {
  if (!hasDetailValue(value)) {
    return null;
  }

  return (
    <div className="detail-cell">
      <span>{label}</span>
      <strong>
        {cleanText(value)}
        {suffix}
      </strong>
    </div>
  );
};

const formatCountValue = (value: string | null | undefined): string | null => {
  if (!hasDetailValue(value)) {
    return null;
  }

  const normalized = cleanText(value);
  const parsed = Number(normalized);
  return Number.isInteger(parsed) ? String(parsed) : normalized;
};

const hideUnspecifiedDetailValue = (value: string): string | null =>
  value === "指定なし" || value === "制限なし" ? null : value;

const DetailLinkCell = ({
  href,
  label,
  value,
}: {
  href: string | null;
  label: string;
  value: string;
}) => {
  if (!href) {
    return null;
  }

  return (
    <div className="detail-cell">
      <span>{label}</span>
      <strong>
        <a href={href} rel="noreferrer" target="_blank">
          {value}
        </a>
      </strong>
    </div>
  );
};

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
  const conditionLabel =
    raceTags.length > 0 ? raceTags.join(" / ") : cleanText(race.kyosoJokenMeisho);
  const courseFacts = getCourseFacts(courseText, race.kyori, race.trackCode);
  const courseParagraphs = courseText
    ? formatCourseParagraphs(courseText)
    : ["このコースの説明データは見つかりませんでした。"];
  const courseImagePath = getCourseImagePath(keibajoCode, race.trackCode, race.kyori);
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";
  const realtimeRequest = {
    apiBaseUrl: realtimeApiBaseUrl,
    day,
    keibajoCode,
    month,
    raceNumber,
    source: raceSource,
    year,
  } satisfies RealtimeRaceRequest;
  const initialRealtimePayload = await fetchInitialRealtimePayload(realtimeRequest);
  const raceStartsAt = getRaceStartsAt(year, month, day, race.hassoJikoku);
  const sharePath = getRaceDetailPath({
    kaisaiNen: year,
    kaisaiTsukihi: `${month}${day}`,
    keibajoCode,
    raceBango: raceNumber,
    source: raceSource,
  });
  const jraRaceEntryUrl = buildJraRaceEntryUrl(race);
  const jraRaceResultUrl = buildJraRaceResultUrl(race);
  const showJraResultLink = raceSource === "jra" && isJraResultLinkAvailable(year, month, day);
  const visibleJraRaceEntryUrl = showJraResultLink ? null : jraRaceEntryUrl;
  const visibleJraRaceResultUrl = showJraResultLink ? jraRaceResultUrl : null;
  const decodeHexHorseWeight = raceSource === "nar" && isBanEiKeibajoCode(keibajoCode);
  const raceTrendDefaultStartDate = addDaysToIsoDate(
    year,
    month,
    day,
    raceSource === "jra" ? -1 : -3,
  );
  const raceTrendDefaultEndDate = `${year}-${month}-${day}`;
  const showRacePacePrediction = isCornerPacePredictionSupported({
    distance: race.kyori,
    keibajoCode,
    source: raceSource,
  });
  const baseProcessedData = {
    adjacentRaces: {
      next: nextRace
        ? {
            label: getAdjacentRaceLabel(nextRace),
            path: getRaceDetailPath(nextRace),
            race: nextRace,
          }
        : null,
      previous: previousRace
        ? {
            label: getAdjacentRaceLabel(previousRace),
            path: getRaceDetailPath(previousRace),
            race: previousRace,
          }
        : null,
    },
    course: {
      facts: courseFacts,
      imagePath: courseImagePath,
      paragraphs: courseParagraphs,
      text: courseText,
    },
    detailCells: {
      condition: conditionLabel,
      dirtCondition: formatBaba(race.babajotaiCodeDirt),
      entryUrl: visibleJraRaceEntryUrl,
      grade: getGradeLabel(race.gradeCode, race.source),
      raceSymbol: getRaceSymbolDetailLabel(race.kyosoKigoCode),
      registeredRunnerCount: race.torokuTosu,
      resultUrl: visibleJraRaceResultUrl,
      runnerCount: race.shussoTosu,
      turfCondition: formatBaba(race.babajotaiCodeShiba),
      weather: formatWeather(race.tenkoCode),
      weightType: getWeightLabel(race.juryoShubetsuCode),
    },
    globalSummary: {
      distance: formatDistance(race.kyori),
      raceNumber: formatRaceNumber(raceNumber),
      startsAt: raceStartsAt,
      startTime: `${formatTime(race.hassoJikoku)}発走`,
      surface: getTrackSurfaceLabel(race.trackCode) ?? formatTrack(race.trackCode),
      venue: formatKeibajo(keibajoCode),
    },
    hero: {
      badgeDistance: formatDistance(race.kyori),
      badgeTrack: formatTrack(race.trackCode),
      date: formatDate(year, `${month}${day}`),
      raceName,
      sourceLabel: SOURCE_LABELS[raceSource],
      subtitle: `${formatKeibajo(keibajoCode)} ${formatRaceNumber(raceNumber)} ${formatTime(
        race.hassoJikoku,
      )}発走`,
      tags: raceTags,
    },
    runnerRows: runners.map((runner) => ({
      carriedWeight: formatCarriedWeight(runner.futanJuryo, decodeHexHorseWeight),
      finishOrder: formatRunnerValue(runner.kakuteiChakujun, "00"),
      frameNumber: cleanText(runner.wakuban),
      horseName: cleanText(runner.bamei),
      horseNumber: formatRunnerNumber(runner.umaban),
      horseWeight: formatHorseWeight(
        runner.bataiju,
        runner.zogenFugo,
        runner.zogenSa,
        decodeHexHorseWeight,
      ),
      jockeyName: cleanText(runner.kishumeiRyakusho),
      ownerName: cleanText(runner.banushimei),
      sexAge: formatSexAge(runner.seibetsuCode, runner.barei),
      storedWinOdds: formatStoredOddsForExport(runner.tanshoOdds),
      trainerName: cleanText(runner.chokyoshimeiRyakusho),
    })),
    sharePath,
  };
  return (
    <RealtimeRaceProvider initialPayload={initialRealtimePayload} request={realtimeRequest}>
      <section className="page-shell">
        <RaceShareControls path={sharePath} />
        <div className="race-global-summary" aria-label="race summary in global header">
          <div>
            <span>{formatTime(race.hassoJikoku)}発走</span>
            <RaceStartCountdown startsAt={raceStartsAt} />
            <span>{formatKeibajo(keibajoCode)}</span>
            <span>{formatRaceNumber(raceNumber)}</span>
            {conditionLabel ? (
              <span className="race-global-summary-condition">{conditionLabel}</span>
            ) : null}
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
          <Link href={`/races/${year}`}>{year}年</Link>
          <Link href={`/races/${year}/${month}`}>{Number(month)}月</Link>
          <Link href={`/races/${year}/${month}/${day}`}>
            {formatDisplayDate(year, `${month}${day}`)}
          </Link>
          <Link href={`/races/${year}/${month}/${day}/${keibajoCode}`}>
            {formatKeibajo(keibajoCode)}
          </Link>
          <span>{formatRaceNumber(raceNumber)}</span>
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
          <DetailCell label="副題" value={race.kyosomeiFukudai} />
          <DetailCell label="括弧内名称" value={race.kyosomeiKakkonai} />
          <DetailCell label="条件" value={conditionLabel} />
          <DetailCell
            label={race.source === "nar" ? "重賞種別" : "グレード"}
            value={getGradeLabel(race.gradeCode, race.source)}
          />
          <DetailCell
            label="競走記号"
            value={hideUnspecifiedDetailValue(getRaceSymbolDetailLabel(race.kyosoKigoCode))}
          />
          <DetailCell
            label="重量種別"
            value={hideUnspecifiedDetailValue(getWeightLabel(race.juryoShubetsuCode))}
          />
          <DetailCell label="出走頭数" suffix=" 頭" value={formatCountValue(race.shussoTosu)} />
          <DetailCell label="登録頭数" suffix=" 頭" value={formatCountValue(race.torokuTosu)} />
          <DetailLinkCell href={visibleJraRaceEntryUrl} label="JRA出馬表" value="公式ページ" />
          <DetailLinkCell href={visibleJraRaceResultUrl} label="JRA成績" value="公式ページ" />
          <DetailCell label="天候" value={formatWeather(race.tenkoCode)} />
          <DetailCell label="芝馬場" value={formatBaba(race.babajotaiCodeShiba)} />
          <DetailCell label="ダート馬場" value={formatBaba(race.babajotaiCodeDirt)} />
        </section>

        <TrackConditionSection trackCode={race.trackCode} />

        <RaceAiAssistant
          basePostgresqlData={{
            courseInfo,
            race,
            raceDayRaces,
            runners,
          }}
          baseProcessedData={baseProcessedData}
          day={day}
          keibajoCode={keibajoCode}
          month={month}
          raceNumber={raceNumber}
          source={raceSource}
          year={year}
        />

        <PaddockSection
          day={day}
          decodeHexHorseWeight={decodeHexHorseWeight}
          keibajoCode={keibajoCode}
          month={month}
          raceNumber={raceNumber}
          runners={runners}
          source={race.source}
          year={year}
        />

        <RaceTrendSection
          day={day}
          defaultEndDate={raceTrendDefaultEndDate}
          defaultStartDate={raceTrendDefaultStartDate}
          keibajoCode={keibajoCode}
          month={month}
          raceNumber={raceNumber}
          source={raceSource}
          year={year}
        />

        <section className="course-section">
          <div className="section-heading compact">
            <h2>コース情報</h2>
            <span>
              {formatKeibajo(keibajoCode)} {formatTrack(race.trackCode)}{" "}
              {formatDistance(race.kyori)}
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
          </div>
          {runners.length === 0 ? (
            <p className="empty-state">出走馬情報はまだありません。</p>
          ) : (
            <RunnersTable
              decodeHexHorseWeight={decodeHexHorseWeight}
              initialRealtimePayload={initialRealtimePayload}
              realtimeRequest={realtimeRequest}
              runners={runners}
            />
          )}
        </section>

        <RunningStyleRaceSection
          category={raceSource === "nar" && isBanEiKeibajoCode(keibajoCode) ? "ban-ei" : raceSource}
          kaisaiNen={year}
          kaisaiTsukihi={`${month.padStart(2, "0")}${day.padStart(2, "0")}`}
          keibajoCode={keibajoCode}
          raceBango={raceNumber}
          runnersByUmaban={Object.fromEntries(
            runners.map((runner) => [
              Number(runner.umaban ?? "0"),
              {
                bamei: cleanText(runner.bamei, "") || null,
                jockey: cleanText(runner.kishumeiRyakusho, "") || null,
              },
            ]),
          )}
          source={raceSource}
        />

        {showRacePacePrediction ? (
          <LazyRacePacePredictionSection
            day={day}
            keibajoCode={keibajoCode}
            month={month}
            raceNumber={raceNumber}
            realtimeApiBaseUrl={realtimeApiBaseUrl}
            source={raceSource}
            year={year}
          />
        ) : null}

        <LazyFinishPredictionSection
          day={day}
          keibajoCode={keibajoCode}
          month={month}
          raceNumber={raceNumber}
          realtimeApiBaseUrl={realtimeApiBaseUrl}
          source={raceSource}
          year={year}
        />

        <RealtimeRaceSection
          apiBaseUrl={realtimeApiBaseUrl}
          day={day}
          initialPayload={initialRealtimePayload}
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
          realtimeApiBaseUrl={realtimeApiBaseUrl}
          source={raceSource}
          year={year}
        />
        <AiJsonExportSection
          basePostgresqlData={{
            courseInfo,
            race,
            raceDayRaces,
            runners,
          }}
          baseProcessedData={baseProcessedData}
          day={day}
          keibajoCode={keibajoCode}
          month={month}
          raceNumber={raceNumber}
          source={raceSource}
          year={year}
        />
      </section>
    </RealtimeRaceProvider>
  );
}
