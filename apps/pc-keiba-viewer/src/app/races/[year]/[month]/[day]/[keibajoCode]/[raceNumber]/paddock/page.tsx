import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { getCloudflareContext } from "@opennextjs/cloudflare";

import {
  getActiveRunningStylePredictions,
  getRaceCourseInfo,
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
  getSameVenueRacesByDate,
  type ActiveRunningStylePrediction,
} from "../../../../../../../../db/queries";
import {
  buildRaceDetailSsrCacheKey,
  getCachedRaceDetailSsrSnapshot,
  putRaceDetailSsrSnapshot,
} from "../../../../../../../../lib/race-detail-ssr-cache.server";
import {
  cleanText,
  formatDate,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  formatTime,
  formatTrack,
  getTrackSurfaceLabel,
} from "../../../../../../../../lib/format";
import { getRaceTags } from "../../../../../../../../lib/race-classification";
import { getRaceRunningStylesWithCache } from "../../../../../../../../lib/running-style-cache.server";
import { isBanEiKeibajoCode } from "../../../../../../../../lib/runner-format";
import { PaddockSection } from "../../../../../../../races/detail/paddock-section";
import { RaceStartCountdown } from "../../../../../../../races/detail/race-start-countdown";

export const dynamic = "force-dynamic";

interface PaddockEditPageProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const isValidRouteParams = (
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
): boolean =>
  /^\d{4}$/.test(year) &&
  /^\d{2}$/.test(month) &&
  /^\d{2}$/.test(day) &&
  /^[0-9A-Z]{2}$/.test(keibajoCode) &&
  /^\d{2}$/.test(raceNumber);

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

const buildFullRaceTitle = (race: {
  kyosomeiFukudai: string | null;
  kyosomeiHondai: string | null;
  kyosomeiKakkonai: string | null;
}): string => {
  const titleParts = [
    cleanText(race.kyosomeiHondai, ""),
    cleanText(race.kyosomeiFukudai, ""),
    cleanText(race.kyosomeiKakkonai, ""),
  ].filter((part) => part.length > 0);
  return titleParts.length > 0 ? titleParts.join(" ") : "一般競走";
};

export async function generateMetadata({ params }: PaddockEditPageProps): Promise<Metadata> {
  const { day, keibajoCode, month, raceNumber, year } = await params;
  if (!isValidRouteParams(year, month, day, keibajoCode, raceNumber)) {
    return { title: "パドック編集" };
  }
  const source = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!source) {
    return { title: "パドック編集" };
  }
  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
  return {
    title: race
      ? `${buildFullRaceTitle(race)} パドック編集 ${formatKeibajo(keibajoCode)} ${formatRaceNumber(raceNumber)}`
      : "パドック編集",
  };
}

export default async function PaddockEditPage({ params }: PaddockEditPageProps) {
  const { day, keibajoCode, month, raceNumber, year } = await params;
  if (!isValidRouteParams(year, month, day, keibajoCode, raceNumber)) {
    notFound();
  }

  const source = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!source) {
    notFound();
  }

  const runningStyleCategory =
    source === "nar" && isBanEiKeibajoCode(keibajoCode) ? "ban-ei" : source;
  const kaisaiTsukihi = `${month}${day}`;
  const runningStylePredictionsPromise: Promise<ActiveRunningStylePrediction[]> =
    runningStyleCategory === "ban-ei"
      ? Promise.resolve([])
      : getActiveRunningStylePredictions({
          category: runningStyleCategory,
          day,
          keibajoCode,
          month,
          raceNumber,
          source,
          year,
        })
          .then(async (rows) => {
            if (rows.length > 0) {
              return rows;
            }
            const d1Rows = await getRaceRunningStylesWithCache({
              kaisaiNen: year,
              kaisaiTsukihi,
              keibajoCode,
              raceBango: raceNumber,
              source,
            }).catch(() => []);
            return d1Rows.map((row) => ({
              horseNumber: row.horseNumber,
              predictedLabel: row.predictedLabel,
            }));
          })
          .catch(() => []);

  // Reuse the race-detail SSR snapshot (race + runners + courseInfo +
  // sameVenueRaces) so the paddock-edit fan-out collapses to a single KV
  // read on warm requests. The cron at `0 12 * * *` / `*/15 * * * *`
  // populates this for upcoming and today's races.
  const ssrCacheKey = buildRaceDetailSsrCacheKey({
    day,
    keibajoCode,
    month,
    raceNumber,
    source,
    year,
  });
  const cachedSnapshot = await getCachedRaceDetailSsrSnapshot(ssrCacheKey);
  const [snapshot, runningStylePredictions] = await Promise.all([
    cachedSnapshot
      ? Promise.resolve(cachedSnapshot)
      : (async () => {
          const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
          if (!race) {
            return null;
          }
          const [courseInfo, runners, sameVenueRaces] = await Promise.all([
            getRaceCourseInfo(keibajoCode, race.kyori, race.trackCode),
            getRaceRunners(source, year, month, day, keibajoCode, raceNumber),
            getSameVenueRacesByDate(source, year, month, day, keibajoCode),
          ]);
          return { courseInfo, race, runners, sameVenueRaces };
        })(),
    runningStylePredictionsPromise,
  ]);
  if (!snapshot) {
    notFound();
  }
  const { race, runners } = snapshot;
  if (!cachedSnapshot) {
    try {
      const cloudflareCtx = (
        await getCloudflareContext<Record<string, unknown>, PcKeibaExecutionContext>({
          async: true,
        })
      ).ctx;
      cloudflareCtx?.waitUntil(
        putRaceDetailSsrSnapshot({
          cacheKey: ssrCacheKey,
          params: { day, keibajoCode, month, raceNumber, source, year },
          snapshot,
        }),
      );
    } catch {
      // Cloudflare context unavailable during local dev — skip cache write.
    }
  }

  const raceDetailPath = `/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}`;
  const raceDetailUrl = `https://pc-keiba-viewer.kkk4oru.com${raceDetailPath}`;
  const raceStartsAt = getRaceStartsAt(year, month, day, race.hassoJikoku);
  const raceTitle = buildFullRaceTitle(race);
  const raceTags = getRaceTags(race);
  const conditionLabel =
    raceTags.length > 0 ? raceTags.join(" / ") : cleanText(race.kyosoJokenMeisho);
  const racePlace = formatKeibajo(keibajoCode);
  const raceNumberLabel = formatRaceNumber(raceNumber);
  const raceStartsAtLabel = `${formatDate(year, `${month}${day}`)} ${formatTime(race.hassoJikoku)}発走`;
  const raceMeta = `${formatDate(year, `${month}${day}`)} ${formatKeibajo(keibajoCode)} ${formatRaceNumber(raceNumber)} / ${formatTime(race.hassoJikoku)}発走 / ${formatTrack(race.trackCode)} ${formatDistance(race.kyori)}`;
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";
  const decodeHexHorseWeight = source === "nar" && isBanEiKeibajoCode(keibajoCode);
  const runningStyleLabelsByHorse = Object.fromEntries(
    runningStylePredictions.map((row) => [String(row.horseNumber), row.predictedLabel]),
  );

  return (
    <main className="page-shell">
      <section className="race-global-summary" aria-label="レース概要">
        <div>
          <RaceStartCountdown startsAt={raceStartsAt} />
          <span>{formatKeibajo(keibajoCode)}</span>
          <span>{formatRaceNumber(raceNumber)}</span>
          {conditionLabel ? (
            <span className="race-global-summary-condition">{conditionLabel}</span>
          ) : null}
          <span>{getTrackSurfaceLabel(race.trackCode) ?? formatTrack(race.trackCode)}</span>
          <span>{formatDistance(race.kyori)}</span>
        </div>
      </section>
      <nav className="breadcrumbs" aria-label="パンくずリスト">
        <Link href="/races">開催日一覧</Link>
        <Link href={`/races/${year}/${month}/${day}/${keibajoCode}`}>
          {formatKeibajo(keibajoCode)}
        </Link>
        <Link href={raceDetailPath}>{formatRaceNumber(raceNumber)}</Link>
        <span>パドック編集</span>
      </nav>

      <header className="page-title-row paddock-edit-title-row">
        <div>
          <p className="eyebrow">
            {formatDate(year, `${month}${day}`)} / {formatTime(race.hassoJikoku)}
          </p>
          <h1>{raceTitle} パドック編集</h1>
          <p className="race-meta">
            {formatKeibajo(keibajoCode)} {formatRaceNumber(raceNumber)} /{" "}
            {formatTrack(race.trackCode)} {formatDistance(race.kyori)}
          </p>
        </div>
        <Link className="paddock-edit-link" href={raceDetailPath}>
          詳細へ戻る
        </Link>
      </header>

      <PaddockSection
        detailUrl={raceDetailUrl}
        decodeHexHorseWeight={decodeHexHorseWeight}
        editFooterDetailPath={raceDetailPath}
        editable
        day={day}
        keibajoCode={keibajoCode}
        month={month}
        raceNumberLabel={raceNumberLabel}
        racePlace={racePlace}
        raceMeta={raceMeta}
        raceNumber={raceNumber}
        raceStartsAt={raceStartsAt}
        raceStartsAtLabel={raceStartsAtLabel}
        raceTitle={raceTitle}
        realtimeRequest={{
          apiBaseUrl: realtimeApiBaseUrl,
          day,
          keibajoCode,
          month,
          raceNumber,
          source,
          year,
        }}
        runningStyleLabelsByHorse={runningStyleLabelsByHorse}
        runners={runners}
        source={source}
        year={year}
      />
    </main>
  );
}
