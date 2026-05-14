import Link from "next/link";
import { notFound } from "next/navigation";

import {
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
} from "../../../../../../../../db/queries";
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

export default async function PaddockEditPage({ params }: PaddockEditPageProps) {
  const { day, keibajoCode, month, raceNumber, year } = await params;
  if (!isValidRouteParams(year, month, day, keibajoCode, raceNumber)) {
    notFound();
  }

  const source = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!source) {
    notFound();
  }

  const [race, runners] = await Promise.all([
    getRaceDetail(source, year, month, day, keibajoCode, raceNumber),
    getRaceRunners(source, year, month, day, keibajoCode, raceNumber),
  ]);
  if (!race) {
    notFound();
  }

  const raceDetailPath = `/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}`;
  const raceDetailUrl = `https://pc-keiba-viewer.kkk4oru.com${raceDetailPath}`;
  const raceStartsAt = getRaceStartsAt(year, month, day, race.hassoJikoku);
  const raceTitle = cleanText(race.kyosomeiHondai, "一般競走");
  const racePlace = formatKeibajo(keibajoCode);
  const raceNumberLabel = formatRaceNumber(raceNumber);
  const raceStartsAtLabel = `${formatDate(year, `${month}${day}`)} ${formatTime(race.hassoJikoku)}発走`;
  const raceMeta = `${formatDate(year, `${month}${day}`)} ${formatKeibajo(keibajoCode)} ${formatRaceNumber(raceNumber)} / ${formatTime(race.hassoJikoku)}発走 / ${formatTrack(race.trackCode)} ${formatDistance(race.kyori)}`;
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

  return (
    <section className="page-shell">
      <div className="race-global-summary" aria-label="race summary in global header">
        <div>
          <span>{formatTime(race.hassoJikoku)}発走</span>
          <RaceStartCountdown startsAt={raceStartsAt} />
          <span>{formatKeibajo(keibajoCode)}</span>
          <span>{formatRaceNumber(raceNumber)}</span>
          <span>{getTrackSurfaceLabel(race.trackCode) ?? formatTrack(race.trackCode)}</span>
          <span>{formatDistance(race.kyori)}</span>
        </div>
      </div>
      <div className="breadcrumbs">
        <Link href="/races">開催日一覧</Link>
        <Link href={`/races/${year}/${month}/${day}/${keibajoCode}`}>
          {formatKeibajo(keibajoCode)}
        </Link>
        <Link href={raceDetailPath}>{formatRaceNumber(raceNumber)}</Link>
        <span>パドック編集</span>
      </div>

      <div className="page-title-row">
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
      </div>

      <PaddockSection
        detailUrl={raceDetailUrl}
        editFooterDetailPath={raceDetailPath}
        editable
        day={day}
        keibajoCode={keibajoCode}
        month={month}
        raceNumberLabel={raceNumberLabel}
        racePlace={racePlace}
        raceMeta={raceMeta}
        raceNumber={raceNumber}
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
        runners={runners}
        year={year}
      />
    </section>
  );
}
