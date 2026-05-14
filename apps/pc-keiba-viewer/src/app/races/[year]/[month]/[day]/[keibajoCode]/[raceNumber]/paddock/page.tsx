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
} from "../../../../../../../../lib/format";
import { PaddockSection } from "../../../../../../../races/detail/paddock-section";

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
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

  return (
    <section className="page-shell">
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
          <h1>{cleanText(race.kyosomeiHondai, "一般競走")} パドック編集</h1>
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
        editable
        day={day}
        keibajoCode={keibajoCode}
        month={month}
        raceNumber={raceNumber}
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

      <div className="paddock-edit-footer paddock-edit-footer-sticky">
        <Link className="paddock-edit-link" href={raceDetailPath}>
          詳細へ戻る
        </Link>
      </div>
    </section>
  );
}
