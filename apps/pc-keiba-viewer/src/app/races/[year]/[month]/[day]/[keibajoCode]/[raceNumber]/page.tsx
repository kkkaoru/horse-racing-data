import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { getRaceDetail, getRaceSourceByRoute } from "../../../../../../../db/queries";
import { cleanText, formatKeibajo, formatRaceNumber } from "../../../../../../../lib/format";
import { RaceDetailView } from "../../../../../../races/detail/race-detail-page";

export const dynamic = "force-dynamic";

interface RaceDetailRoutePageProps {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
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

const buildMetadataRaceTitle = (race: {
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

export async function generateMetadata({ params }: RaceDetailRoutePageProps): Promise<Metadata> {
  const { day, keibajoCode, month, raceNumber, year } = await params;
  if (!isValidRouteParams(year, month, day, keibajoCode, raceNumber)) {
    return { title: "レース詳細" };
  }
  const source = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!source) {
    return { title: "レース詳細" };
  }
  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
  return {
    title: race
      ? `${buildMetadataRaceTitle(race)} ${formatKeibajo(keibajoCode)} ${formatRaceNumber(raceNumber)}`
      : "レース詳細",
  };
}

export default async function RaceDetailRoutePage({
  params,
  searchParams,
}: RaceDetailRoutePageProps) {
  const { day, keibajoCode, month, raceNumber, year } = await params;
  if (!isValidRouteParams(year, month, day, keibajoCode, raceNumber)) {
    notFound();
  }

  const source = await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!source) {
    notFound();
  }
  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    notFound();
  }

  return (
    <RaceDetailView
      day={day}
      initialRace={race}
      keibajoCode={keibajoCode}
      month={month}
      raceNumber={raceNumber}
      searchParams={await searchParams}
      source={source}
      year={year}
    />
  );
}
