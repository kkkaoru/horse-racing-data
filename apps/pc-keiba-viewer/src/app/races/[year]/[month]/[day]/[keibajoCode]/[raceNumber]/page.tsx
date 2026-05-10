import { notFound } from "next/navigation";

import { getRaceSourceByRoute } from "../../../../../../../db/queries";
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

  return (
    <RaceDetailView
      day={day}
      keibajoCode={keibajoCode}
      month={month}
      raceNumber={raceNumber}
      searchParams={await searchParams}
      source={source}
      year={year}
    />
  );
}
