import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { getRaceDaySummaries, getRaceYears } from "../../../db/queries";
import { RaceCalendar } from "../calendar";

export const dynamic = "force-dynamic";

interface RaceYearPageProps {
  params: Promise<{
    year: string;
  }>;
}

export async function generateMetadata({ params }: RaceYearPageProps): Promise<Metadata> {
  const { year } = await params;
  return { title: /^\d{4}$/.test(year) ? `${year}年 開催日一覧` : "開催日一覧" };
}

export default async function RaceYearPage({ params }: RaceYearPageProps) {
  const { year } = await params;
  if (!/^\d{4}$/.test(year)) {
    notFound();
  }

  const [years, days] = await Promise.all([getRaceYears(), getRaceDaySummaries(year)]);
  if (days.length === 0) {
    notFound();
  }

  return <RaceCalendar days={days} selectedYear={year} years={years} />;
}
