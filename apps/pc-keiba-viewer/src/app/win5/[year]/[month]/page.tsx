import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { getWin5DaySummaries } from "../../../../lib/win5-queries.server";
import { Win5MonthList } from "../../win5-calendar";

export const dynamic = "force-dynamic";

interface Win5MonthPageProps {
  params: Promise<{
    year: string;
    month: string;
  }>;
}

export async function generateMetadata({ params }: Win5MonthPageProps): Promise<Metadata> {
  const { month, year } = await params;
  return {
    title:
      /^\d{4}$/.test(year) && /^\d{2}$/.test(month) ? `${year}年${Number(month)}月 WIN5` : "WIN5",
  };
}

export default async function Win5MonthPage({ params }: Win5MonthPageProps) {
  const { year, month } = await params;
  if (!/^\d{4}$/.test(year) || !/^\d{2}$/.test(month)) {
    notFound();
  }

  const days = (await getWin5DaySummaries(year)).filter((day) => day.month === month);
  if (days.length === 0) {
    notFound();
  }

  return <Win5MonthList days={days} month={month} year={year} />;
}
