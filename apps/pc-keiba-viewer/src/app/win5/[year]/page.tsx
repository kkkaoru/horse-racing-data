import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { getWin5DaySummaries, getWin5Years } from "../../../lib/win5-queries.server";
import { Win5Calendar } from "../win5-calendar";

export const dynamic = "force-dynamic";

interface Win5YearPageProps {
  params: Promise<{
    year: string;
  }>;
}

export async function generateMetadata({ params }: Win5YearPageProps): Promise<Metadata> {
  const { year } = await params;
  return { title: /^\d{4}$/.test(year) ? `${year}年 WIN5一覧` : "WIN5一覧" };
}

export default async function Win5YearPage({ params }: Win5YearPageProps) {
  const { year } = await params;
  if (!/^\d{4}$/.test(year)) {
    notFound();
  }

  const [years, days] = await Promise.all([getWin5Years(), getWin5DaySummaries(year)]);
  if (days.length === 0) {
    notFound();
  }

  return <Win5Calendar days={days} selectedYear={year} years={years} />;
}
