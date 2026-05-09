import { notFound } from "next/navigation";

import { getRaceDaySummaries, getRaceYears } from "../../db/queries";
import { RaceCalendar } from "./calendar";

export const dynamic = "force-dynamic";

export default async function RacesPage() {
  const years = await getRaceYears();
  const selectedYear = years[0]?.year;

  if (!selectedYear) {
    notFound();
  }

  const days = await getRaceDaySummaries(selectedYear);
  return <RaceCalendar days={days} selectedYear={selectedYear} years={years} />;
}
