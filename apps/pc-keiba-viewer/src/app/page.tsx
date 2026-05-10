import Link from "next/link";

import { getTopRaceWindows } from "../db/queries";
import { HomeRealtime } from "./home-realtime";

export const dynamic = "force-dynamic";

const links = [
  { href: "/races", label: "開催日一覧" },
  { href: "/horses", label: "馬一覧" },
  { href: "/jockeys", label: "騎手一覧" },
  { href: "/trainers", label: "調教師一覧" },
];

export default async function HomePage() {
  const { finished, upcoming } = await getTopRaceWindows();
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

  return (
    <section className="page-shell home-shell">
      <div className="home-heading">
        <p className="eyebrow">PC-KEIBA Viewer</p>
        <h1>レースと成績をすばやく確認</h1>
      </div>
      <nav className="home-link-grid" aria-label="primary pages">
        {links.map((link) => (
          <Link href={link.href} key={link.href}>
            {link.label}
          </Link>
        ))}
      </nav>
      <HomeRealtime
        initialFinished={finished}
        initialUpcoming={upcoming}
        realtimeApiBaseUrl={realtimeApiBaseUrl}
      />
    </section>
  );
}
