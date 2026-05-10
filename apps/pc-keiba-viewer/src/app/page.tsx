import Link from "next/link";
import { Suspense } from "react";

import { getTopRaceWindows } from "../db/queries";
import { HomeRealtime } from "./home-realtime";

export const dynamic = "force-dynamic";

const links = [
  { href: "/races", label: "開催日一覧" },
  { href: "/horses", label: "馬一覧" },
  { href: "/jockeys", label: "騎手一覧" },
  { href: "/trainers", label: "調教師一覧" },
];

async function HomeRealtimePanel() {
  const { finished, upcoming } = await getTopRaceWindows();
  const realtimeApiBaseUrl =
    process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

  return (
    <HomeRealtime
      initialFinished={finished}
      initialUpcoming={upcoming}
      realtimeApiBaseUrl={realtimeApiBaseUrl}
    />
  );
}

function HomeRealtimeSkeleton() {
  return (
    <div className="home-live-grid" aria-label="トップページのレース情報を読み込み中">
      {["次のレース", "直近の発走済み", "オッズ更新"].map((title, index) => (
        <section className={index === 2 ? "home-panel home-panel-wide" : "home-panel"} key={title}>
          <div className="section-heading compact">
            <h2>{title}</h2>
            <span>読み込み中</span>
          </div>
          <div className="home-race-list home-race-list-skeleton">
            {Array.from({ length: index === 2 ? 3 : 5 }, (_, itemIndex) => (
              <div className="home-race-skeleton-item" key={itemIndex}>
                <span className="skeleton-text short" />
                <span className="skeleton-text medium" />
              </div>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

export default function HomePage() {
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
      <Suspense fallback={<HomeRealtimeSkeleton />}>
        <HomeRealtimePanel />
      </Suspense>
    </section>
  );
}
