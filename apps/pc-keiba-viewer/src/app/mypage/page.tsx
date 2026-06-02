import type { Metadata } from "next";

import { parseFavoritesFromSearchParams } from "../../lib/favorites";
import { MyPageClient } from "./mypage-client";
import { UserIdentityPanel } from "./user-identity-panel";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "マイページ",
};

interface MyPageProps {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export default async function MyPage({ searchParams }: MyPageProps) {
  const params = new URLSearchParams();
  const rawParams = await searchParams;
  for (const [key, value] of Object.entries(rawParams)) {
    if (Array.isArray(value)) {
      for (const item of value) {
        params.append(key, item);
      }
    } else if (value) {
      params.set(key, value);
    }
  }
  const initialFavorites = parseFavoritesFromSearchParams(params);

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">My Page</p>
          <h1>マイページ</h1>
        </div>
        <span className="page-count">{initialFavorites.length} 件</span>
      </div>
      <UserIdentityPanel />
      <MyPageClient initialFavorites={initialFavorites} />
    </section>
  );
}
