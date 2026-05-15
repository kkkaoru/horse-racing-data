import Link from "next/link";

import { FavoritesManager } from "./favorites-manager";

export const dynamic = "force-dynamic";

export default function FavoritesPage() {
  return (
    <section className="page-shell">
      <div className="breadcrumbs">
        <Link href="/mypage">マイページ</Link>
        <span>お気に入り管理</span>
      </div>
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Favorites</p>
          <h1>お気に入り管理</h1>
        </div>
      </div>
      <FavoritesManager />
    </section>
  );
}
