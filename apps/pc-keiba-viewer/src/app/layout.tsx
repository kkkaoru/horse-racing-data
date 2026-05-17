import type { Metadata } from "next";
import Link from "next/link";
import type { ReactNode } from "react";

import { BrandRunner } from "./brand-runner";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "PC-KEIBA Viewer",
    template: "%s | PC-KEIBA Viewer",
  },
  description: "Local PostgreSQL viewer for PC-KEIBA race data",
};

interface RootLayoutProps {
  children: ReactNode;
}

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <html lang="ja">
      <body>
        <header className="app-header">
          <Link className="brand" href="/">
            <BrandRunner />
            <h1 className="brand-title">PC-KEIBA Viewer</h1>
          </Link>
          <details className="header-menu">
            <summary aria-label="メニューを開閉">
              <span aria-hidden="true" />
            </summary>
            <nav aria-label="page navigation">
              <Link href="/races">開催日一覧</Link>
              <Link href="/horses">馬一覧</Link>
              <Link href="/jockeys">騎手一覧</Link>
              <Link href="/owners">馬主一覧</Link>
              <Link href="/trainers">調教師一覧</Link>
              <Link href="/mypage">マイページ</Link>
            </nav>
          </details>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
