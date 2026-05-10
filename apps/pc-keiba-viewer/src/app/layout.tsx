import type { Metadata } from "next";
import { M_PLUS_2 } from "next/font/google";
import Link from "next/link";
import type { ReactNode } from "react";

import "./globals.css";

const mPlus2 = M_PLUS_2({
  display: "swap",
  subsets: ["latin"],
  variable: "--font-m-plus-2",
  weight: ["400", "600", "800"],
});

export const metadata: Metadata = {
  title: "PC-KEIBA Viewer",
  description: "Local PostgreSQL viewer for PC-KEIBA race data",
};

interface RootLayoutProps {
  children: ReactNode;
}

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <html className={mPlus2.variable} lang="ja">
      <body>
        <header className="app-header">
          <Link className="brand" href="/">
            <span className="brand-mark" aria-hidden="true" />
            <span>
              <strong>PC-KEIBA Viewer</strong>
            </span>
          </Link>
          <nav>
            <Link href="/races">開催日一覧</Link>
            <Link href="/horses">馬</Link>
            <Link href="/jockeys">騎手</Link>
            <Link href="/trainers">調教師</Link>
          </nav>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
