import type { Metadata } from "next";

import { getPersonList } from "../../db/queries";
import { EntityFilterForm, parseEntityListQuery, PersonListTable } from "../entity-ui";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "馬主一覧",
};

interface OwnersPageProps {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export default async function OwnersPage({ searchParams }: OwnersPageProps) {
  const query = parseEntityListQuery(await searchParams);
  const rows = await getPersonList("owners", query);

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Owners</p>
          <h1>馬主一覧</h1>
        </div>
        <span className="page-count">{rows.length} 件</span>
      </div>
      <EntityFilterForm action="/owners" query={query} searchPlaceholder="馬主名" />
      <PersonListTable basePath="/owners" rows={rows} />
    </section>
  );
}
