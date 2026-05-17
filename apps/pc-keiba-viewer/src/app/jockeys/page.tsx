import type { Metadata } from "next";

import { getPersonList } from "../../db/queries";
import { EntityFilterForm, parseEntityListQuery, PersonListTable } from "../entity-ui";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "騎手一覧",
};

interface JockeysPageProps {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export default async function JockeysPage({ searchParams }: JockeysPageProps) {
  const query = parseEntityListQuery(await searchParams);
  const rows = await getPersonList("jockeys", query);

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Jockeys</p>
          <h1>騎手一覧</h1>
        </div>
        <span className="page-count">{rows.length} 件</span>
      </div>
      <EntityFilterForm action="/jockeys" query={query} searchPlaceholder="騎手名" />
      <PersonListTable basePath="/jockeys" rows={rows} />
    </section>
  );
}
