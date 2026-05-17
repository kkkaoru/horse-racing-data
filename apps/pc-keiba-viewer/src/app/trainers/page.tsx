import type { Metadata } from "next";

import { getPersonList } from "../../db/queries";
import { EntityFilterForm, parseEntityListQuery, PersonListTable } from "../entity-ui";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "調教師一覧",
};

interface TrainersPageProps {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export default async function TrainersPage({ searchParams }: TrainersPageProps) {
  const query = parseEntityListQuery(await searchParams);
  const rows = await getPersonList("trainers", query);

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Trainers</p>
          <h1>調教師一覧</h1>
        </div>
        <span className="page-count">{rows.length} 件</span>
      </div>
      <EntityFilterForm action="/trainers" query={query} searchPlaceholder="調教師名" />
      <PersonListTable basePath="/trainers" rows={rows} />
    </section>
  );
}
