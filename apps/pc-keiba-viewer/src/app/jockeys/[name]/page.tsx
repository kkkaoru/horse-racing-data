import { notFound } from "next/navigation";

import { getPersonDetailData } from "../../../db/queries";
import {
  EntityDetailFilterForm,
  EntityRaceResultsTable,
  EntitySummary,
  parseEntityListQuery,
} from "../../entity-ui";

export const dynamic = "force-dynamic";

interface JockeyDetailPageProps {
  params: Promise<{ name: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export default async function JockeyDetailPage({ params, searchParams }: JockeyDetailPageProps) {
  const { name } = await params;
  const decodedName = decodeURIComponent(name);
  const query = parseEntityListQuery(await searchParams);
  const data = await getPersonDetailData("jockeys", decodedName, query);
  if (!data) {
    notFound();
  }

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Jockey Detail</p>
          <h1>{data.summary.name}</h1>
        </div>
        <span className="page-count">{data.results.length} 件</span>
      </div>
      <EntitySummary summary={data.summary} />
      <EntityDetailFilterForm
        action={`/jockeys/${encodeURIComponent(decodedName)}`}
        query={query}
        searchPlaceholder="馬名・レース名"
      />
      <EntityRaceResultsTable rows={data.results} />
    </section>
  );
}
