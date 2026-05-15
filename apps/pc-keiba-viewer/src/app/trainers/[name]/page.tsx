import { notFound } from "next/navigation";

import { getPersonDetailData } from "../../../db/queries";
import {
  EntityDetailFilterForm,
  EntityRaceResultsTable,
  EntitySummary,
  parseEntityListQuery,
} from "../../entity-ui";
import { FavoriteButton } from "../../favorite-button";

export const dynamic = "force-dynamic";

interface TrainerDetailPageProps {
  params: Promise<{ name: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export default async function TrainerDetailPage({ params, searchParams }: TrainerDetailPageProps) {
  const { name } = await params;
  const decodedName = decodeURIComponent(name);
  const query = parseEntityListQuery(await searchParams);
  const data = await getPersonDetailData("trainers", decodedName, query);
  if (!data) {
    notFound();
  }

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Trainer Detail</p>
          <h1>{data.summary.name}</h1>
        </div>
        <div className="entity-title-actions">
          <FavoriteButton item={{ id: decodedName, kind: "trainer", label: data.summary.name }} />
          <span className="page-count">{data.results.length} 件</span>
        </div>
      </div>
      <EntitySummary summary={data.summary} />
      <EntityDetailFilterForm
        action={`/trainers/${encodeURIComponent(decodedName)}`}
        query={query}
        searchPlaceholder="馬名・レース名"
      />
      <EntityRaceResultsTable rows={data.results} />
    </section>
  );
}
