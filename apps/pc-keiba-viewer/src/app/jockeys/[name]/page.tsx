import type { Metadata } from "next";
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

interface JockeyDetailPageProps {
  params: Promise<{ name: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export async function generateMetadata({ params }: JockeyDetailPageProps): Promise<Metadata> {
  const { name } = await params;
  return { title: `${decodeURIComponent(name)} 騎手詳細` };
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
        <div className="entity-title-actions">
          <FavoriteButton item={{ id: decodedName, kind: "jockey", label: data.summary.name }} />
          <span className="page-count">{data.results.length} 件</span>
        </div>
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
