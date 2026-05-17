import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { getHorseDetailData } from "../../../db/queries";
import {
  EntityDetailFilterForm,
  EntityRaceResultsTable,
  EntitySummary,
  parseEntityListQuery,
} from "../../entity-ui";
import { FavoriteButton } from "../../favorite-button";

export const dynamic = "force-dynamic";

interface HorseDetailPageProps {
  params: Promise<{ kettoTorokuBango: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export async function generateMetadata({ params }: HorseDetailPageProps): Promise<Metadata> {
  const { kettoTorokuBango } = await params;
  return { title: `${decodeURIComponent(kettoTorokuBango)} 馬詳細` };
}

export default async function HorseDetailPage({ params, searchParams }: HorseDetailPageProps) {
  const { kettoTorokuBango } = await params;
  const query = parseEntityListQuery(await searchParams);
  const data = await getHorseDetailData(decodeURIComponent(kettoTorokuBango), query);
  if (!data) {
    notFound();
  }

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Horse Detail</p>
          <h1>{data.summary.name}</h1>
        </div>
        <div className="entity-title-actions">
          <FavoriteButton
            item={{
              id: decodeURIComponent(kettoTorokuBango),
              kind: "horse",
              label: data.summary.name,
            }}
          />
          <span className="page-count">{data.results.length} 件</span>
        </div>
      </div>
      <EntitySummary summary={data.summary} />
      <EntityDetailFilterForm
        action={`/horses/${encodeURIComponent(kettoTorokuBango)}`}
        query={query}
        searchPlaceholder="詳細内検索"
      />
      <EntityRaceResultsTable rows={data.results} showRaceTimeColumns />
    </section>
  );
}
