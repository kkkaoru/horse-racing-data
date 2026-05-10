import { getHorseList } from "../../db/queries";
import { EntityFilterForm, HorseListTable, parseEntityListQuery } from "../entity-ui";

export const dynamic = "force-dynamic";

interface HorsesPageProps {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export default async function HorsesPage({ searchParams }: HorsesPageProps) {
  const query = parseEntityListQuery(await searchParams);
  const rows = await getHorseList(query);

  return (
    <section className="page-shell">
      <div className="page-title-row">
        <div>
          <p className="eyebrow">Horses</p>
          <h1>馬一覧</h1>
        </div>
        <span className="page-count">{rows.length} 件</span>
      </div>
      <EntityFilterForm action="/horses" query={query} searchPlaceholder="馬名・血統登録番号" />
      <HorseListTable rows={rows} />
    </section>
  );
}
