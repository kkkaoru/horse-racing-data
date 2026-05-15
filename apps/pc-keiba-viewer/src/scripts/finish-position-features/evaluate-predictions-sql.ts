// Run with: imported from evaluate-predictions.ts (bun runtime)

const EVALUATIONS_TABLE = "model_prediction_evaluations";
const PREDICTIONS_TABLE = "race_finish_position_model_predictions";
const SOURCE_TABLE = "race_entry_corner_features";

const PRIMARY_KEY_COLUMNS = [
  "model_version",
  "category",
  "evaluation_window_from",
  "evaluation_window_to",
];

export const buildEvaluationsDdl = (): string => `
    create table if not exists ${EVALUATIONS_TABLE} (
      model_version text not null,
      category text not null,
      evaluation_window_from text not null,
      evaluation_window_to text not null,
      race_count integer not null,
      prediction_count integer not null,
      top1_accuracy numeric,
      top3_box_accuracy numeric,
      top3_exact_accuracy numeric,
      place1_accuracy numeric,
      place2_accuracy numeric,
      place3_accuracy numeric,
      top3_winner_capture numeric,
      top5_winner_capture numeric,
      pair_score numeric,
      ndcg_at_3 numeric,
      evaluated_at timestamptz not null default now(),
      primary key (${PRIMARY_KEY_COLUMNS.join(", ")})
    )
  `;

export const buildCategorySourceFilter = (category: string): string => {
  if (category === "jra") return "rec.source = 'jra'";
  if (category === "nar") return "rec.source = 'nar' and rec.keibajo_code <> '83'";
  if (category === "ban-ei") return "rec.source = 'nar' and rec.keibajo_code = '83'";
  return "true";
};

interface BuildAggregateSqlArgs {
  modelVersion: string;
  category: string;
  fromDate: string;
  toDate: string;
}

export const buildAggregateMetricsSql = ({
  modelVersion,
  category,
  fromDate,
  toDate,
}: BuildAggregateSqlArgs): string => `
    with predictions as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             ketto_toroku_bango, predicted_rank, predicted_score
      from ${PREDICTIONS_TABLE}
      where model_version = '${modelVersion.replaceAll("'", "''")}'
    ),
    actuals as (
      select rec.source, rec.kaisai_nen, rec.kaisai_tsukihi, rec.keibajo_code, rec.race_bango,
             rec.ketto_toroku_bango, rec.finish_position, rec.shusso_tosu
      from ${SOURCE_TABLE} rec
      where rec.race_date between '${fromDate}' and '${toDate}'
        and rec.finish_position is not null
        and ${buildCategorySourceFilter(category)}
    ),
    joined as (
      select p.*, a.finish_position, a.shusso_tosu
      from predictions p
      join actuals a using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
    ),
    per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             count(*) runner_count,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) top1_hit,
             (sum(case when predicted_rank <= 3 and finish_position <= 3 then 1 else 0 end) = 3)::int top3_box_hit,
             (
               max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) = 1
               and max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) = 1
               and max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) = 1
             )::int top3_exact_hit,
             max(case when predicted_rank = 1 and finish_position = 1 then 1 else 0 end) place1_hit,
             max(case when predicted_rank = 2 and finish_position = 2 then 1 else 0 end) place2_hit,
             max(case when predicted_rank = 3 and finish_position = 3 then 1 else 0 end) place3_hit,
             max(case when predicted_rank <= 3 and finish_position = 1 then 1 else 0 end) top3_winner_capture_hit,
             max(case when predicted_rank <= 5 and finish_position = 1 then 1 else 0 end) top5_winner_capture_hit
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    ),
    pair_per_race as (
      select j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango,
             avg(
               case
                 when (j1.predicted_rank < j2.predicted_rank) = (j1.finish_position < j2.finish_position)
                 then 1.0 else 0.0
               end
             ) pair_correct
      from joined j1
      join joined j2
        on j1.source = j2.source
        and j1.kaisai_nen = j2.kaisai_nen
        and j1.kaisai_tsukihi = j2.kaisai_tsukihi
        and j1.keibajo_code = j2.keibajo_code
        and j1.race_bango = j2.race_bango
        and j1.ketto_toroku_bango < j2.ketto_toroku_bango
      group by j1.source, j1.kaisai_nen, j1.kaisai_tsukihi, j1.keibajo_code, j1.race_bango
    ),
    ndcg_per_race as (
      select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
             sum(
               case
                 when predicted_rank <= 3
                 then (greatest(0, 4 - finish_position)) / ln(2 + predicted_rank)
                 else 0
               end
             ) dcg,
             (3 / ln(2 + 1) + 2 / ln(2 + 2) + 1 / ln(2 + 3)) ideal_dcg
      from joined
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    )
    select
      (select count(*) from per_race) race_count,
      (select count(*) from joined) prediction_count,
      (select avg(top1_hit::numeric) from per_race) top1_accuracy,
      (select avg(top3_box_hit::numeric) from per_race) top3_box_accuracy,
      (select avg(top3_exact_hit::numeric) from per_race) top3_exact_accuracy,
      (select avg(place1_hit::numeric) from per_race) place1_accuracy,
      (select avg(place2_hit::numeric) from per_race) place2_accuracy,
      (select avg(place3_hit::numeric) from per_race) place3_accuracy,
      (select avg(top3_winner_capture_hit::numeric) from per_race) top3_winner_capture,
      (select avg(top5_winner_capture_hit::numeric) from per_race) top5_winner_capture,
      (select avg(pair_correct) from pair_per_race) pair_score,
      (select avg(case when ideal_dcg > 0 then dcg / ideal_dcg else null end) from ndcg_per_race) ndcg_at_3
  `;

export const buildUpsertSql = (): string => `
    insert into ${EVALUATIONS_TABLE} (
      model_version, category, evaluation_window_from, evaluation_window_to,
      race_count, prediction_count,
      top1_accuracy, top3_box_accuracy, top3_exact_accuracy,
      place1_accuracy, place2_accuracy, place3_accuracy,
      top3_winner_capture, top5_winner_capture,
      pair_score, ndcg_at_3, evaluated_at
    )
    values (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, now()
    )
    on conflict (model_version, category, evaluation_window_from, evaluation_window_to)
    do update set
      race_count = excluded.race_count,
      prediction_count = excluded.prediction_count,
      top1_accuracy = excluded.top1_accuracy,
      top3_box_accuracy = excluded.top3_box_accuracy,
      top3_exact_accuracy = excluded.top3_exact_accuracy,
      place1_accuracy = excluded.place1_accuracy,
      place2_accuracy = excluded.place2_accuracy,
      place3_accuracy = excluded.place3_accuracy,
      top3_winner_capture = excluded.top3_winner_capture,
      top5_winner_capture = excluded.top5_winner_capture,
      pair_score = excluded.pair_score,
      ndcg_at_3 = excluded.ndcg_at_3,
      evaluated_at = now()
  `;

export { EVALUATIONS_TABLE, PREDICTIONS_TABLE, PRIMARY_KEY_COLUMNS, SOURCE_TABLE };
