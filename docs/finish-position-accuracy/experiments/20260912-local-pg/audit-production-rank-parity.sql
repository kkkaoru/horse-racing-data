-- Production inference agreement ONLY; no observed outcomes or accuracy claims.
CREATE TEMP VIEW document AS
SELECT json_extract_string(json,'$.data.content[0].text')::JSON AS payload
FROM read_json_objects('docs/finish-position-accuracy/experiments/20260912-local-pg/cloudflare-banei-rank-parity.response.json',format='unstructured')
WHERE json_extract_string(json,'$.ok')='true';
CREATE TEMP VIEW production_races AS
SELECT payload,r.value AS race
FROM document,json_each(payload,'$.values') r;
CREATE TEMP VIEW production_values AS
SELECT concat_ws('-','nar',json_extract_string(payload,'$.date'),json_extract_string(payload,'$.venue'),json_extract_string(race,'$.race')) AS race_id,
       json_extract_string(h.value,'$.horseNumber')::INTEGER AS horse_number,
       json_extract(h.value,'$.norm')::DOUBLE AS norm
FROM production_races,json_each(race,'$.rows') h;
CREATE TEMP VIEW production AS
SELECT *,row_number() OVER(PARTITION BY race_id ORDER BY norm,horse_number) AS production_rank FROM production_values;
CREATE TEMP VIEW local AS
SELECT race_id,horse_number::INTEGER AS horse_number,predicted_rank AS local_rank
FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/local-upcoming-frozen-v1/83/predictions.parquet');
CREATE TEMP VIEW compared AS
SELECT *,local_rank IS NOT NULL AND production_rank IS NOT NULL AS matched,
       local_rank=production_rank AS same_rank
FROM local FULL OUTER JOIN production USING(race_id,horse_number);
SELECT json_object(
 'purpose','inference agreement, never finish accuracy',
 'production_races',(SELECT count(*) FROM production_races),
 'expected_model_races',(SELECT count(*) FROM production_races WHERE json_array_length(race,'$.models')=1 AND json_extract_string(race,'$.models[0]')='banei-cb-v9-sim-2011'),
 'production_rows',(SELECT count(*) FROM production),
 'local_rows',(SELECT count(*) FROM local),
 'invalid_norms',(SELECT count(*) FROM production WHERE norm IS NULL OR NOT isfinite(norm) OR norm<0 OR norm>1),
 'duplicate_production_keys',(SELECT count(*) FROM (SELECT race_id,horse_number FROM production GROUP BY ALL HAVING count(*)>1)),
 'duplicate_norms',(SELECT count(*) FROM (SELECT race_id,norm FROM production GROUP BY ALL HAVING count(*)>1)),
 'unmatched_rows',(SELECT count(*) FROM compared WHERE NOT matched),
 'same_rank_rows',(SELECT count(*) FROM compared WHERE same_rank),
 'same_top1_races',(SELECT count(*) FROM compared WHERE local_rank=1 AND production_rank=1),
 'race_agreement',(SELECT to_json(list(t ORDER BY race_id)) FROM (
    SELECT race_id,count(*) AS rows,count(*) FILTER(WHERE same_rank) AS same_rank_rows,
           list(horse_number ORDER BY local_rank) AS local_order,
           list(horse_number ORDER BY production_rank) AS production_order
    FROM compared GROUP BY race_id
 ) t)
);
