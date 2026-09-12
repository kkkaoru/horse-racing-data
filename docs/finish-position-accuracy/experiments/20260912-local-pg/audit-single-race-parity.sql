-- Focused inference parity, never finish-position accuracy.
CREATE TEMP VIEW document AS
SELECT json_extract_string(json,'$.data.content[0].text')::JSON AS payload
FROM read_json_objects('docs/finish-position-accuracy/experiments/20260912-local-pg/cloudflare-banei-rank-parity.response.json',format='unstructured')
WHERE json_extract_string(json,'$.ok')='true';
CREATE TEMP VIEW production_values AS
SELECT concat_ws('-','nar',json_extract_string(payload,'$.date'),json_extract_string(payload,'$.venue'),json_extract_string(r.value,'$.race')) AS race_id,
 json_extract_string(h.value,'$.horseNumber')::INTEGER AS horse_number,
 json_extract(h.value,'$.norm')::DOUBLE AS norm
FROM document,json_each(payload,'$.values') r,json_each(r.value,'$.rows') h
WHERE json_extract_string(r.value,'$.race')='01';
CREATE TEMP VIEW production AS
SELECT *,row_number() OVER(PARTITION BY race_id ORDER BY norm,horse_number) AS production_rank FROM production_values;
CREATE TEMP VIEW focused AS
SELECT race_id,horse_number::INTEGER AS horse_number,predicted_rank AS focused_rank
FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/local-upcoming-single-frozen-v1/83/predictions.parquet');
CREATE TEMP VIEW whole_card AS
SELECT race_id,horse_number::INTEGER AS horse_number,predicted_rank AS card_rank
FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/local-upcoming-frozen-v1/83/predictions.parquet')
WHERE race_id='nar-20260912-83-01';
CREATE TEMP VIEW comparison AS
SELECT * FROM focused FULL JOIN production USING(race_id,horse_number) FULL JOIN whole_card USING(race_id,horse_number);
SELECT json_object(
 'purpose','Focused local versus deployed inference, not accuracy',
 'rows',(SELECT count(*) FROM comparison),
 'unmatched',(SELECT count(*) FROM comparison WHERE focused_rank IS NULL OR production_rank IS NULL OR card_rank IS NULL),
 'focused_production_rank_matches',(SELECT count(*) FROM comparison WHERE focused_rank=production_rank),
 'card_production_rank_matches',(SELECT count(*) FROM comparison WHERE card_rank=production_rank),
 'focused_card_rank_matches',(SELECT count(*) FROM comparison WHERE focused_rank=card_rank),
 'orders',(SELECT to_json(list(t)) FROM (SELECT list(horse_number ORDER BY focused_rank) AS focused,list(horse_number ORDER BY card_rank) AS whole_card,list(horse_number ORDER BY production_rank) AS production FROM comparison) t)
);
