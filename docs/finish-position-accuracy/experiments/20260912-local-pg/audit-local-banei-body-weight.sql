BEGIN READ ONLY;
SET LOCAL statement_timeout='90s';
WITH observed AS (
 SELECT kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango,bataiju,
        CASE WHEN trim(bataiju) ~ '^[0-9]+$' THEN trim(bataiju)::numeric END AS numeric_weight
 FROM nvd_se WHERE keibajo_code IN ('81','82','83','84')
), samples AS (
 SELECT * FROM observed WHERE numeric_weight>2000 OR numeric_weight BETWEEN 1 AND 100
 ORDER BY numeric_weight DESC LIMIT 20
)
SELECT json_build_object(
 'source','local-pg nvd_se historical Ban-ei venues',
 'rows',(SELECT count(*) FROM observed),
 'numeric_over_2000',(SELECT count(*) FROM observed WHERE numeric_weight>2000),
 'numeric_positive_below_100',(SELECT count(*) FROM observed WHERE numeric_weight BETWEEN 1 AND 100),
 'nonnumeric_nonblank',(SELECT count(*) FROM observed WHERE trim(bataiju)<>'' AND numeric_weight IS NULL),
 'extreme_examples',(SELECT json_agg(samples) FROM samples)
);
COMMIT;
