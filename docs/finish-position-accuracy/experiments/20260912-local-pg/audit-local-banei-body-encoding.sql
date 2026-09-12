BEGIN READ ONLY;
SET LOCAL statement_timeout='90s';
WITH bodies AS (
 SELECT kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango,trim(bataiju) AS raw_body,
        CASE WHEN trim(bataiju) ~ '^[0-9A-Fa-f]{1,3}$'
          THEN ('x'||lpad(trim(bataiju),8,'0'))::bit(32)::int END AS hex_body
 FROM nvd_se WHERE keibajo_code IN ('81','82','83','84')
), examples AS (
 SELECT * FROM bodies WHERE raw_body ~ '^[0-9]+[Ee][0-9]+$'
 ORDER BY kaisai_nen DESC,kaisai_tsukihi DESC LIMIT 20
)
SELECT json_build_object(
 'hex_decodable_rows',(SELECT count(*) FROM bodies WHERE hex_body IS NOT NULL),
 'hex_positive_min',(SELECT min(hex_body) FROM bodies WHERE hex_body>0),
 'hex_positive_max',(SELECT max(hex_body) FROM bodies WHERE hex_body>0),
 'scientific_notation_lookalikes',(SELECT count(*) FROM bodies WHERE raw_body ~ '^[0-9]+[Ee][0-9]+$'),
 'examples',(SELECT json_agg(examples) FROM examples)
);
COMMIT;
