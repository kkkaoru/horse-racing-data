BEGIN READ ONLY;
SET LOCAL statement_timeout='90s';
COPY (
 SELECT concat_ws('-','nar',kaisai_nen||kaisai_tsukihi,keibajo_code,race_bango) AS race_id,
        ketto_toroku_bango AS horse_id,bataiju AS raw_body
 FROM nvd_se
 WHERE keibajo_code IN ('81','82','83','84')
   AND kaisai_nen||kaisai_tsukihi <= '20260912'
 ORDER BY kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango
) TO STDOUT WITH CSV HEADER;
COMMIT;
