"""Complete NAR starter histories, retaining unclassified actual starters."""

from pathlib import Path

import duckdb

STARTER_SQL: str = """
SELECT concat('nar:',s.kaisai_nen,':',s.kaisai_tsukihi,':',
              s.keibajo_code,':',s.race_bango) AS race_id,
       s.kaisai_nen||s.kaisai_tsukihi AS race_date,
       s.ketto_toroku_bango AS horse_id, s.keibajo_code AS venue_code,
       try_cast(s.umaban AS INTEGER) AS horse_number,
       trim(s.ijo_kubun_code) AS abnormality_code,
       coalesce(try_cast(s.kakutei_chakujun AS INTEGER),0) AS finish_position,
       try_cast(s.tansho_odds AS DOUBLE)/10.0 AS decimal_odds,
       try_cast(r.shusso_tosu AS BIGINT) AS field_size
FROM se s JOIN ra r USING(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)
WHERE r.data_kubun<>'9' AND trim(s.ijo_kubun_code) NOT IN ('1','2','3')
"""
KEYS: str = "kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango"


def export_starter_history(*, runners: Path, races: Path, output: Path) -> dict[str, int]:
    """Codes 1-3 never started; 4-5 started but have no classified finish.

    Undefined performance stays NULL; it is not a fabricated last-place label.
    All race denominators are checked before any cohort-horse filtering.
    """
    if output.exists():
        raise FileExistsError(output)
    with duckdb.connect() as db:
        db.execute("SET threads=4")
        db.from_parquet(str(runners)).create_view("se")
        db.from_parquet(str(races)).create_view("ra")
        duplicate_se = db.execute(
            f"SELECT {KEYS},umaban FROM se GROUP BY ALL HAVING count(*)>1"
        ).fetchall()
        duplicate_ra = db.execute(
            f"SELECT {KEYS} FROM ra GROUP BY ALL HAVING count(*)>1"
        ).fetchall()
        if duplicate_se or duplicate_ra:
            raise ValueError("Duplicate raw race or runner identity")
        unknown = db.execute(
            "SELECT 1 FROM se WHERE ijo_kubun_code IS NULL "
            "OR trim(ijo_kubun_code) NOT IN ('0','1','2','3','4','5','6','7') LIMIT 1"
        ).fetchall()
        if unknown:
            raise ValueError("Unknown abnormality code")
        missing = db.execute(f"SELECT 1 FROM se s ANTI JOIN ra r USING({KEYS}) LIMIT 1").fetchall()
        if missing:
            raise ValueError("Missing official race metadata")
        db.execute(f"CREATE TABLE starters AS {STARTER_SQL}")
        inconsistent = db.execute(
            "SELECT race_id FROM starters GROUP BY race_id "
            "HAVING count(*)<>min(field_size) OR min(field_size) IS NULL "
            "OR min(field_size)<1 OR min(field_size)<>max(field_size)"
        ).fetchall()
        if inconsistent:
            raise ValueError("Official starter count does not match complete source roster")
        invalid = db.execute(
            "SELECT 1 FROM starters WHERE horse_id IS NULL OR horse_id IN ('','0000000000') "
            "OR horse_number IS NULL OR horse_number<1 OR finish_position<0 "
            "OR finish_position>field_size LIMIT 1"
        ).fetchall()
        if invalid:
            raise ValueError("Invalid starter identity or finishing position")
        ambiguous = db.execute(
            "SELECT race_id,horse_id FROM starters GROUP BY ALL HAVING count(*)>1"
        ).fetchall()
        if ambiguous:
            raise ValueError("Ambiguous horse registration within race")
        unknown_recent = db.execute(
            "SELECT 1 FROM starters WHERE race_date>='20200101' "
            "AND abnormality_code IN ('0','6','7') AND finish_position=0 LIMIT 1"
        ).fetchall()
        if unknown_recent:
            raise ValueError("Missing classified result in training/development period")
        db.execute(
            "CREATE TABLE rated AS SELECT *,CASE WHEN finish_position>0 "
            "THEN 1.0-(finish_position-1.0)/greatest(field_size-1.0,1.0) "
            "ELSE NULL END AS performance_rating FROM starters"
        )
        counts = db.execute(
            "SELECT count(*),count(DISTINCT race_id),"
            "count(*) FILTER(WHERE performance_rating IS NULL),"
            "count(*) FILTER(WHERE performance_rating IS NULL "
            "AND abnormality_code IN ('0','6','7')) FROM rated"
        ).fetchall()[0]
        db.execute(
            "COPY (SELECT * FROM rated ORDER BY race_date,race_id,horse_number) "
            "TO ? (FORMAT PARQUET,COMPRESSION ZSTD)",
            [str(output)],
        )
    return {
        "starters": int(counts[0]),
        "races": int(counts[1]),
        "unclassified_starters": int(counts[2]),
        "missing_historical_labels": int(counts[3]),
    }
