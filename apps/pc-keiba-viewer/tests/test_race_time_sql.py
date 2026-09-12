from __future__ import annotations

import duckdb
from _race_time import encoded_race_time_tenths_sql


def test_encoded_race_time_tenths_sql_decodes_mssd_and_rejects_invalid_values() -> None:
    con = duckdb.connect(":memory:")
    expression = encoded_race_time_tenths_sql("encoded")

    rows = con.execute(
        f"""
        select encoded, {expression} as tenths
        from (values ('598'), ('1008'), ('1599'), ('2000'), ('1999'), ('0000'),
                     (''), ('12A'), (null)) values_table(encoded)
        order by encoded nulls last
        """
    ).fetchall()

    assert rows == [
        ("", None),
        ("0000", None),
        ("1008", 608),
        ("12A", None),
        ("1599", 1199),
        ("1999", None),
        ("2000", 1200),
        ("598", 598),
        (None, None),
    ]
