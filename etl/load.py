import psycopg2
import psycopg2.extras
import pandas as pd
from config import DB_CONFIG


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS crimes (
    crime_id         BIGINT         PRIMARY KEY,
    persistent_id    TEXT           NULL,
    category         TEXT           NOT NULL,
    month            DATE           NOT NULL,
    location_type    TEXT           NOT NULL,
    location_subtype TEXT           NULL,
    context          TEXT           NULL,
    latitude         NUMERIC(10,6)  NOT NULL,
    longitude        NUMERIC(10,6)  NOT NULL,
    street_id        BIGINT         NOT NULL,
    street_name      TEXT           NOT NULL,
    outcome_status   TEXT           NULL
);
"""

UPSERT_SQL = """
INSERT INTO crimes (
    crime_id, persistent_id, category, month,
    location_type, location_subtype, context,
    latitude, longitude, street_id, street_name, outcome_status
) VALUES %s
ON CONFLICT (crime_id) DO UPDATE SET
    persistent_id    = EXCLUDED.persistent_id,
    category         = EXCLUDED.category,
    month            = EXCLUDED.month,
    location_type    = EXCLUDED.location_type,
    location_subtype = EXCLUDED.location_subtype,
    context          = EXCLUDED.context,
    latitude         = EXCLUDED.latitude,
    longitude        = EXCLUDED.longitude,
    street_id        = EXCLUDED.street_id,
    street_name      = EXCLUDED.street_name,
    outcome_status   = EXCLUDED.outcome_status;
"""

# Ordered to match the VALUES %s tuple below
_COLUMNS = [
    "crime_id", "persistent_id", "category", "month",
    "location_type", "location_subtype", "context",
    "latitude", "longitude", "street_id", "street_name", "outcome_status",
]


def _df_to_tuples(df: pd.DataFrame) -> list[tuple]:
    """Convert DataFrame rows to tuples, replacing pandas NA with None."""
    subset = df[_COLUMNS].where(df[_COLUMNS].notna(), other=None)
    return [tuple(row) for row in subset.itertuples(index=False, name=None)]


def load(df: pd.DataFrame) -> None:
    """
    Create the crimes table if absent, then upsert all rows from df.

    Args:
        df: Clean DataFrame produced by transform.transform()
    """
    if df.empty:
        print("No records to load.")
        return

    records = _df_to_tuples(df)

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            psycopg2.extras.execute_values(cur, UPSERT_SQL, records, page_size=500)
            print(f"Upserted {cur.rowcount} rows into crimes table.")


if __name__ == "__main__":
    from extract import extract
    from transform import transform

    df = transform(extract())
    load(df)
