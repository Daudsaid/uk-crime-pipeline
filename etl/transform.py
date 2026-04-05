import pandas as pd


# Columns where empty string should become None
_NULLABLE_STR_COLS = ["persistent_id", "context", "location_subtype"]


def _flatten_record(record: dict) -> dict:
    loc = record.get("location", {})
    street = loc.get("street", {})
    outcome = record.get("outcome_status")

    return {
        "crime_id":        record["id"],
        "persistent_id":   record.get("persistent_id", ""),
        "category":        record.get("category"),
        "month":           record.get("month"),
        "location_type":   record.get("location_type"),
        "location_subtype": record.get("location_subtype", ""),
        "context":         record.get("context", ""),
        "latitude":        loc.get("latitude"),
        "longitude":       loc.get("longitude"),
        "street_id":       street.get("id"),
        "street_name":     street.get("name"),
        "outcome_status":  outcome["category"] if outcome else None,
    }


def transform(raw_records: list[dict]) -> pd.DataFrame:
    """
    Flatten and clean raw API records into a typed DataFrame.

    Transformations applied:
    - Nested location fields flattened to latitude, longitude, street_id, street_name
    - outcome_status dict collapsed to its category string (None if absent)
    - latitude / longitude cast from str to float
    - Empty strings in persistent_id, context, location_subtype replaced with None
    - month string converted to date (first of that month)
    """
    if not raw_records:
        return pd.DataFrame()

    df = pd.DataFrame([_flatten_record(r) for r in raw_records])

    # Cast coordinates
    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)

    # Empty string → None for nullable text columns
    for col in _NULLABLE_STR_COLS:
        df[col] = df[col].replace("", None)

    # month string → first day of that month
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m")

    return df


if __name__ == "__main__":
    from extract import extract

    raw = extract()
    df = transform(raw)

    print(df.dtypes)
    print(f"\nShape: {df.shape}")
    print(f"\nNull counts:\n{df.isnull().sum()}")
    print(f"\nSample:\n{df.head(3).to_string()}")
