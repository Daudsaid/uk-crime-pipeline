import requests
from config import API_BASE_URL, LATITUDE, LONGITUDE, MONTHS_TO_FETCH


def get_available_months() -> list[str]:
    """Fetch available date months from the UK Police API availability endpoint."""
    url = f"{API_BASE_URL}/crimes-street-dates"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    # Each entry looks like {"date": "2024-09", "stop-and-search": [...]}
    months = [entry["date"] for entry in response.json()]

    # API returns newest last; sort descending and take the most recent N
    months.sort(reverse=True)
    return months[:MONTHS_TO_FETCH]


def fetch_crimes_for_month(month: str) -> list[dict]:
    """Fetch street-level crimes for a given month at the configured location."""
    url = f"{API_BASE_URL}/crimes-street/all-crime"
    params = {
        "lat": LATITUDE,
        "lng": LONGITUDE,
        "date": month,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def extract() -> list[dict]:
    """
    Main extract function.
    Returns a flat list of raw crime records across the last N available months.
    """
    months = get_available_months()
    print(f"Fetching data for months: {months}")

    all_records = []
    for month in months:
        records = fetch_crimes_for_month(month)
        print(f"  {month}: {len(records)} crimes fetched")
        all_records.extend(records)

    print(f"Total records extracted: {len(all_records)}")
    return all_records


if __name__ == "__main__":
    data = extract()
    # Quick sanity check — print the first record
    if data:
        import json
        print("\nSample record:")
        print(json.dumps(data[0], indent=2))
