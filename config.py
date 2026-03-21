import os
from dotenv import load_dotenv

load_dotenv()

# UK Police API
API_BASE_URL = "https://data.police.uk/api"
LATITUDE = 51.5074
LONGITUDE = -0.1278
MONTHS_TO_FETCH = 3

# PostgreSQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "crime_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}
