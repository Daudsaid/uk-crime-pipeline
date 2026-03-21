# uk_crime_dbt

dbt project layered on top of [uk-crime-pipeline](../uk-crime-pipeline), transforming raw UK Police API data into analytical models.

## Project Structure

```
uk_crime_dbt/
├── models/
│   ├── staging/
│   │   ├── stg_crimes.sql       # Cleans raw crimes table
│   │   └── sources.yml          # Source + staging model docs/tests
│   └── marts/
│       ├── mart_crimes_by_category.sql  # Crime count + outcome rate per category
│       ├── mart_monthly_trends.sql      # Month-over-month volume trends
│       ├── mart_crimes_by_street.sql    # Hotspot analysis by street
│       └── schema.yml                   # Mart model docs/tests
├── dbt_project.yml
├── profiles.yml
└── README.md
```

## Lineage

```
public.crimes (raw)
      ↓
stg_crimes (view) — cleans NaNs, extracts date parts, strips street prefixes
      ↓
mart_crimes_by_category  (table)
mart_monthly_trends      (table)
mart_crimes_by_street    (table)
```

## Setup

1. Make sure PostgreSQL is running and `crime_db` is loaded
2. Copy `profiles.yml` to `~/.dbt/profiles.yml`
3. Install dbt: `pip3 install dbt-postgres`

## Commands

```bash
# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Generate + serve docs (shows lineage DAG)
dbt docs generate
dbt docs serve
```

## Stack
- Python 3 · PostgreSQL · dbt-postgres
- Source data: [data.police.uk](https://data.police.uk)
