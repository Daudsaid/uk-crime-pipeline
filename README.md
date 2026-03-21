# 🔍 UK Crime Pipeline

A end-to-end **Data Engineering pipeline** that ingests real crime data from the UK Police API, stores it in PostgreSQL, and transforms it into analytical models using dbt.

---

## 🏗️ Architecture

```
UK Police API
      │
      ▼
 extract.py          ← Fetches raw crime data via REST API
      │
      ▼
transform.py         ← Cleans & structures data with pandas
      │
      ▼
  load.py            ← Loads into PostgreSQL (crime_db)
      │
      ▼
 PostgreSQL
      │
      ▼
  dbt models         ← Transforms raw data into analytics-ready marts
      │
      ├── stg_crimes             (staging)
      ├── mart_crimes_by_category
      ├── mart_crimes_by_street
      └── mart_monthly_trends
```

---

## 🛠️ Tech Stack

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.11-orange?logo=dbt)
![pandas](https://img.shields.io/badge/pandas-2.x-150458?logo=pandas)

| Layer | Tool |
|---|---|
| Extract | Python `requests` |
| Transform | `pandas` |
| Load | `psycopg2` → PostgreSQL |
| Modelling | dbt (staging + marts) |
| Source | [UK Police API](https://data.police.uk/docs/) |

---

## 📁 Project Structure

```
uk-crime-pipeline/
├── extract.py          # API ingestion
├── transform.py        # Data cleaning & structuring
├── load.py             # PostgreSQL loader
├── config.py           # DB connection config
├── main.py             # Pipeline orchestrator
├── requirements.txt
├── .env.example        # Environment variable template
└── uk_crime_dbt/
    ├── dbt_project.yml
    ├── models/
    │   ├── staging/
    │   │   ├── stg_crimes.sql
    │   │   └── sources.yml
    │   └── marts/
    │       ├── mart_crimes_by_category.sql
    │       ├── mart_crimes_by_street.sql
    │       ├── mart_monthly_trends.sql
    │       └── schema.yml
    └── README.md
```

---

## ⚙️ Setup & Run

### Prerequisites

- Python 3.10+
- PostgreSQL running locally
- dbt-postgres installed

### 1. Clone the repo

```bash
git clone https://github.com/Daudsaid/uk-crime-pipeline.git
cd uk-crime-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 4. Run the ETL pipeline

```bash
python main.py
```

### 5. Run dbt transformations

```bash
cd uk_crime_dbt
dbt run
dbt test
```

---

## 📊 dbt Models

| Model | Layer | Description |
|---|---|---|
| `stg_crimes` | Staging | Cleaned source data from `crime_db.crimes` |
| `mart_crimes_by_category` | Mart | Total crimes and outcome rates per category |
| `mart_crimes_by_street` | Mart | Crime counts aggregated by street |
| `mart_monthly_trends` | Mart | Month-over-month crime volume trends |

All models include **not-null** and **unique** tests — fully passing ✅

---

## 🌐 Data Source

Data is sourced from the [UK Police API](https://data.police.uk/docs/) — a free, open API providing street-level crime data across England, Wales, and Northern Ireland.

---

## 👤 Author

**Daud Abdi**
- GitHub: [github.com/Daudsaid](https://github.com/Daudsaid)
- LinkedIn: [linkedin.com/in/daudabdi0506](https://linkedin.com/in/daudabdi0506)
- Portfolio: [daud-abdi-portfolio-site.vercel.app](https://daud-abdi-portfolio-site.vercel.app)
