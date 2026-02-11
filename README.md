# Marketing Performance Dashboard

Automated ETL pipeline + Streamlit dashboard for GA4 e-commerce data.

---

## Quick Start

```bash
# 1. Install uv (package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Install dependencies
uv sync

# 3. Setup environment
cp .env.example .env
# DATABASE_URL already included in .env.example (PostgreSQL Neon)

# 4. Initialize database
uv run init-db

# 5. Run ETL
uv run etl

# 6. Open dashboard
streamlit run src/dashboard.py
```

Dashboard opens at **http://localhost:8501**

---

## Commands

| Command | Description |
|---------|-------------|
| `uv sync` | Install dependencies |
| `uv run etl` | Run ETL manually |
| `uv run scheduler` | Start daily scheduler (2 AM) |
| `uv run init-db` | Initialize/reset database |

---

## Dashboard Features

- KPI Cards (Sessions, Users, Revenue, Conversion Rate)
- Charts (Revenue & Sessions trends, Traffic sources)
- Tables (Daily metrics, Traffic sources, Product performance)
- Date filter + Run ETL directly from dashboard

## Tech Stack

- **Runtime**: Python 3.12 + uv
- **Database**: PostgreSQL Neon
- **Data Source**: Kaggle (kagglehub - no API key needed)
- **Dashboard**: Streamlit
- **Charts**: Plotly
- **Postgres Client**: `psycopg` v3