# Marketing Performance Dashboard

ETL pipeline otomatis + Dashboard Streamlit untuk data e-commerce GA4.

---

## Quick Start

### 1. Install uv
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Install Dependencies
```bash
uv sync
```

### 3. Setup Environment
```bash
cp .env.example .env
# Edit .env dan tambahkan DATABASE_URL
```

### 4. Initialize Database
```bash
uv run init-db
```

### 5. Run ETL
```bash
uv run etl
```

### 6. Open Dashboard
```bash
streamlit run src/dashboard.py
```

Dashboard buka di: **http://localhost:8501**

---

## Commands

| Command | Description |
|---------|-------------|
| `uv sync` | Install dependencies |
| `uv run etl` | Run ETL manual |
| `uv run scheduler` | Start daily scheduler (2 AM) |
| `uv run init-db` | Initialize/reset database |
| `streamlit run src/dashboard.py` | Open dashboard |

---

## Dashboard Features

- **KPI Cards**: Sessions, Users, Page Views, Revenue, Transactions, Conversion Rate
- **Charts**: Trend revenue harian & sessions, Pie chart traffic sources
- **Tables**: Daily metrics, Traffic sources, Product performance
- **Filters**: Date range slider
- **Actions**: Run ETL langsung dari dashboard

---

## Project Structure

```
submission/
├── pyproject.toml       # Dependencies & scripts
├── README.md
├── .env.example
└── src/
    ├── etl.py          # ETL pipeline (kagglehub → PostgreSQL)
    ├── scheduler.py    # Cron scheduler
    ├── dashboard.py    # Streamlit dashboard
    └── agent.py        # (optional) AI agent
```

---

## Database Schema

| Table | Description |
|-------|-------------|
| `raw_events` | Raw GA4 event data |
| `daily_metrics` | Aggregated daily KPIs |
| `traffic_source_metrics` | Traffic source breakdown |
| `product_metrics` | Product performance |
| `etl_runs` | ETL execution history |
| `dashboard_summary` | Materialized view untuk quick queries |

---

## Sample Queries

```sql
-- Daily summary
SELECT * FROM dashboard_summary
ORDER BY date DESC LIMIT 30;

-- Top traffic sources
SELECT source, medium, SUM(revenue) as total_revenue
FROM traffic_source_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY source, medium
ORDER BY total_revenue DESC;

-- Top products
SELECT item_name, SUM(purchases) as total_purchases, SUM(revenue) as total_revenue
FROM product_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY item_name
ORDER BY total_revenue DESC LIMIT 10;
```

---

## Tech Stack

- **Runtime**: Python 3.12 + uv
- **Database**: PostgreSQL Neon
- **Data Source**: Kaggle (kagglehub - no API key needed)
- **Dashboard**: Streamlit
- **Charts**: Plotly
- **Postgres Client**: `psycopg` v3
