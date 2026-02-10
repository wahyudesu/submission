# Marketing Performance Dashboard - Approach

## Overview

Automated ETL pipeline for GA4 e-commerce data using Python. Downloads data from Kaggle, transforms it into metrics, and loads into PostgreSQL Neon for dashboard queries.

## Architecture

**Single-File Design**
The ETL pipeline in `src/etl.py` has three phases:

1. **Extract**: Downloads GA4 dataset using Kaggle CLI
2. **Transform**: Parses CSV, cleans NULL values, aggregates metrics
3. **Load**: Batch inserts to PostgreSQL with upsert logic

**Scheduler**
Simple `schedule` library runs daily at 2 AM (configurable).

## Tools

- **Runtime**: Python 3.10+
- **Database**: PostgreSQL Neon (serverless Postgres)
- **Scheduler**: `schedule` library
- **Data Processing**: `pandas` for data handling
- **API**: Kaggle CLI for downloads

## Data Flow

```
Kaggle API → CSV Files → Transform → Aggregate → PostgreSQL
                                            ↓
                                    Materialized View
                                      (dashboard_summary)
```

## Key Metrics

- **Daily**: sessions, users, page views, revenue, AOV, conversion rate
- **Traffic Sources**: breakdown by source/medium
- **Products**: views, add-to-carts, purchases, revenue

## Dashboard Access

Data accessible via SQL queries to `dashboard_summary` materialized view or individual metrics tables. Can connect to BI tools like Metabase, Grafana, or any PostgreSQL client.

## Automation

Scheduler runs automatically. Manual execution via `python src/etl.py`. Failed runs logged to `etl_runs` table.
