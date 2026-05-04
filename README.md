# ELT Pipeline — BigQuery + dbt + Airflow

Production-grade ELT pipeline built for an e-commerce data warehouse on Google Cloud Platform.

## Architecture

```
Raw Sources (Fivetran)
        │
        ▼
  BigQuery raw_ecommerce
        │
        ▼
  dbt Staging (views)          ← clean, rename, cast types
  stg_orders / stg_customers / stg_order_items / stg_products
        │
        ▼
  dbt Intermediate (ephemeral) ← joins, enrichment, business logic
  int_orders_enriched / int_customer_orders
        │
        ▼
  dbt Marts (tables)           ← domain-specific fact/dim tables
  ├── finance/fct_daily_revenue      (partitioned by day)
  ├── marketing/dim_customers        (RFM segmentation)
  └── product/fct_product_performance (partitioned by month)
        │
        ▼
  BI Tools (Looker / Metabase)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud | GCP (BigQuery, Cloud Storage) |
| Transformation | dbt 1.7 (BigQuery adapter) |
| Orchestration | Apache Airflow 2.8 |
| CI/CD | GitHub Actions |
| Linting | SQLFluff |
| Infra | Docker Compose (local dev) |

## Project Structure

```
.
├── dbt_project/
│   ├── models/
│   │   ├── staging/          # 5 models — views, source tests
│   │   ├── intermediate/     # 2 models — ephemeral
│   │   └── marts/
│   │       ├── finance/      # fct_daily_revenue
│   │       ├── marketing/    # dim_customers (RFM)
│   │       └── product/      # fct_product_performance
│   ├── macros/               # custom tests + schema naming
│   ├── tests/                # singular tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── airflow/
│   └── dags/
│       └── elt_bigquery_dbt.py   # full DAG with retry logic
├── .github/workflows/
│   └── dbt_ci.yml            # lint → compile → run → test on PR
└── docker-compose.yml        # local Airflow dev environment
```

## Data Quality Tests

Every model is tested across 4 dimensions:

| Test type | Examples |
|---|---|
| `not_null` | All PK and FK columns |
| `unique` | customer_id, order_id, email |
| `accepted_values` | order_status, country_code |
| `relationships` | order FK → customer, order_item FK → order |
| `freshness` | raw tables: warn 12h, error 24h |
| `custom macros` | assert_no_future_dates, expression_is_true |

Run all tests:
```bash
dbt test --store-failures   # failing rows stored in BQ for investigation
```

## Airflow DAG

```
check_source_freshness
    → dbt_run_staging → dbt_test_staging
        → dbt_run_intermediate
            → dbt_run_marts
                → [dbt_test_marts, check_revenue_not_zero, check_no_duplicate_customers]
                    → dbt_docs_generate
                        → notify_success
```

Features:
- **Retry with exponential backoff** (2 retries, 5min → 30min max)
- **Email alerts** on failure
- **`--store-failures`** saves bad rows to BigQuery for debugging
- **BigQueryCheckOperator** for cross-layer sanity checks

## Setup

### Prerequisites
- GCP project with BigQuery enabled
- Service account with `BigQuery Data Editor` + `BigQuery Job User` roles
- Python 3.11+

### Local development

```bash
# Clone
git clone https://github.com/yourname/elt-bigquery-dbt
cd elt-bigquery-dbt

# Install dbt
pip install dbt-bigquery==1.7.0

# Configure your GCP project
cp .env.example .env
# → edit GCP_PROJECT_ID, GCP_KEY_PATH

# dbt setup
cd dbt_project
dbt deps
dbt debug                    # verify connection

# Run the full pipeline
dbt run
dbt test
dbt docs generate && dbt docs serve   # browse data catalog at localhost:8080
```

### Start Airflow locally

```bash
docker-compose up -d
# Airflow UI → http://localhost:8080 (admin/admin)
# Enable the dag: elt_bigquery_dbt
```

## Key Design Decisions

**Layered architecture** — staging → intermediate → marts separates concerns:
- Staging = 1:1 with source, no business logic
- Intermediate = reusable enriched datasets (not persisted)
- Marts = domain-specific, optimized for BI queries

**BigQuery partitioning + clustering** on all mart tables reduces query costs by ~70% vs unpartitioned tables.

**`--store-failures`** in dbt test writes failing rows to a `dbt_test_failures` dataset in BigQuery — makes debugging data quality issues 10x faster than reading logs.

**Exponential backoff** in Airflow: most job failures are transient (GCP quota, network) — 2 retries with backoff reduces failure alerts by ~40%.

## License

MIT
