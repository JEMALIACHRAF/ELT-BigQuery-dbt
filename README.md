<div align="center">

<img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white"/>
<img src="https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white"/>
<img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white"/>
<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>

# ELT Pipeline — BigQuery · dbt · Airflow

**Production-grade e-commerce data warehouse on Google Cloud Platform**

*47/49 data quality tests passing · 7 dbt models · Daily Airflow orchestration · CI/CD on GitHub Actions*

</div>

---

## Overview

This project implements a fully tested and documented **ELT pipeline** for an e-commerce company.  
Raw transactional data is loaded into **BigQuery**, transformed through **3 dbt layers**, and orchestrated daily by **Apache Airflow**.

The goal: deliver reliable, domain-specific datasets to Finance, Marketing, and Product teams.

```
Raw Sources  →  BigQuery (raw)  →  dbt staging  →  dbt intermediate  →  dbt marts  →  BI Tools
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                │
│           Transactional DB · Web Analytics (GA4) · CRM              │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  Fivetran / batch load
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     BIGQUERY  (GCP)                                  │
│                                                                      │
│   raw_ecommerce                                                      │
│   ├── orders          ← source of truth, never modified              │
│   ├── customers                                                      │
│   ├── order_items                                                    │
│   └── products                                                       │
│                                                                      │
│   staging  (views)         ← clean · rename · cast types             │
│   ├── stg_orders                                                     │
│   ├── stg_customers                                                  │
│   ├── stg_order_items                                                │
│   └── stg_products                                                   │
│                                                                      │
│   intermediate  (ephemeral) ← joins · enrichment · business logic   │
│   ├── int_orders_enriched                                            │
│   └── int_customer_orders                                            │
│                                                                      │
│   marts  (partitioned tables) ← domain-specific, BI-ready           │
│   ├── finance/    fct_daily_revenue      (partition by day)          │
│   ├── marketing/  dim_customers          (RFM segmentation)          │
│   └── product/    fct_product_performance (partition by month)       │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
                    Looker · Metabase · Data Studio
```

**Airflow DAG** — daily at 4:00 AM UTC:

```
check_source_freshness
        │
        ▼
dbt_run_staging ──────► dbt_test_staging
        │
        ▼
dbt_run_intermediate
        │
        ▼
dbt_run_marts
        │
        ├──► dbt_test_marts
        ├──► check_revenue_not_zero        (BigQueryCheckOperator)
        └──► check_no_duplicate_customers  (BigQueryCheckOperator)
                        │
                        ▼
                dbt_docs_generate
                        │
                        ▼
                notify_success
```

---

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Cloud Warehouse | Google BigQuery | — |
| Transformation | dbt-bigquery | 1.7.0 |
| Orchestration | Apache Airflow | 2.8.0 |
| Containerization | Docker + Compose | — |
| CI/CD | GitHub Actions | — |
| SQL Linting | SQLFluff | — |
| dbt Packages | dbt-utils | 1.1.1 |

---

## Project Structure

```
elt-bigquery-dbt/
│
├── dbt_project/
│   ├── dbt_project.yml              # materialization strategy per layer
│   ├── packages.yml                 # dbt-utils dependency
│   ├── profiles.yml.example         # connection template (copy → profiles.yml)
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml          # source declarations + freshness SLAs
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_order_items.sql
│   │   │   └── stg_products.sql
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_orders_enriched.sql    # orders + items + products
│   │   │   └── int_customer_orders.sql   # customer lifetime aggregates
│   │   │
│   │   └── marts/
│   │       ├── schema.yml                # column docs + tests
│   │       ├── finance/fct_daily_revenue.sql
│   │       ├── marketing/dim_customers.sql
│   │       └── product/fct_product_performance.sql
│   │
│   ├── macros/
│   │   └── custom_tests.sql         # assert_no_future_dates · generate_schema_name
│   │
│   └── seeds/raw_ecommerce/         # sample data for local dev
│       ├── customers.csv
│       ├── orders.csv  (+orders.yml)
│       ├── order_items.csv
│       └── products.csv
│
├── airflow/dags/
│   └── elt_bigquery_dbt.py          # full DAG — retry · alerting · BQ checks
│
├── .github/workflows/
│   └── dbt_ci.yml                   # lint → compile → run → test on every PR
│
├── Dockerfile                       # Airflow 2.8 + dbt-bigquery
├── docker-compose.yaml              # full Airflow stack (webserver · scheduler · worker)
└── README.md
```

---

## Data Model

### Marts

#### `fct_daily_revenue` — Finance
Partitioned by `order_date` (day) · Clustered by `country_code`, `order_status`

| Column | Description |
|---|---|
| `order_date` | Partition key |
| `country_code` | Shipping country |
| `nb_orders` | Order count |
| `confirmed_revenue_eur` | Revenue from delivered orders only |
| `cancellation_rate` | Cancelled / total orders |
| `avg_order_value_eur` | Average basket size |

#### `dim_customers` — Marketing
RFM segmentation · Clustered by `customer_segment`, `country_code`

| Column | Description |
|---|---|
| `customer_id` | Primary key |
| `customer_segment` | `active` · `at_risk` · `churned` · `no_purchase` |
| `recency_score` | NTILE(5) — days since last order |
| `frequency_score` | NTILE(5) — total orders |
| `monetary_score` | NTILE(5) — total revenue |
| `rfm_total_score` | Sum (0–15) |
| `is_vip` | monetary = 5 AND frequency ≥ 4 |

#### `fct_product_performance` — Product
Partitioned by month · Clustered by `category`, `country_code`

| Column | Description |
|---|---|
| `product_id` | Product key |
| `units_sold` | Total quantity |
| `net_revenue_eur` | Revenue after discounts |
| `return_rate` | Cancelled + refunded / total |

---

## Data Quality — 49 Tests

```
not_null          ████████████████████  18 tests
unique            ████████              8 tests
accepted_values   ██████                6 tests
relationships     ████                  4 tests (FK integrity)
freshness         █████                 5 tests (warn 12h / error 24h)
expression_is_true██████               6 tests (revenue ≥ 0, rates 0–1)
custom macros     ██                    2 tests (assert_no_future_dates)
```

Run with failure storage — failing rows saved to BigQuery for investigation:

```bash
dbt test --profiles-dir . --store-failures
```

**Result on sample data: 47 / 49 passed ✓**

---

## Local Setup

### Prerequisites

- Python 3.11+
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- GCP project with BigQuery API enabled *(free tier: 10 GB storage + 1 TB queries/month)*

### Installation

```bash
# Clone
git clone https://github.com/JEMALIACHRAF/ELT-BigQuery-dbt.git
cd ELT-BigQuery-dbt

# Virtual environment
python -m venv dbt-env
dbt-env\Scripts\activate        # Windows
# source dbt-env/bin/activate   # Mac / Linux

# Install dbt
pip install dbt-bigquery==1.7.0

# GCP authentication
gcloud auth application-default login

# Configure your project
cp dbt_project/profiles.yml.example dbt_project/profiles.yml
# Edit profiles.yml → set project: "your-gcp-project-id"

cd dbt_project

# Install dbt packages
dbt deps

# Test connection
dbt debug --profiles-dir .
# ✓ All checks passed!
```

### Run the pipeline

```bash
# Load sample data into BigQuery
dbt seed --profiles-dir .

# Build all models (staging → intermediate → marts)
dbt run --profiles-dir .

# Run all 49 tests
dbt test --profiles-dir . --store-failures

# Open data catalog
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
# → http://localhost:8080
```

**Useful selectors:**

```bash
dbt run --select tag:staging --profiles-dir .     # staging only
dbt run --select tag:marts --profiles-dir .       # marts only
dbt run --select stg_orders --profiles-dir .      # single model
dbt source freshness --profiles-dir .             # check raw table SLAs
```

---

## Airflow Setup (Docker)

### Prerequisites

- Docker Desktop — minimum 4 GB RAM allocated

```bash
# From project root
mkdir dags logs plugins config
copy airflow\dags\elt_bigquery_dbt.py dags\   # Windows
echo AIRFLOW_UID=50000 > .env

# Build custom image (Airflow + dbt-bigquery) — ~10 min
docker-compose -f docker-compose.yaml build

# Initialize database
docker-compose -f docker-compose.yaml up airflow-init

# Start all services
docker-compose -f docker-compose.yaml up -d

# Check status
docker ps
```

Open **http://localhost:8080** — login: `airflow` / `airflow`

To trigger the DAG manually: toggle it **ON** → click **▶ Trigger DAG**

```bash
# Stop
docker-compose -f docker-compose.yaml down

# Stop + wipe database
docker-compose -f docker-compose.yaml down --volumes
```

---

## CI/CD

Every pull request to `main` runs automatically:

```
SQLFluff lint  →  dbt compile  →  dbt source freshness  →  dbt run  →  dbt test
```

Failing tests write rows to BigQuery (`--store-failures`) and upload artifacts for inspection.

---

## Key Design Decisions

**Layered architecture** separates concerns cleanly:
- Staging = 1:1 with source, zero business logic, easy to audit
- Intermediate = ephemeral (no BQ table created), reused across marts
- Marts = domain-owned, optimized for specific query patterns

**BigQuery partitioning + clustering** on all mart tables — reduces query cost by ~70% on large datasets vs unpartitioned tables.

**Exponential backoff** in Airflow — most failures are transient (GCP quota, network). 2 retries at 5min → 30min max reduces alert noise significantly.

**`--store-failures`** — dbt writes failing rows to a `dbt_test_failures` dataset instead of just logging. Makes debugging data quality issues 10x faster.

---

## Results

| Step | Output |
|---|---|
| `dbt seed` | 4 tables loaded in BigQuery |
| `dbt run` | 7 models built (4 views + 3 partitioned tables) |
| `dbt test` | 47 / 49 tests passed |
| `dbt docs` | Full data catalog with lineage graph |

BigQuery datasets created:

| Dataset | Contents |
|---|---|
| `dev_ecommerce_raw_ecommerce` | 4 source tables (seed data) |
| `dev_ecommerce_staging` | 4 staging views |
| `dev_ecommerce_marts` | 3 mart tables — finance · marketing · product |

---

<div align="center">

Made with dbt · BigQuery · Airflow · ☕

</div>