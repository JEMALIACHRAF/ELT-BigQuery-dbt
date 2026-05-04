"""
ELT Pipeline DAG — BigQuery + dbt
Orchestrates: source freshness check → dbt run → dbt test → doc generation
Schedule: daily at 5am CET
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import logging

# ─── Config ────────────────────────────────────────────────────────────────────

DBT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
DBT_TARGET = "prod"
ALERT_EMAIL = Variable.get("ALERT_EMAIL", default_var="data-team@company.com")

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# ─── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="elt_bigquery_dbt",
    default_args=default_args,
    description="Daily ELT: raw BigQuery → dbt staging → marts",
    schedule_interval="0 4 * * *",  # 4am UTC = 5am CET
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["elt", "dbt", "bigquery", "production"],
    doc_md="""
    ## ELT Pipeline

    Full daily refresh of the data warehouse:

    1. **Source freshness check** — validates raw tables were updated
    2. **dbt run staging** — clean + rename raw data
    3. **dbt run intermediate** — join and enrich
    4. **dbt run marts** — finance, marketing, product fact/dim tables
    5. **dbt test** — data quality checks (not_null, unique, freshness, custom)
    6. **dbt docs generate** — update data catalog

    **SLA**: complete by 7am CET  
    **Owner**: data-team@company.com
    """,
) as dag:

    # ── 1. Source freshness ────────────────────────────────────────────────────

    check_source_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt source freshness "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            "--select source:raw_ecommerce"
        ),
        doc="Verify raw BigQuery tables were synced within SLA (warn: 12h, error: 24h)",
    )

    # ── 2. dbt run by layer (staged) ──────────────────────────────────────────

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            "--select tag:staging "
            "--threads 4"
        ),
        doc="Materialize all staging views",
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            "--select tag:intermediate "
            "--threads 4"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            "--select tag:marts "
            "--threads 8"
        ),
        doc="Build finance, marketing, product mart tables",
    )

    # ── 3. dbt tests by layer ─────────────────────────────────────────────────

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            "--select tag:staging "
            "--store-failures"  # store failing rows in BQ for investigation
        ),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            "--select tag:marts "
            "--store-failures"
        ),
    )

    # ── 4. Data quality sanity checks (BigQuery SQL) ──────────────────────────

    check_revenue_not_zero = BigQueryCheckOperator(
        task_id="check_revenue_not_zero",
        sql="""
            SELECT COUNT(*) > 0
            FROM `{{ var.value.GCP_PROJECT_ID }}.prod_ecommerce.marts.fct_daily_revenue`
            WHERE order_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """,
        use_legacy_sql=False,
        doc="Verify yesterday's revenue rows were written",
    )

    check_no_duplicate_customers = BigQueryCheckOperator(
        task_id="check_no_duplicate_customers",
        sql="""
            SELECT COUNT(*) = 0 FROM (
                SELECT customer_id, COUNT(*) as cnt
                FROM `{{ var.value.GCP_PROJECT_ID }}.prod_ecommerce.marts.dim_customers`
                GROUP BY 1 HAVING cnt > 1
            )
        """,
        use_legacy_sql=False,
    )

    # ── 5. Generate dbt docs (data catalog) ──────────────────────────────────

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt docs generate "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET}"
        ),
        doc="Regenerate catalog.json and manifest.json for the data catalog",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── 6. Notify success ────────────────────────────────────────────────────

    def notify_success(**context):
        execution_date = context["execution_date"]
        logging.info(f"ELT pipeline completed successfully for {execution_date}")
        # Slack/Teams webhook call could go here

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─── Dependencies ────────────────────────────────────────────────────────

    (
        check_source_freshness
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> [dbt_test_marts, check_revenue_not_zero, check_no_duplicate_customers]
        >> dbt_docs_generate
        >> notify
    )
