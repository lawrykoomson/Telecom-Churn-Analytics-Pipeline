"""
Apache Airflow DAG — Telecom Churn Analytics Pipeline
======================================================
Schedules the churn analytics pipeline to run every
Sunday at 07:00 AM UTC (weekly subscriber re-scoring).

Tasks:
    1. extract_subscribers   — generate/load subscriber data
    2. transform_features    — engineer churn risk features
    3. load_to_postgres      — load into PostgreSQL
    4. run_dbt_models        — refresh analytical layer
    5. notify_completion     — log completion summary

Author: Lawrence Koomson
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)

default_args = {
    "owner":            "lawrence_koomson",
    "depends_on_past":  False,
    "email":            ["koomsonlawrence64@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="telecom_churn_analytics_pipeline",
    default_args=default_args,
    description="Weekly churn risk scoring for MTN Ghana subscribers",
    schedule_interval="0 7 * * 0",   # Every Sunday at 07:00 AM UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["churn", "telecom", "mtn", "ghana", "data-engineering"],
)


def task_extract(**context):
    from churn_pipeline import extract
    df = extract()
    temp_path = "/tmp/churn_raw.csv"
    df.to_csv(temp_path, index=False)
    context["ti"].xcom_push(key="raw_count", value=len(df))
    context["ti"].xcom_push(key="temp_path", value=temp_path)
    logger.info(f"Extracted {len(df):,} subscribers")
    return len(df)


def task_transform(**context):
    import pandas as pd
    from churn_pipeline import transform
    temp_path = context["ti"].xcom_pull(task_ids="extract_subscribers", key="temp_path")
    df = pd.read_csv(temp_path)
    result = transform(df)
    clean_path = "/tmp/churn_clean.csv"
    result.to_csv(clean_path, index=False)
    context["ti"].xcom_push(key="clean_path",     value=clean_path)
    context["ti"].xcom_push(key="critical_count",
                             value=int((result["churn_risk_tier"] == "Critical").sum()))
    context["ti"].xcom_push(key="revenue_at_risk",
                             value=float(result["revenue_at_risk_ghs"].sum()))
    logger.info(f"Transformed {len(result):,} subscribers")
    return len(result)


def task_load(**context):
    import pandas as pd
    from churn_pipeline import transform, load
    temp_path = context["ti"].xcom_pull(task_ids="extract_subscribers", key="temp_path")
    df = pd.read_csv(temp_path)
    result = transform(df)
    load(result)
    logger.info("Load to PostgreSQL complete")
    return "success"


def task_notify(**context):
    run_date     = context["ds"]
    critical     = context["ti"].xcom_pull(task_ids="transform_features", key="critical_count")
    revenue_risk = context["ti"].xcom_pull(task_ids="transform_features", key="revenue_at_risk")
    raw_count    = context["ti"].xcom_pull(task_ids="extract_subscribers", key="raw_count")

    logger.info("=" * 55)
    logger.info("  CHURN PIPELINE — WEEKLY RUN COMPLETE")
    logger.info("=" * 55)
    logger.info(f"  Run Date           : {run_date}")
    logger.info(f"  Subscribers Scored : {raw_count:,}")
    logger.info(f"  Critical Risk      : {critical:,}")
    logger.info(f"  Revenue at Risk    : GHS {revenue_risk:,.2f}")
    logger.info("=" * 55)
    return "notified"


start = EmptyOperator(task_id="pipeline_start", dag=dag)

extract_task   = PythonOperator(task_id="extract_subscribers",  python_callable=task_extract,   dag=dag)
transform_task = PythonOperator(task_id="transform_features",   python_callable=task_transform,  dag=dag)
load_task      = PythonOperator(task_id="load_to_postgres",     python_callable=task_load,       dag=dag)
notify_task    = PythonOperator(task_id="notify_completion",    python_callable=task_notify,     dag=dag)
end            = EmptyOperator(task_id="pipeline_end", dag=dag)

start >> extract_task >> transform_task >> load_task >> notify_task >> end