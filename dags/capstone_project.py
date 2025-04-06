from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import os
import json
from datetime import datetime

# ---------- TASK 1: Extract ----------
def extract_aqi_data(**context):
    api_key = os.getenv("api_key_AQI")
    url = f"https://api.airvisual.com/v2/city?city=Bangkok&state=Bangkok&country=Thailand&key={api_key}"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    context['ti'].xcom_push(key='raw_data', value=json.dumps(data))

# ---------- TASK 2: Transform ----------
def transform_aqi_data(**context):
    raw_data = json.loads(context['ti'].xcom_pull(task_ids='extract_aqi_data', key='raw_data'))

    aqi = raw_data['data']['current']['pollution']['aqius']
    ts = raw_data['data']['current']['pollution']['ts']
    fetched_at = datetime.utcnow().isoformat()

    if not isinstance(aqi, int) or not (0 <= aqi <= 500):
        raise ValueError(f"Invalid AQI value: {aqi}")
    if ts is None:
        raise ValueError("Missing timestamp in data")

    transformed = {
        "timestamp": ts,
        "aqi": aqi,
        "fetched_at": fetched_at
    }

    context['ti'].xcom_push(key='transformed_data', value=json.dumps(transformed))

# ---------- TASK 3: Load ----------
def load_to_postgres(**context):
    transformed = json.loads(context['ti'].xcom_pull(task_ids='transform_aqi_data', key='transformed_data'))

    hook = PostgresHook(postgres_conn_id="hook_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS aqi_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            aqi INTEGER,
            fetched_at TIMESTAMP
        );
    """)

    cur.execute("""
        INSERT INTO aqi_data (timestamp, aqi, fetched_at)
        VALUES (%s, %s, %s);
    """, (transformed['timestamp'], transformed['aqi'], transformed['fetched_at']))

    conn.commit()
    cur.close()
    conn.close()

# ---------- TASK 4: Build Daily Summary ----------
def build_daily_summary():
    hook = PostgresHook(postgres_conn_id="hook_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS aqi_daily_summary (
            date DATE PRIMARY KEY,
            max_aqi INTEGER,
            min_aqi INTEGER,
            avg_aqi NUMERIC(5,2),
            record_count INTEGER
        );
    """)
    cur.execute("DELETE FROM aqi_daily_summary;")
    cur.execute("""
        INSERT INTO aqi_daily_summary (date, max_aqi, min_aqi, avg_aqi, record_count)
        SELECT
            DATE(timestamp) AS date,
            MAX(aqi),
            MIN(aqi),
            ROUND(AVG(aqi)::numeric, 2),
            COUNT(*)
        FROM aqi_data
        GROUP BY DATE(timestamp)
        ORDER BY DATE(timestamp);
    """)

    conn.commit()
    cur.close()
    conn.close()

# ---------- TASK 5: Build Weekly Summary ----------
def build_weekly_summary():
    hook = PostgresHook(postgres_conn_id="hook_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS aqi_weekly_summary;")
    cur.execute("""
        CREATE TABLE aqi_weekly_summary (
            week_start DATE PRIMARY KEY,
            avg_weekly_aqi NUMERIC(5,2),
            max_aqi INTEGER,
            min_aqi INTEGER
        );
    """)
    cur.execute("""
        INSERT INTO aqi_weekly_summary (week_start, avg_weekly_aqi, max_aqi, min_aqi)
        SELECT
            DATE_TRUNC('week', timestamp)::DATE AS week_start,
            ROUND(AVG(aqi)::numeric, 2),
            MAX(aqi),
            MIN(aqi)
        FROM aqi_data
        GROUP BY DATE_TRUNC('week', timestamp)
        ORDER BY week_start;
    """)

    conn.commit()
    cur.close()
    conn.close()

# ---------- TASK 6: Build Monthly Summary ----------
def build_monthly_summary():
    hook = PostgresHook(postgres_conn_id="hook_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS aqi_monthly_summary;")
    cur.execute("""
        CREATE TABLE aqi_monthly_summary (
            month_start DATE PRIMARY KEY,
            avg_monthly_aqi NUMERIC(5,2),
            max_aqi INTEGER,
            min_aqi INTEGER
        );
    """)
    cur.execute("""
        INSERT INTO aqi_monthly_summary (month_start, avg_monthly_aqi, max_aqi, min_aqi)
        SELECT
            DATE_TRUNC('month', timestamp)::DATE AS month_start,
            ROUND(AVG(aqi)::numeric, 2),
            MAX(aqi),
            MIN(aqi)
        FROM aqi_data
        WHERE timestamp >= CURRENT_DATE - INTERVAL '3 months'
        GROUP BY DATE_TRUNC('month', timestamp)
        ORDER BY month_start;
    """)

    conn.commit()
    cur.close()
    conn.close()

# ---------- DAG ----------
with DAG(
    dag_id="capstone_pipeline",
    start_date=days_ago(1),
    schedule_interval="0 */12 * * *",  # Run every 12 hours
    catchup=False,
    tags=["dpu", "aqi"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_aqi_data",
        python_callable=extract_aqi_data,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="transform_aqi_data",
        python_callable=transform_aqi_data,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_summary,
    )

    t5 = PythonOperator(
        task_id="build_weekly_summary",
        python_callable=build_weekly_summary,
    )

    t6 = PythonOperator(
        task_id="build_monthly_summary",
        python_callable=build_monthly_summary,
    )

    # Task dependencies
    t1 >> t2 >> t3 >> [t4, t5, t6]
