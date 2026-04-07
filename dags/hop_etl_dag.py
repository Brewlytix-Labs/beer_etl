from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Saved modules
from include.etl.extract import fetch_hops_data
from include.etl.load import load_hops_to_duckdb
from include.etl.transform import transform_hop_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def extract_hops(**context):
    print("🔄 Extracting hop data...")
    df = fetch_hops_data()
    if df.empty:
        raise ValueError("❌ DataFrame is empty. Extraction failed or site changed.")
    print(f"✅ Extracted {len(df)} hops")
    context['ti'].xcom_push(key='raw_hops_df', value=df.to_dict(orient='records'))

def transform_hops(**context):
    print("🧪 Transforming hop data...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_hops', key='raw_hops_df')
    df = pd.DataFrame(raw_dict)
    df = transform_hop_data(df)
    print(f"✅ Transformed {len(df)} hops")
    context['ti'].xcom_push(key='clean_hops_df', value=df.to_dict(orient='records'))

def load_hops(**context):
    print("💾 Loading hops to DuckDB...")
    clean_dict = context['ti'].xcom_pull(task_ids='transform_hops', key='clean_hops_df')
    df = pd.DataFrame(clean_dict)
    load_hops_to_duckdb(df)
    print("✅ Done loading to DuckDB.")

with DAG(
    dag_id="hop_etl_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["hops", "etl", "motherduck", "beermaverick"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_hops",
        python_callable=extract_hops,
    )

    t2 = PythonOperator(
        task_id="transform_hops",
        python_callable=transform_hops,
    )

    t3 = PythonOperator(
        task_id="load_hops",
        python_callable=load_hops,
    )

    t1 >> t2 >> t3
