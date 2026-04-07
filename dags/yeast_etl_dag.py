from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Saved modules
from include.etl.extract import fetch_yeasts_data
from include.etl.load import load_yeasts_to_duckdb
from include.etl.transform import transform_yeast_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def extract_yeasts(**context):
    print("🔄 Extracting yeast data...")
    df = fetch_yeasts_data()  # Assuming this function fetches yeast data
    if df.empty:
        raise ValueError("❌ DataFrame is empty. Extraction failed or site changed.")
    print(f"✅ Extracted {len(df)} yeasts")
    context['ti'].xcom_push(key='raw_yeasts_df', value=df.to_dict(orient='records'))

def transform_yeasts(**context):
    print("🧪 Transforming yeast data...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_yeasts', key='raw_yeasts_df')
    df = pd.DataFrame(raw_dict)
    df = transform_yeast_data(df)  # Assuming this function transforms yeast data
    print(f"✅ Transformed {len(df)} yeasts")
    context['ti'].xcom_push(key='clean_yeasts_df', value=df.to_dict(orient='records'))

def load_yeasts(**context):
    print("💾 Loading yeasts to DuckDB...")
    clean_dict = context['ti'].xcom_pull(task_ids='transform_yeasts', key='clean_yeasts_df')
    df = pd.DataFrame(clean_dict)
    load_yeasts_to_duckdb(df)  # Assuming this function loads yeast data to DuckDB
    print("✅ Done loading to DuckDB.")

with DAG(
    dag_id="yeast_etl_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["yeasts", "etl", "motherduck", "beermaverick"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_yeasts",
        python_callable=extract_yeasts,
    )

    t2 = PythonOperator(
        task_id="transform_yeasts",
        python_callable=transform_yeasts,
    )

    t3 = PythonOperator(
        task_id="load_yeasts",
        python_callable=load_yeasts,
    )

    t1 >> t2 >> t3  # Set task dependencies 
