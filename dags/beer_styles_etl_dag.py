from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Saved modules
from include.etl.extract import fetch_styles  # reuse for now
from include.etl.transform import transform_styles_data  # reuse for now
from include.etl.load import load_styles_to_duckdb  # reuse loader

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def extract_styles(**context):
    print("🔄 Extracting hop data...")
    df = fetch_styles()
    if df.empty:
        raise ValueError("❌ DataFrame is empty. Extraction failed or site changed.")
    print(f"✅ Extracted {len(df)} styles")
    context['ti'].xcom_push(key='raw_styles_df', value=df.to_dict(orient='records'))

def transform_styles(**context):
    print("🧪 Transforming hop data...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_styles', key='raw_styles_df')
    df = pd.DataFrame(raw_dict)
    df = transform_styles_data(df)
    print(f"✅ Transformed {len(df)} styles")
    context['ti'].xcom_push(key='clean_styles_df', value=df.to_dict(orient='records'))

def load_styles(**context):
    print("💾 Loading styles to DuckDB...")
    clean_dict = context['ti'].xcom_pull(task_ids='transform_styles', key='clean_styles_df')
    df = pd.DataFrame(clean_dict)
    load_styles_to_duckdb(df)
    print("✅ Done loading to DuckDB.")

with DAG(
    dag_id="styles_etl_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["beer_styles", "etl", "motherduck", "BJCP" ],
) as dag:

    t1 = PythonOperator(
        task_id="extract_styles",
        python_callable=extract_styles,
    )

    t2 = PythonOperator(
        task_id="transform_styles",
        python_callable=transform_styles,
    )

    t3 = PythonOperator(
        task_id="load_styles",
        python_callable=load_styles,
    )

    t1 >> t2 >> t3
