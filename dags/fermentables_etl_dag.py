from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Saved modules
from include.etl.extract import fetch_fermentables  # reuse for now
from include.etl.transform import transform_fermentables_data  # reuse for now
from include.etl.load import load_fermentables_to_duckdb  # reuse loader

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def extract_fermentables(**context):
    print("🔄 Extracting hop data...")
    df = fetch_fermentables()
    if df.empty:
        raise ValueError("❌ DataFrame is empty. Extraction failed or site changed.")
    print(f"✅ Extracted {len(df)} fermentables")
    context['ti'].xcom_push(key='raw_fermentables_df', value=df.to_dict(orient='records'))

def transform_fermentables(**context):
    print("🧪 Transforming hop data...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_fermentables', key='raw_fermentables_df')
    df = pd.DataFrame(raw_dict)
    df = transform_fermentables_data(df)
    print(f"✅ Transformed {len(df)} fermentables")
    context['ti'].xcom_push(key='clean_fermentables_df', value=df.to_dict(orient='records'))

def load_fermentables(**context):
    print("💾 Loading fermentables to DuckDB...")
    clean_dict = context['ti'].xcom_pull(task_ids='transform_fermentables', key='clean_fermentables_df')
    df = pd.DataFrame(clean_dict)
    load_fermentables_to_duckdb(df)
    print("✅ Done loading to DuckDB.")

with DAG(
    dag_id="fermentables_etl_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["fermentables", "etl", "motherduck", "beermaverick"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_fermentables",
        python_callable=extract_fermentables,
    )

    t2 = PythonOperator(
        task_id="transform_fermentables",
        python_callable=transform_fermentables,
    )

    t3 = PythonOperator(
        task_id="load_fermentables",
        python_callable=load_fermentables,
    )

    t1 >> t2 >> t3
