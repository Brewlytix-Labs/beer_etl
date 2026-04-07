from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd

# Saved modules
from include.etl.extract import fetch_clone_recipes  # reuse for now
from include.etl.transform import transform_clone_recipes_data  # reuse for now
from include.etl.load import load_clone_recipes_to_duckdb  # reuse loader

def extract_clone_recipes(**context):
    # Get max pages to scrape from Airflow Variable (defaults to 30)
    max_pages = int(Variable.get("CLONE_MAX_PAGES", default_var="30"))
    print(f"🔄 Extracting clone recipes (up to {max_pages} pages)...")
    df = fetch_clone_recipes(max_pages=max_pages)
    if df.empty:
        raise ValueError("❌ DataFrame is empty. Extraction failed or site changed.")
    print(f"✅ Extracted {len(df)} clone recipes from up to {max_pages} pages")
    context['ti'].xcom_push(key='raw_clone_recipes_df', value=df.to_dict(orient='records'))


def transform_clone_recipes(**context):
    print("🧪 Transforming clone recipes data...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_clone_recipes', key='raw_clone_recipes_df')
    df = pd.DataFrame(raw_dict)
    df = transform_clone_recipes_data(df)
    print(f"✅ Transformed {len(df)} clone recipes")
    context['ti'].xcom_push(key='clean_clone_recipes_df', value=df.to_dict(orient='records'))


def load_clone_recipes(**context):
    print("💾 Loading clone recipes to DuckDB...")
    clean_dict = context['ti'].xcom_pull(task_ids='transform_clone_recipes', key='clean_clone_recipes_df')
    df = pd.DataFrame(clean_dict)
    load_clone_recipes_to_duckdb(df)
    print("✅ Done loading to DuckDB.")

with DAG(
    dag_id="clone_recipes_etl_dag",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
    },
    schedule=None,
    catchup=False,
    tags=["beer_clone_recipes", "etl", "motherduck", "BJCP", "Brewer's Friend", "parameters"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_clone_recipes",
        python_callable=extract_clone_recipes,
    )

    t2 = PythonOperator(
        task_id="transform_clone_recipes",
        python_callable=transform_clone_recipes,
    )

    t3 = PythonOperator(
        task_id="load_clone_recipes",
        python_callable=load_clone_recipes,
    )

    t1 >> t2 >> t3
