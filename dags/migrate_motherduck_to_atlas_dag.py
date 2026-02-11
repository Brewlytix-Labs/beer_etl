from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import duckdb
from airflow.models import Variable
import os

# Import our migration functions
from include.etl.migrate_motherduck_to_mongodb import (
    extract_from_motherduck,
    transform_for_mongodb,
    load_catalog_to_mongodb
)

def convert_dataframe_to_json_safe(df):
    """Convert DataFrame to JSON-serializable format, handling data types properly."""
    df_clean = df.copy()
    
    # Replace NaN values with None (which becomes null in JSON)
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    # Convert to records, handling data types properly
    records = []
    for _, row in df_clean.iterrows():
        record = {}
        for col, value in row.items():
            # Handle different data types safely
            if value is None:
                record[col] = None
            elif isinstance(value, np.ndarray):
                # Handle arrays
                if value.size == 0:
                    record[col] = []
                elif value.size == 1:
                    record[col] = value.item() if not pd.isna(value.item()) else None
                else:
                    record[col] = value.tolist()
            elif isinstance(value, (np.integer, np.floating)):
                # Handle numpy numeric types
                if pd.isna(value):
                    record[col] = None
                else:
                    record[col] = value.item()
            elif isinstance(value, str):
                # Handle strings
                record[col] = value if value != '' else None
            else:
                # Handle other types
                try:
                    if pd.isna(value):
                        record[col] = None
                    else:
                        record[col] = str(value)
                except (ValueError, TypeError):
                    # If pd.isna fails, just convert to string
                    record[col] = str(value) if value is not None else None
        records.append(record)
    
    return records

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': 300,  # 5 minutes
}

def extract_styles_from_motherduck(**context):
    """Extract beer styles data from MotherDuck"""
    print("🔄 Extracting beer styles from MotherDuck...")
    df = extract_from_motherduck("beer_styles")
    if df.empty:
        raise ValueError("❌ No beer styles data found in MotherDuck")
    print(f"✅ Extracted {len(df)} beer styles records")
    
    # Convert DataFrame to JSON-serializable format
    records = convert_dataframe_to_json_safe(df)
    context['ti'].xcom_push(key='raw_styles_df', value=records)
    return len(df)

def transform_styles_for_mongodb(**context):
    """Transform beer styles data for MongoDB catalog_styles collection"""
    print("🧪 Transforming beer styles data for MongoDB...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_styles_from_motherduck', key='raw_styles_df')
    df = pd.DataFrame(raw_dict)
    
    # Transform for catalog_styles collection schema
    transformed_data = transform_for_mongodb(df, collection_type="catalog_styles")
    
    print(f"✅ Transformed {len(transformed_data)} styles records")
    context['ti'].xcom_push(key='transformed_styles', value=transformed_data)
    return len(transformed_data)

def load_styles_to_mongodb(**context):
    """Load transformed styles data to MongoDB catalog_styles collection"""
    print("💾 Loading styles to MongoDB catalog_styles collection...")
    transformed_data = context['ti'].xcom_pull(task_ids='transform_styles_for_mongodb', key='transformed_styles')
    
    result = load_catalog_to_mongodb(
        data=transformed_data,
        collection_name="catalog_styles",
        connection_id="brewlytix-atlas-scram"
    )
    
    print(f"✅ Loaded styles to MongoDB: {result}")
    return result

def extract_hops_from_motherduck(**context):
    """Extract hops data from MotherDuck"""
    print("🔄 Extracting hops from MotherDuck...")
    df = extract_from_motherduck("hops")
    if df.empty:
        raise ValueError("❌ No hops data found in MotherDuck")
    print(f"✅ Extracted {len(df)} hops records")
    
    # Convert DataFrame to JSON-serializable format
    records = convert_dataframe_to_json_safe(df)
    context['ti'].xcom_push(key='raw_hops_df', value=records)
    return len(df)

def transform_hops_for_mongodb(**context):
    """Transform hops data for MongoDB catalog_hops collection"""
    print("🧪 Transforming hops data for MongoDB...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_hops_from_motherduck', key='raw_hops_df')
    df = pd.DataFrame(raw_dict)
    
    # Transform for catalog_hops collection schema
    transformed_data = transform_for_mongodb(df, collection_type="catalog_hops")
    
    print(f"✅ Transformed {len(transformed_data)} hops records")
    context['ti'].xcom_push(key='transformed_hops', value=transformed_data)
    return len(transformed_data)

def load_hops_to_mongodb(**context):
    """Load transformed hops data to MongoDB catalog_hops collection"""
    print("💾 Loading hops to MongoDB catalog_hops collection...")
    transformed_data = context['ti'].xcom_pull(task_ids='transform_hops_for_mongodb', key='transformed_hops')
    
    result = load_catalog_to_mongodb(
        data=transformed_data,
        collection_name="catalog_hops",
        connection_id="brewlytix-atlas-scram"
    )
    
    print(f"✅ Loaded hops to MongoDB: {result}")
    return result

def extract_fermentables_from_motherduck(**context):
    """Extract fermentables data from MotherDuck"""
    print("🔄 Extracting fermentables from MotherDuck...")
    df = extract_from_motherduck("fermentables")
    if df.empty:
        raise ValueError("❌ No fermentables data found in MotherDuck")
    print(f"✅ Extracted {len(df)} fermentables records")
    
    # Convert DataFrame to JSON-serializable format
    records = convert_dataframe_to_json_safe(df)
    context['ti'].xcom_push(key='raw_fermentables_df', value=records)
    return len(df)

def transform_fermentables_for_mongodb(**context):
    """Transform fermentables data for MongoDB catalog_fermentables collection"""
    print("🧪 Transforming fermentables data for MongoDB...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_fermentables_from_motherduck', key='raw_fermentables_df')
    df = pd.DataFrame(raw_dict)
    
    # Transform for catalog_fermentables collection schema
    transformed_data = transform_for_mongodb(df, collection_type="catalog_fermentables")
    
    print(f"✅ Transformed {len(transformed_data)} fermentables records")
    context['ti'].xcom_push(key='transformed_fermentables', value=transformed_data)
    return len(transformed_data)

def load_fermentables_to_mongodb(**context):
    """Load transformed fermentables data to MongoDB catalog_fermentables collection"""
    print("💾 Loading fermentables to MongoDB catalog_fermentables collection...")
    transformed_data = context['ti'].xcom_pull(task_ids='transform_fermentables_for_mongodb', key='transformed_fermentables')
    
    result = load_catalog_to_mongodb(
        data=transformed_data,
        collection_name="catalog_fermentables",
        connection_id="brewlytix-atlas-scram"
    )
    
    print(f"✅ Loaded fermentables to MongoDB: {result}")
    return result

def extract_yeasts_from_motherduck(**context):
    """Extract yeasts data from MotherDuck"""
    print("🔄 Extracting yeasts from MotherDuck...")
    df = extract_from_motherduck("yeasts")
    if df.empty:
        raise ValueError("❌ No yeasts data found in MotherDuck")
    print(f"✅ Extracted {len(df)} yeasts records")
    
    # Convert DataFrame to JSON-serializable format
    records = convert_dataframe_to_json_safe(df)
    context['ti'].xcom_push(key='raw_yeasts_df', value=records)
    return len(df)

def transform_yeasts_for_mongodb(**context):
    """Transform yeasts data for MongoDB catalog_yeasts collection"""
    print("🧪 Transforming yeasts data for MongoDB...")
    raw_dict = context['ti'].xcom_pull(task_ids='extract_yeasts_from_motherduck', key='raw_yeasts_df')
    df = pd.DataFrame(raw_dict)
    
    # Transform for catalog_yeasts collection schema
    transformed_data = transform_for_mongodb(df, collection_type="catalog_yeasts")
    
    print(f"✅ Transformed {len(transformed_data)} yeasts records")
    context['ti'].xcom_push(key='transformed_yeasts', value=transformed_data)
    return len(transformed_data)

def load_yeasts_to_mongodb(**context):
    """Load transformed yeasts data to MongoDB catalog_yeasts collection"""
    print("💾 Loading yeasts to MongoDB catalog_yeasts collection...")
    transformed_data = context['ti'].xcom_pull(task_ids='transform_yeasts_for_mongodb', key='transformed_yeasts')
    
    result = load_catalog_to_mongodb(
        data=transformed_data,
        collection_name="catalog_yeasts",
        connection_id="brewlytix-atlas-scram"
    )
    
    print(f"✅ Loaded yeasts to MongoDB: {result}")
    return result

def migration_summary(**context):
    """Print summary of migration tasks (styles only for now)"""
    print("📊 Migration Summary:")
    
    # Get results from styles task only
    styles_result = context['ti'].xcom_pull(task_ids='load_styles_to_mongodb')
    
    # Extract counts from results (handle both dict and int returns)
    def extract_count(result):
        if isinstance(result, dict):
            return result.get('inserted', 0) + result.get('modified', 0)
        elif isinstance(result, int):
            return result
        else:
            return 0
    
    styles_count = extract_count(styles_result)
    
    print(f"✅ Beer Styles migrated: {styles_count}")
    print(f"⏸️ Other migrations disabled for now")
    
    total = styles_count
    print(f"🎉 Total records migrated: {total}")
    
    # Print detailed results if available
    if isinstance(styles_result, dict):
        print(f"📋 Detailed Results:")
        print(f"   Styles: {styles_result}")
    
    return total

with DAG(
    dag_id="migrate_motherduck_to_atlas_dag",
    default_args=default_args,
    description="Migrate catalog data from MotherDuck to MongoDB Atlas",
    schedule_interval=None,
    catchup=False,
    tags=["migration", "motherduck", "mongodb", "atlas", "catalog"],
    doc_md="""
    # MotherDuck to MongoDB Atlas Migration DAG
    
    This DAG migrates catalog data from MotherDuck DuckDB to MongoDB Atlas collections.
    Currently focused on catalog_styles migration only.
    
    ## Current Migration: catalog_styles
    - **Required fields**: name, BJCP_code
    - **Category fields**: category, category_number
    - **Descriptive fields**: overallimpression, aroma, appearance, flavor, mouthfeel
    - **Style guides**: Array of style guide references
    - **Ranges**: OG, FG, IBU, SRM ranges with min/max values
    
    ## Prerequisites
    - MotherDuck token configured in Airflow Variables
    - MongoDB Atlas connection 'brewlytix-atlas-scram' configured in Airflow Connections
    - Source tables must exist in MotherDuck 'beer_etl' database
    
    ## Schema Mapping
    - name ← name (required)
    - BJCP_code ← number (required)
    - category ← category
    - category_number ← categorynumber
    - overallimpression ← overallimpression
    - aroma ← aroma
    - appearance ← appearance
    - flavor ← flavor
    - mouthfeel ← mouthfeel
    - style_guides ← currentlydefinedtypes/tags
    - og_range ← ogmin/ogmax
    - fg_range ← fgmin/fgmax
    - ibu_range ← ibumin/ibumax
    - srm_range ← srmmin/srmmax
    """,
) as dag:

    # Beer Styles Migration (starting point as requested)
    extract_styles_task = PythonOperator(
        task_id="extract_styles_from_motherduck",
        python_callable=extract_styles_from_motherduck,
        provide_context=True,
    )

    transform_styles_task = PythonOperator(
        task_id="transform_styles_for_mongodb",
        python_callable=transform_styles_for_mongodb,
        provide_context=True,
    )

    load_styles_task = PythonOperator(
        task_id="load_styles_to_mongodb",
        python_callable=load_styles_to_mongodb,
        provide_context=True,
    )

    # Hops Migration - DISABLED FOR NOW
    # extract_hops_task = PythonOperator(
    #     task_id="extract_hops_from_motherduck",
    #     python_callable=extract_hops_from_motherduck,
    #     provide_context=True,
    # )

    # transform_hops_task = PythonOperator(
    #     task_id="transform_hops_for_mongodb",
    #     python_callable=transform_hops_for_mongodb,
    #     provide_context=True,
    # )

    # load_hops_task = PythonOperator(
    #     task_id="load_hops_to_mongodb",
    #     python_callable=load_hops_to_mongodb,
    #     provide_context=True,
    # )

    # Fermentables Migration - DISABLED FOR NOW
    # extract_fermentables_task = PythonOperator(
    #     task_id="extract_fermentables_from_motherduck",
    #     python_callable=extract_fermentables_from_motherduck,
    #     provide_context=True,
    # )

    # transform_fermentables_task = PythonOperator(
    #     task_id="transform_fermentables_for_mongodb",
    #     python_callable=transform_fermentables_for_mongodb,
    #     provide_context=True,
    # )

    # load_fermentables_task = PythonOperator(
    #     task_id="load_fermentables_to_mongodb",
    #     python_callable=load_fermentables_to_mongodb,
    #     provide_context=True,
    # )

    # Yeasts Migration - DISABLED FOR NOW
    # extract_yeasts_task = PythonOperator(
    #     task_id="extract_yeasts_from_motherduck",
    #     python_callable=extract_yeasts_from_motherduck,
    #     provide_context=True,
    # )

    # transform_yeasts_task = PythonOperator(
    #     task_id="transform_yeasts_for_mongodb",
    #     python_callable=transform_yeasts_for_mongodb,
    #     provide_context=True,
    # )

    # load_yeasts_task = PythonOperator(
    #     task_id="load_yeasts_to_mongodb",
    #     python_callable=load_yeasts_to_mongodb,
    #     provide_context=True,
    # )

    # Summary task
    summary_task = PythonOperator(
        task_id="migration_summary",
        python_callable=migration_summary,
        provide_context=True,
        trigger_rule="all_done",  # Run even if some tasks fail
    )

    # Define task dependencies
    # Start with beer styles as requested
    extract_styles_task >> transform_styles_task >> load_styles_task
    
    # Other migrations disabled for now
    # extract_hops_task >> transform_hops_task >> load_hops_task
    # extract_fermentables_task >> transform_fermentables_task >> load_fermentables_task
    # extract_yeasts_task >> transform_yeasts_task >> load_yeasts_task
    
    # Only styles migration feeds into summary for now
    load_styles_task >> summary_task
