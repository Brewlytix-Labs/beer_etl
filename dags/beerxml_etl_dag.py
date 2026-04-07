from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import numpy as np

# Saved modules
from include.etl.extract import fetch_beerxml_from_s3
from include.etl.flatten import flatten_beerxml_to_json
from include.etl.load_mongodb import load_beerxml_to_mongodb


def clean_data_for_xcom(data):
    """
    Clean data to ensure it's JSON serializable for XCom.
    
    Args:
        data: DataFrame or list of dictionaries
    
    Returns:
        Cleaned data that can be serialized to JSON
    """
    if isinstance(data, pd.DataFrame):
        # Convert DataFrame to list of dictionaries
        data_dict = data.to_dict(orient='records')
    else:
        data_dict = data
    
    cleaned_data = []
    for item in data_dict:
        cleaned_item = {}
        for key, value in item.items():
            # Handle different data types
            if isinstance(value, (list, tuple)):
                # Handle lists and tuples
                cleaned_item[key] = clean_list_for_xcom(value)
            elif isinstance(value, dict):
                # Handle nested dictionaries
                cleaned_item[key] = clean_dict_for_xcom(value)
            elif isinstance(value, (np.ndarray, list)) and len(value) == 0:
                cleaned_item[key] = []
            elif isinstance(value, (np.integer, np.floating)):
                cleaned_item[key] = value.item()  # Convert numpy types to Python types
            elif isinstance(value, pd.Timestamp):
                cleaned_item[key] = value.isoformat() if pd.notna(value) else None
            elif isinstance(value, str):
                cleaned_item[key] = value
            elif value is None:
                cleaned_item[key] = None
            else:
                # Try to handle pandas NaN/NaT values
                try:
                    if pd.isna(value) or value is pd.NaT:
                        cleaned_item[key] = None
                    else:
                        cleaned_item[key] = value
                except (ValueError, TypeError):
                    # If pd.isna fails (e.g., with arrays), just use the value as-is
                    cleaned_item[key] = value
        cleaned_data.append(cleaned_item)
    
    return cleaned_data


def clean_list_for_xcom(lst):
    """Clean a list for JSON serialization."""
    cleaned_list = []
    for item in lst:
        if isinstance(item, dict):
            cleaned_list.append(clean_dict_for_xcom(item))
        elif isinstance(item, (np.integer, np.floating)):
            cleaned_list.append(item.item())
        elif isinstance(item, pd.Timestamp):
            cleaned_list.append(item.isoformat() if pd.notna(item) else None)
        elif isinstance(item, str):
            cleaned_list.append(item)
        elif item is None:
            cleaned_list.append(None)
        else:
            try:
                if pd.isna(item) or item is pd.NaT:
                    cleaned_list.append(None)
                else:
                    cleaned_list.append(item)
            except (ValueError, TypeError):
                cleaned_list.append(item)
    return cleaned_list


def clean_dict_for_xcom(dct):
    """Clean a dictionary for JSON serialization."""
    cleaned_dict = {}
    for key, value in dct.items():
        if isinstance(value, dict):
            cleaned_dict[key] = clean_dict_for_xcom(value)
        elif isinstance(value, (list, tuple)):
            cleaned_dict[key] = clean_list_for_xcom(value)
        elif isinstance(value, (np.integer, np.floating)):
            cleaned_dict[key] = value.item()
        elif isinstance(value, pd.Timestamp):
            cleaned_dict[key] = value.isoformat() if pd.notna(value) else None
        elif isinstance(value, str):
            cleaned_dict[key] = value
        elif value is None:
            cleaned_dict[key] = None
        else:
            try:
                if pd.isna(value) or value is pd.NaT:
                    cleaned_dict[key] = None
                else:
                    cleaned_dict[key] = value
            except (ValueError, TypeError):
                cleaned_dict[key] = value
    return cleaned_dict


def _to_float(value, default=None, min_value=None, max_value=None):
    try:
        if value is None:
            return default
        num = float(value)
        if min_value is not None and num < min_value:
            num = min_value
        if max_value is not None and num > max_value:
            num = max_value
        return num
    except Exception:
        return default


def _to_int(value, default=None, min_value=None, max_value=None):
    try:
        if value is None:
            return default
        num = int(float(value))
        if min_value is not None and num < min_value:
            num = min_value
        if max_value is not None and num > max_value:
            num = max_value
        return num
    except Exception:
        return default


def sanitize_recipes_for_mongo(recipes):
    """
    Normalize and filter flattened recipes to satisfy MongoDB schema constraints.
    - Ensure required fields exist and have correct types
    - Coerce numeric types and clamp ranges
    - Drop invalid sub-items; skip recipes that still violate required array constraints
    """
    valid_recipes = []
    skipped_recipes = 0
    fix_counts = {
        'fermentables_dropped': 0,
        'hops_dropped': 0,
        'yeasts_dropped': 0,
        'miscs_dropped': 0
    }

    for doc in recipes or []:
        if not isinstance(doc, dict):
            continue

        # version >= 1
        doc['version'] = _to_int(doc.get('version', 1), default=1, min_value=1)

        # type enum already mapped upstream; keep as-is

        # batch object with required fields and types
        batch = doc.get('batch') or {}
        batch['target_volume_L'] = _to_float(batch.get('target_volume_L'), default=0.0, min_value=0.0)
        batch['boil_volume_L'] = _to_float(batch.get('boil_volume_L'), default=None, min_value=0.0)
        batch['boil_time_min'] = _to_int(batch.get('boil_time_min'), default=60, min_value=0)
        batch['efficiency_pct'] = _to_int(batch.get('efficiency_pct'), default=75, min_value=0, max_value=100)
        doc['batch'] = batch

        # fermentables array (minItems:1, required: name, amount_g)
        cleaned_fermentables = []
        for f in (doc.get('fermentables') or []):
            if not isinstance(f, dict):
                fix_counts['fermentables_dropped'] += 1
                continue
            name = f.get('name')
            amount_g = _to_float(f.get('amount_g'), default=None, min_value=0.0)
            if not name or amount_g is None:
                fix_counts['fermentables_dropped'] += 1
                continue
            # yields_potential_sg must be double|null with minimum 1.0 when present
            yps = f.get('yields_potential_sg')
            if yps is not None:
                yps = _to_float(yps, default=None, min_value=1.0)
            f['yields_potential_sg'] = yps
            f['amount_g'] = amount_g
            cleaned_fermentables.append(f)
        doc['fermentables'] = cleaned_fermentables

        # hops items (required: name, amount_g, use)
        cleaned_hops = []
        for h in (doc.get('hops') or []):
            if not isinstance(h, dict):
                fix_counts['hops_dropped'] += 1
                continue
            name = h.get('name')
            use = h.get('use') or 'Boil'
            amount_g = _to_float(h.get('amount_g'), default=None, min_value=0.0)
            if not name or amount_g is None or not use:
                fix_counts['hops_dropped'] += 1
                continue
            h['time_min'] = _to_float(h.get('time_min'), default=None, min_value=0.0)
            h['alpha_acid_pct'] = _to_float(h.get('alpha_acid_pct'), default=None, min_value=0.0, max_value=40.0)
            h['amount_g'] = amount_g
            h['use'] = use
            cleaned_hops.append(h)
        doc['hops'] = cleaned_hops

        # yeasts array (minItems:1, required: name, type, form)
        cleaned_yeasts = []
        for y in (doc.get('yeasts') or []):
            if not isinstance(y, dict):
                fix_counts['yeasts_dropped'] += 1
                continue
            name = y.get('name')
            y_type = y.get('type')
            form = y.get('form')
            if not name or not y_type or not form:
                fix_counts['yeasts_dropped'] += 1
                continue
            y['attenuation_pct'] = _to_int(y.get('attenuation_pct'), default=None, min_value=0, max_value=100)
            y['min_temp_C'] = _to_float(y.get('min_temp_C'), default=None)
            y['max_temp_C'] = _to_float(y.get('max_temp_C'), default=None)
            y['amount_cells_billion'] = _to_float(y.get('amount_cells_billion'), default=None, min_value=0.0)
            cleaned_yeasts.append(y)
        doc['yeasts'] = cleaned_yeasts

        # miscs items (optional array; required fields when present)
        cleaned_miscs = []
        for m in (doc.get('miscs') or []):
            if not isinstance(m, dict):
                fix_counts['miscs_dropped'] += 1
                continue
            name = m.get('name')
            use = m.get('use')
            m_type = m.get('type')
            time_min = _to_float(m.get('time_min'), default=None, min_value=0.0)
            amount_g = _to_float(m.get('amount_g'), default=None, min_value=0.0)
            if not name or not use or not m_type or time_min is None or amount_g is None:
                fix_counts['miscs_dropped'] += 1
                continue
            m['time_min'] = _to_float(m.get('time_min'), default=0.0, min_value=0.0)
            m['amount_g'] = _to_float(m.get('amount_g'), default=0.0, min_value=0.0)
            cleaned_miscs.append(m)
        doc['miscs'] = cleaned_miscs if cleaned_miscs else None

        # audit.created_at must exist (string/date). Flatten already sets; ensure string fallback
        audit = doc.get('audit') or {}
        audit['created_at'] = audit.get('created_at') or datetime.now().isoformat()
        doc['audit'] = audit

        # Required top-level strings
        org_id = doc.get('org_id')
        name = doc.get('name')
        r_type = doc.get('type')

        # Enforce required arrays minItems
        if not org_id or not name or not r_type or len(doc['fermentables']) == 0 or len(doc['yeasts']) == 0:
            skipped_recipes += 1
            continue

        valid_recipes.append(doc)

    print(f"🧹 Sanitation: kept={len(valid_recipes)}, skipped={skipped_recipes}, fixes={fix_counts}")
    return valid_recipes

def extract_beerxml(**context):
    """
    Extract BeerXML recipe data from S3 bucket.
    """
    # Get S3 configuration from Airflow Variables
    bucket_name = Variable.get("BEERXML_S3_BUCKET", default_var="beer-etl")
    prefix = Variable.get("BEERXML_S3_PREFIX", default_var="raw-clone-recipes/clone-recipes/")
    connection_id = Variable.get("BEERXML_AWS_CONNECTION_ID", default_var="aws-s3-user")
    
    print(f"🔄 Extracting BeerXML recipes from S3 bucket: {bucket_name}, prefix: {prefix}")
    print(f"🔗 Using AWS connection: {connection_id}")
    
    try:
        # Fetch BeerXML data from S3
        recipes_data = fetch_beerxml_from_s3(
            bucket_name=bucket_name, 
            prefix=prefix, 
            connection_id=connection_id
        )
        
        if not recipes_data:
            raise ValueError("❌ No BeerXML recipe data extracted from S3 bucket")
        
        print(f"✅ Extracted {len(recipes_data)} BeerXML recipes from S3")
        
        # Clean the raw data for JSON serialization
        cleaned_recipes_data = clean_data_for_xcom(recipes_data)
        context['ti'].xcom_push(key='raw_beerxml_recipes', value=cleaned_recipes_data)
        
    except Exception as e:
        print(f"❌ Error extracting BeerXML data: {str(e)}")
        raise


def flatten_beerxml(**context):
    """
    Flatten raw BeerXML recipe data into JSON format for MongoDB with timezone support.
    """
    print("🧪 Flattening BeerXML recipe data to JSON format...")
    
    try:
        # Pull raw data from previous task
        raw_recipes = context['ti'].xcom_pull(task_ids='extract_beerxml', key='raw_beerxml_recipes')
        
        if not raw_recipes:
            raise ValueError("❌ No raw BeerXML data found in XCom")
        
        print(f"📊 Processing {len(raw_recipes)} raw BeerXML recipes")
        
        # Get configuration from Airflow Variables
        org_id = Variable.get("BREWLYTIX_ORG_ID", default_var="brewlytix")
        timezone_name = Variable.get("BREWLYTIX_TIMEZONE", default_var="UTC")
        
        print(f"🌍 Using timezone: {timezone_name}")
        
        # Flatten each recipe to JSON format
        flattened_recipes = []
        duplicate_count = 0
        content_hashes = set()
        
        for recipe in raw_recipes:
            try:
                # Extract XML content and file key from the recipe data
                xml_content = recipe.get('xml_content', '')
                file_key = recipe.get('file_key', '')
                
                if xml_content and file_key:
                    # Flatten the BeerXML to JSON format with timezone
                    flattened_recipe = flatten_beerxml_to_json(xml_content, file_key, org_id, timezone_name)
                    if flattened_recipe:
                        # Check for content-based duplicates
                        for recipe_doc in flattened_recipe:
                            try:
                                content_hash = recipe_doc.get('content_hash')
                                if content_hash and content_hash in content_hashes:
                                    duplicate_count += 1
                                    print(f"⚠️ Skipping duplicate recipe: {recipe_doc.get('name', 'unknown')} (content hash: {content_hash[:8]}...)")
                                    continue
                                
                                if content_hash:
                                    content_hashes.add(content_hash)
                                
                                flattened_recipes.append(recipe_doc)
                            except Exception as e:
                                print(f"⚠️ Error processing recipe {recipe_doc.get('name', 'unknown')}: {str(e)}")
                                # Still add the recipe even if hash processing failed
                                flattened_recipes.append(recipe_doc)
                else:
                    print(f"⚠️ Skipping recipe with missing XML content or file key")
            except Exception as e:
                print(f"⚠️ Error flattening recipe {recipe.get('file_key', 'unknown')}: {str(e)}")
                continue
        
        print(f"✅ Flattened {len(flattened_recipes)} BeerXML recipes to JSON format")
        print(f"📈 Flattening summary:")
        print(f"   - Total recipes processed: {len(flattened_recipes)}")
        print(f"   - Duplicates skipped: {duplicate_count}")
        print(f"   - Timezone: {timezone_name}")
        
        # Count recipe types
        recipe_types = {}
        for recipe in flattened_recipes:
            recipe_type = recipe.get('type', 'Unknown')
            recipe_types[recipe_type] = recipe_types.get(recipe_type, 0) + 1
        
        print(f"   - Recipe types: {recipe_types}")
        
        # Clean the flattened data for JSON serialization
        cleaned_flattened_data = clean_data_for_xcom(flattened_recipes)
        
        # Push flattened data to XCom
        context['ti'].xcom_push(key='flattened_beerxml_recipes', value=cleaned_flattened_data)
        
    except Exception as e:
        print(f"❌ Error flattening BeerXML data: {str(e)}")
        raise


def load_beerxml(**context):
    """
    Load flattened BeerXML recipe data to MongoDB with duplicate handling.
    
    Duplicate Handling Strategies:
    - upsert: Update existing recipes, insert new ones (default)
    - skip: Only insert new recipes, skip duplicates
    - insert: Original behavior - fail on duplicates
    
    Configure via Airflow Variable: BREWLYTIX_DUPLICATE_STRATEGY
    """
    print("💾 Loading BeerXML recipes to MongoDB...")
    
    try:
        # Pull flattened data from previous task
        flattened_recipes = context['ti'].xcom_pull(task_ids='flatten_beerxml', key='flattened_beerxml_recipes')
        
        if not flattened_recipes:
            raise ValueError("❌ No flattened BeerXML data found in XCom")
        
        # Sanitize per MongoDB schema before load
        sanitized_recipes = sanitize_recipes_for_mongo(flattened_recipes)

        # Get MongoDB configuration from Airflow Variables
        database_name = Variable.get("BREWLYTIX_DB_NAME", default_var="brewlytix")
        collection_name = Variable.get("BREWLYTIX_COLLECTION_NAME", default_var="recipes")
        connection_id = Variable.get("BREWLYTIX_MONGODB_CONNECTION_ID", default_var="brewlytix-atlas-scram")
        
        # Get duplicate handling strategy
        try:
            duplicate_strategy = Variable.get("BREWLYTIX_DUPLICATE_STRATEGY", default_var="upsert")
            print(f"🔄 Duplicate handling strategy: {duplicate_strategy}")
        except Exception:
            duplicate_strategy = "upsert"
            print(f"🔄 Using default duplicate handling strategy: {duplicate_strategy}")
        
        # Load to MongoDB with duplicate handling
        load_beerxml_to_mongodb(
            recipes_data=sanitized_recipes,
            database_name=database_name,
            collection_name=collection_name,
            connection_id=connection_id
        )
        
        print("✅ Successfully loaded BeerXML recipes to MongoDB")
        
        # Print summary statistics
        print(f"📊 Load Summary:")
        print(f"   - Total recipes processed: {len(sanitized_recipes)}")
        print(f"   - Database: {database_name}")
        print(f"   - Collection: {collection_name}")
        print(f"   - Duplicate strategy: {duplicate_strategy}")
        
        # Count unique values
        unique_brewers = len(set(recipe.get('brewer', '') for recipe in flattened_recipes if recipe.get('brewer')))
        unique_styles = len(set(recipe.get('style', {}).get('name', '') for recipe in flattened_recipes if recipe.get('style', {}).get('name')))
        
        print(f"   - Unique brewers: {unique_brewers}")
        print(f"   - Unique styles: {unique_styles}")
        
        # Count recipe types
        recipe_types = {}
        for recipe in flattened_recipes:
            recipe_type = recipe.get('type', 'Unknown')
            recipe_types[recipe_type] = recipe_types.get(recipe_type, 0) + 1
        
        print(f"   - Recipe types: {recipe_types}")
        
    except Exception as e:
        print(f"❌ Error loading BeerXML data: {str(e)}")
        raise


# Define the DAG
with DAG(
    dag_id="beerxml_etl_dag",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
        'retry_delay': 300,  # 5 minutes
    },
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["beerxml", "etl", "s3", "motherduck", "recipes"],
    description="ETL pipeline for BeerXML recipe files stored in S3",
) as dag:

    # Extract task
    extract_task = PythonOperator(
        task_id="extract_beerxml",
        python_callable=extract_beerxml,
    )

    # Flatten task
    flatten_task = PythonOperator(
        task_id="flatten_beerxml",
        python_callable=flatten_beerxml,
    )

    # Load task
    load_task = PythonOperator(
        task_id="load_beerxml",
        python_callable=load_beerxml,
    )

    # Define task dependencies
    extract_task >> flatten_task >> load_task
