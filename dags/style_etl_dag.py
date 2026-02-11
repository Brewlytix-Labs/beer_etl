"""
Airflow DAG for ETL: MongoDB catalog_styles -> PostgreSQL brewcircuit

This DAG extracts beer styles from MongoDB catalog_styles collection,
transforms them to match the PostgreSQL schema, and loads into:
  - brewcircuit.style_guides
  - brewcircuit.style_categories
  - brewcircuit.styles

Load order respects foreign keys: style_guides -> style_categories -> styles.

Schedule: None (not scheduled; trigger manually if needed).
"""
from datetime import datetime, timedelta
import time
from urllib.parse import quote_plus

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json
import uuid
from typing import List, Dict, Any, Tuple, Optional

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore[misc, assignment]

# Default arguments
default_args = {
    'owner': 'brewlytix-etl',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'style_etl_mongodb_to_postgres',
    default_args=default_args,
    description='ETL DAG to sync beer styles from MongoDB to PostgreSQL (style_guides, style_categories, styles)',
    schedule=None,  # Not scheduled; trigger manually
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'styles', 'mongodb', 'postgresql'],
)


def _agent_log(*, run_id: str, hypothesis_id: str, location: str, message: str, data: Dict[str, Any]) -> None:
    # region agent log
    try:
        payload = {
            "sessionId": "debug-session",
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        # Prefer file logging when available (local dev), but also print so it's visible in Airflow task logs
        try:
            with open("/home/ranojoy/beer_etl/.cursor/debug.log", "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
        except Exception:
            pass
        print("AGENT_LOG " + json.dumps(payload))
    except Exception:
        # Never fail the task because of debug logging
        pass
    # endregion


def _resolve_postgres_conn_id(*, run_id: str) -> str:
    """
    Resolve the Postgres connection id at runtime.

    - Prefers Airflow Variable BREWLYTIX_POSTGRES_CONN_ID if set.
    - Falls back to a small list of common conn_ids.
    - Raises a clear error if none exist.
    """
    # region agent log
    candidates: List[str] = []
    try:
        v = (Variable.get("BREWLYTIX_POSTGRES_CONN_ID", default_var="") or "").strip()
        if v:
            candidates.append(v)
    except Exception:
        pass
    candidates.extend(
        [
            "local-psql",  # primary local Postgres connection
            "brewlytix-postgres",
            "brewlytix-atlas-postgres",
            "postgres_default",
            "postgres",
            "postgresql_default",
        ]
    )
    # de-dup preserve order
    seen = set()
    uniq: List[str] = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            uniq.append(c)

    _agent_log(
        run_id=run_id,
        hypothesis_id="PG1",
        location="style_etl_dag.py:_resolve_postgres_conn_id",
        message="Trying Postgres conn_id candidates",
        data={"candidates": uniq},
    )

    for c in uniq:
        try:
            conn = BaseHook.get_connection(c)
            _agent_log(
                run_id=run_id,
                hypothesis_id="PG2",
                location="style_etl_dag.py:_resolve_postgres_conn_id",
                message="Postgres connection details",
                data={
                    "postgres_conn_id": c,
                    "conn_type": getattr(conn, "conn_type", None),
                    "host": getattr(conn, "host", None),
                    "port": getattr(conn, "port", None),
                    "schema": getattr(conn, "schema", None),
                },
            )
            if (conn.host or "").lower() in {"localhost", "127.0.0.1", "::1"}:
                _agent_log(
                    run_id=run_id,
                    hypothesis_id="PG2",
                    location="style_etl_dag.py:_resolve_postgres_conn_id",
                    message="Suspicious Postgres host for Dockerized Airflow (localhost points to the Airflow container)",
                    data={"postgres_conn_id": c, "host": conn.host, "port": conn.port},
                )
            _agent_log(
                run_id=run_id,
                hypothesis_id="PG1",
                location="style_etl_dag.py:_resolve_postgres_conn_id",
                message="Resolved Postgres conn_id",
                data={"postgres_conn_id": c},
            )
            return c
        except AirflowNotFoundException:
            _agent_log(
                run_id=run_id,
                hypothesis_id="PG1",
                location="style_etl_dag.py:_resolve_postgres_conn_id",
                message="Postgres conn_id not found",
                data={"postgres_conn_id": c},
            )
            continue

    raise AirflowNotFoundException(
        "No Postgres Airflow connection found for this DAG. "
        "Create a Postgres connection in Airflow (Admin → Connections) "
        "and/or set Airflow Variable BREWLYTIX_POSTGRES_CONN_ID to its conn_id. "
        f"Tried: {uniq}"
    )
    # endregion


def _get_mongo_client(conn_id: str = 'brewlytix-atlas-scram'):
    """
    Build a pymongo MongoClient from an Airflow connection (any type, e.g. Generic).
    Works when MongoHook cannot be used because connection type is not 'mongo'.
    For Atlas: uses authSource=admin by default (override via connection Extra).
    """
    if MongoClient is None:
        raise ImportError("pymongo is required; install apache-airflow-providers-mongo or pymongo")
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    host = conn.host or ""
    port = conn.port or 27017
    login = conn.login or ""
    password = conn.password or ""
    schema = conn.schema or ""

    # Atlas SRV: host like cluster0.xxxx.mongodb.net -> use mongodb+srv
    if "mongodb.net" in host or extra.get("srv"):
        # Auth against admin by default (Atlas users are usually in admin); override via Extra
        auth_source = extra.get("authSource", "admin")
        uri = "mongodb+srv://{user}:{password}@{host}/?authSource={auth_source}&retryWrites=true&w=majority".format(
            user=quote_plus(login),
            password=quote_plus(password),
            host=host,
            auth_source=quote_plus(auth_source),
        )
    else:
        auth_source = extra.get("authSource", "admin")
        uri = "mongodb://{user}:{password}@{host}:{port}/?authSource={auth_source}".format(
            user=quote_plus(login),
            password=quote_plus(password),
            host=host,
            port=port,
            auth_source=quote_plus(auth_source),
        )
        if schema:
            uri = "mongodb://{user}:{password}@{host}:{port}/{schema}?authSource={auth_source}".format(
                user=quote_plus(login),
                password=quote_plus(password),
                host=host,
                port=port,
                schema=quote_plus(schema),
                auth_source=quote_plus(auth_source),
            )

    return MongoClient(uri)


def extract_styles_from_mongodb(**context) -> List[Dict[str, Any]]:
    """
    Extract beer styles from MongoDB catalog_styles collection.
    Uses Airflow connection brewlytix-atlas-scram (Generic or MongoDB type).
    """
    client = _get_mongo_client('brewlytix-atlas-scram')
    conn = BaseHook.get_connection('brewlytix-atlas-scram')
    db_name = conn.schema or "admin"
    db = client[db_name]

    collection = db['catalog_styles']
    # Exclude _id (ObjectId) so XCom JSON serialization works
    styles = list(collection.find({}, {"_id": 0}))

    context['ti'].xcom_push(key='extracted_styles', value=styles)
    print(f"Extracted {len(styles)} styles from MongoDB")

    return styles


def transform_style_data(**context) -> Dict[str, Any]:
    """
    Transform MongoDB style documents into three payloads for PostgreSQL:
    - style_guides: list of guide records (key, name, version, source_url, kind, metadata)
    - style_categories: list of category records (style_guide_key, code, name, order_idx)
    - styles: list of style records (style_guide_key, category_code, code, name, attr, requires)

    Uses logical keys (style_guide_key, category_code) so load can resolve IDs after upserts.
    """
    ti = context['ti']
    raw_styles = ti.xcom_pull(key='extracted_styles', task_ids='extract_styles')

    style_guide_key = 'bjcp'
    style_guides = [
        {
            'key': style_guide_key,
            'name': 'BJCP 2021',
            'version': '2021',
            'source_url': 'https://www.bjcp.org/style/',
            'kind': 'bjcp',
            'metadata': {},
        }
    ]

    # Unique categories: (style_guide_key, code, name); order_idx by first occurrence
    seen_categories: Dict[Tuple[str, str], int] = {}
    style_categories: List[Dict[str, Any]] = []
    order_idx = 0
    for style in raw_styles:
        cat_code = style.get('category_number') or style.get('category', '') or ''
        cat_name = style.get('category', '') or ''
        if not cat_code and not cat_name:
            cat_code = 'unknown'
            cat_name = 'Unknown'
        key = (style_guide_key, cat_code)
        if key not in seen_categories:
            seen_categories[key] = order_idx
            style_categories.append({
                'style_guide_key': style_guide_key,
                'code': str(cat_code).strip(),
                'name': str(cat_name).strip(),
                'order_idx': order_idx,
            })
            order_idx += 1

    # Build attr JSON for each style (ranges + sensory)
    transformed_styles: List[Dict[str, Any]] = []
    for style in raw_styles:
        cat_code = style.get('category_number') or style.get('category', '') or 'unknown'
        if not str(cat_code).strip():
            cat_code = 'unknown'

        attr = {
            'og_range': _range_dict(style.get('og_range')),
            'fg_range': _range_dict(style.get('fg_range')),
            'ibu_range': _range_dict(style.get('ibu_range')),
            'srm_range': _range_dict(style.get('srm_range')),
            'overall_impression': style.get('overallimpression') or '',
            'aroma': style.get('aroma') or '',
            'appearance': style.get('appearance') or '',
            'flavor': style.get('flavor') or '',
            'mouthfeel': style.get('mouthfeel') or '',
        }
        transformed_styles.append({
            'style_guide_key': style_guide_key,
            'category_code': str(cat_code).strip(),
            'code': style.get('BJCP_code') or style.get('bjcp_code') or '',
            'name': style.get('name') or '',
            'attr': attr,
            'requires': style.get('requires') if isinstance(style.get('requires'), dict) else {},
        })

    payload = {
        'style_guides': style_guides,
        'style_categories': style_categories,
        'styles': transformed_styles,
    }
    ti.xcom_push(key='transformed_payload', value=payload)
    print(f"Transformed: {len(style_guides)} guides, {len(style_categories)} categories, {len(transformed_styles)} styles")

    return payload


def _range_dict(r: Any) -> Optional[Dict[str, Any]]:
    if not r or not isinstance(r, dict):
        return None
    return {
        'min': r.get('min'),
        'max': r.get('max'),
    }


def load_style_guides_to_postgres(**context) -> Dict[str, str]:
    """
    Upsert style_guides and return mapping: style_guide_key -> style_guide_id (UUID).
    """
    ti = context['ti']
    payload = ti.xcom_pull(key='transformed_payload', task_ids='transform_styles')
    guides = payload['style_guides']

    postgres_conn_id = _resolve_postgres_conn_id(run_id=str(context.get("run_id") or "run"))
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    key_to_id: Dict[str, str] = {}
    for g in guides:
        key = g['key']
        cursor.execute("""
            SELECT style_guide_id
            FROM brewcircuit.style_guides
            WHERE kind = %s AND name = %s AND version = %s
            LIMIT 1
        """, (g['kind'], g['name'], g['version']))
        row = cursor.fetchone()
        if row:
            style_guide_id = str(row[0])
            cursor.execute("""
                UPDATE brewcircuit.style_guides
                SET source_url = COALESCE(NULLIF(%s, ''), source_url),
                    metadata = COALESCE(%s::jsonb, metadata),
                    updated_at = NOW()
                WHERE style_guide_id = %s
            """, (g.get('source_url') or '', json.dumps(g.get('metadata') or {}), style_guide_id))
        else:
            style_guide_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO brewcircuit.style_guides
                (style_guide_id, name, version, source_url, kind, metadata, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
            """, (
                style_guide_id,
                g['name'],
                g['version'],
                g.get('source_url') or '',
                g['kind'],
                json.dumps(g.get('metadata') or {}),
            ))
        key_to_id[key] = style_guide_id
        conn.commit()

    cursor.close()
    conn.close()

    context['ti'].xcom_push(key='style_guide_key_to_id', value=key_to_id)
    print(f"Upserted {len(key_to_id)} style_guides")
    return key_to_id


def load_style_categories_to_postgres(**context) -> Dict[Tuple[str, str], str]:
    """
    Upsert style_categories; requires style_guide_key_to_id from load_style_guides.
    Returns mapping (style_guide_id, category code) -> style_category_id.
    """
    ti = context['ti']
    payload = ti.xcom_pull(key='transformed_payload', task_ids='transform_styles')
    key_to_id = ti.xcom_pull(key='style_guide_key_to_id', task_ids='load_style_guides')
    categories = payload['style_categories']

    postgres_conn_id = _resolve_postgres_conn_id(run_id=str(context.get("run_id") or "run"))
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    id_code_to_category_id: Dict[Tuple[str, str], str] = {}
    for c in categories:
        style_guide_id = key_to_id.get(c['style_guide_key'])
        if not style_guide_id:
            continue
        code = c['code']
        name = c['name']
        order_idx = c.get('order_idx', 0)

        cursor.execute("""
            SELECT style_category_id
            FROM brewcircuit.style_categories
            WHERE style_guide_id = %s AND code = %s
            LIMIT 1
        """, (style_guide_id, code))
        row = cursor.fetchone()
        if row:
            style_category_id = str(row[0])
            cursor.execute("""
                UPDATE brewcircuit.style_categories
                SET name = %s, order_idx = %s, updated_at = NOW()
                WHERE style_category_id = %s
            """, (name, order_idx, style_category_id))
        else:
            style_category_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO brewcircuit.style_categories
                (style_category_id, style_guide_id, code, name, order_idx, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            """, (style_category_id, style_guide_id, code, name, order_idx))
        id_code_to_category_id[(style_guide_id, code)] = style_category_id
        conn.commit()

    cursor.close()
    conn.close()

    # XCom serialization: use list of [k1, k2, v] (JSON-serializable; tuple keys are not)
    serialized = [[k1, k2, v] for (k1, k2), v in id_code_to_category_id.items()]
    context['ti'].xcom_push(key='style_category_map', value=serialized)
    print(f"Upserted {len(id_code_to_category_id)} style_categories")
    return len(id_code_to_category_id)  # return count only; downstream uses style_category_map


def load_styles_to_postgres(**context) -> int:
    """
    Load transformed styles into brewcircuit.styles.
    Uses style_guide_key_to_id and style_category_map from previous tasks.
    UPSERT by (style_guide_id, code).
    """
    ti = context['ti']
    payload = ti.xcom_pull(key='transformed_payload', task_ids='transform_styles')
    key_to_id = ti.xcom_pull(key='style_guide_key_to_id', task_ids='load_style_guides')
    serialized = ti.xcom_pull(key='style_category_map', task_ids='load_style_categories') or []
    # serialized is list of [k1, k2, v] (JSON-serializable)
    style_category_map = {(item[0], item[1]): item[2] for item in serialized}

    styles = payload['styles']

    postgres_conn_id = _resolve_postgres_conn_id(run_id=str(context.get("run_id") or "run"))
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    loaded_count = 0
    updated_count = 0
    error_count = 0

    for style in styles:
        try:
            style_guide_id = key_to_id.get(style['style_guide_key'])
            style_category_id = style_category_map.get((style_guide_id, style['category_code']))
            if not style_guide_id:
                error_count += 1
                continue
            if not style_category_id and style.get('category_code'):
                # Allow styles without category if schema permits; otherwise skip
                pass

            cursor.execute("""
                SELECT style_id
                FROM brewcircuit.styles
                WHERE style_guide_id = %s AND code = %s
            """, (style_guide_id, style['code']))

            existing = cursor.fetchone()
            if existing:
                style_id = existing[0]
                cursor.execute("""
                    UPDATE brewcircuit.styles
                    SET name = %s, attr = %s::jsonb, requires = %s::jsonb,
                        style_category_id = COALESCE(%s::uuid, style_category_id),
                        updated_at = NOW()
                    WHERE style_id = %s
                """, (
                    style['name'],
                    json.dumps(style['attr']),
                    json.dumps(style['requires']),
                    style_category_id,
                    style_id,
                ))
                updated_count += 1
            else:
                style_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO brewcircuit.styles
                    (style_id, style_guide_id, style_category_id, code, name, attr, requires, created_at, updated_at)
                    VALUES (%s, %s, %s::uuid, %s, %s, %s::jsonb, %s::jsonb, NOW(), NOW())
                """, (
                    style_id,
                    style_guide_id,
                    style_category_id,
                    style['code'],
                    style['name'],
                    json.dumps(style['attr']),
                    json.dumps(style['requires']),
                ))
                loaded_count += 1
            conn.commit()
        except Exception as e:
            error_count += 1
            print(f"Error processing style {style.get('code', 'unknown')}: {str(e)}")
            conn.rollback()

    cursor.close()
    conn.close()

    print(f"Styles: {loaded_count} new, {updated_count} updated, {error_count} errors")
    ti.xcom_push(key='loaded_count', value=loaded_count)
    ti.xcom_push(key='updated_count', value=updated_count)
    ti.xcom_push(key='error_count', value=error_count)
    return loaded_count


def verify_style_counts(**context) -> Dict[str, int]:
    """Verify ETL by comparing counts and basic data quality."""
    ti = context['ti']
    extracted = ti.xcom_pull(key='extracted_styles', task_ids='extract_styles')
    loaded_count = ti.xcom_pull(key='loaded_count', task_ids='load_styles')
    updated_count = ti.xcom_pull(key='updated_count', task_ids='load_styles')
    error_count = ti.xcom_pull(key='error_count', task_ids='load_styles')

    postgres_conn_id = _resolve_postgres_conn_id(run_id=str(context.get("run_id") or "run"))
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM brewcircuit.styles")
    styles_total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM brewcircuit.style_categories")
    categories_total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM brewcircuit.style_guides")
    guides_total = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    verification = {
        'mongodb_styles': len(extracted),
        'loaded_new': loaded_count,
        'updated_existing': updated_count,
        'errors': error_count,
        'postgres_styles': styles_total,
        'postgres_style_categories': categories_total,
        'postgres_style_guides': guides_total,
    }
    print(f"Verification: {verification}")

    if error_count and len(extracted) and error_count > len(extracted) * 0.1:
        raise ValueError(f"Too many ETL errors: {error_count} out of {len(extracted)}")

    return verification


# Tasks
extract_task = PythonOperator(
    task_id='extract_styles',
    python_callable=extract_styles_from_mongodb,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_styles',
    python_callable=transform_style_data,
    dag=dag,
)

load_style_guides_task = PythonOperator(
    task_id='load_style_guides',
    python_callable=load_style_guides_to_postgres,
    dag=dag,
)

load_style_categories_task = PythonOperator(
    task_id='load_style_categories',
    python_callable=load_style_categories_to_postgres,
    dag=dag,
)

load_styles_task = PythonOperator(
    task_id='load_styles',
    python_callable=load_styles_to_postgres,
    dag=dag,
)

verify_task = PythonOperator(
    task_id='verify_style_counts',
    python_callable=verify_style_counts,
    dag=dag,
)

# Dependencies: extract -> transform -> load_guides -> load_categories -> load_styles -> verify
extract_task >> transform_task >> load_style_guides_task >> load_style_categories_task >> load_styles_task >> verify_task
