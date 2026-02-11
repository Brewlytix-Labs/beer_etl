"""
Migration utilities for transferring data from MotherDuck DuckDB to MongoDB Atlas.
This module handles extraction, transformation, and loading of catalog data.
"""

import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from datetime import datetime, timezone
import logging
import os

# Import MongoDB utilities
from .load_mongodb import _build_mongo_uri, validate_mongodb_connection

logger = logging.getLogger(__name__)

def extract_from_motherduck(table_name: str, database_name: str = "beer_etl") -> pd.DataFrame:
    """
    Extract data from MotherDuck DuckDB table.
    
    Args:
        table_name: Name of the table to extract from
        database_name: MotherDuck database name (default: "beer_etl")
    
    Returns:
        DataFrame containing the extracted data
    
    Raises:
        ValueError: If table doesn't exist or is empty
        Exception: If connection or extraction fails
    """
    try:
        # Get MotherDuck token from Airflow Variables
        token = Variable.get("MOTHERDUCK_TOKEN")
        os.environ["MOTHERDUCK_TOKEN"] = token
        
        # Check DuckDB version compatibility
        try:
            import duckdb
            duckdb_version = duckdb.__version__
            logger.info(f"🔍 DuckDB version: {duckdb_version}")
            
            # Check if version is compatible with MotherDuck
            if duckdb_version.startswith("1.4"):
                logger.warning("⚠️ DuckDB v1.4.x detected. MotherDuck requires v1.3.2. Consider downgrading.")
                logger.warning("💡 Run: pip install duckdb==1.3.2")
                
        except Exception as ve:
            logger.warning(f"⚠️ Could not check DuckDB version: {ve}")
        
        # Connect to MotherDuck with error handling
        try:
            con = duckdb.connect(f"md:{database_name}")
        except Exception as connection_error:
            if "not yet supported by MotherDuck" in str(connection_error):
                logger.error("❌ DuckDB version incompatibility detected!")
                logger.error("🔧 Solution: Downgrade DuckDB to v1.3.2")
                logger.error("   Run: pip install duckdb==1.3.2")
                logger.error("   Or add 'duckdb==1.3.2' to requirements.txt and rebuild Astro")
                raise Exception("DuckDB version incompatibility. Please downgrade to v1.3.2 for MotherDuck support.")
            else:
                raise connection_error
        
        # Check if table exists
        tables_result = con.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables_result]
        
        if table_name not in table_names:
            raise ValueError(f"❌ Table '{table_name}' not found in MotherDuck database '{database_name}'. Available tables: {table_names}")
        
        # Extract data
        query = f"SELECT * FROM {table_name}"
        df = con.execute(query).df()
        
        con.close()
        
        if df.empty:
            raise ValueError(f"❌ Table '{table_name}' is empty")
        
        logger.info(f"✅ Extracted {len(df)} records from {table_name}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error extracting from MotherDuck table '{table_name}': {str(e)}")
        raise

def transform_for_mongodb(df: pd.DataFrame, collection_type: str) -> List[Dict[str, Any]]:
    """
    Transform DataFrame data to match MongoDB collection schema.
    
    Args:
        df: Source DataFrame
        collection_type: Target collection type (catalog_styles, catalog_hops, etc.)
    
    Returns:
        List of dictionaries ready for MongoDB insertion
    """
    if df.empty:
        return []
    
    # Convert DataFrame to list of dictionaries
    records = df.to_dict(orient='records')
    
    # Transform based on collection type
    if collection_type == "catalog_styles":
        return transform_styles_data(records)
    elif collection_type == "catalog_hops":
        return transform_hops_data(records)
    elif collection_type == "catalog_fermentables":
        return transform_fermentables_data(records)
    elif collection_type == "catalog_yeasts":
        return transform_yeasts_data(records)
    else:
        raise ValueError(f"❌ Unknown collection type: {collection_type}")

def transform_styles_data(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform beer styles data to match catalog_styles schema.
    Maps MotherDuck beer_styles columns to MongoDB catalog_styles schema.
    """
    transformed = []
    
    for record in records:
        # Validate required fields
        name = record.get("name", "").strip()
        bjcp_code = record.get("number", "").strip()
        
        if not name:
            logger.warning(f"⚠️ Skipping record with missing name: {record}")
            continue
            
        if not bjcp_code:
            logger.warning(f"⚠️ Skipping record with missing BJCP_code (number): {record}")
            continue
        
        # Map source columns to target schema
        style_doc = {
            # Required fields
            "name": name,
            "BJCP_code": bjcp_code,
            
            # Category fields
            "category": record.get("category", None),
            "category_number": record.get("categorynumber", None),
            
            # Descriptive fields
            "overallimpression": record.get("overallimpression", None),
            "aroma": record.get("aroma", None),
            "appearance": record.get("appearance", None),
            "flavor": record.get("flavor", None),
            "mouthfeel": record.get("mouthfeel", None),
            
            # Style guides and ranges
            "style_guides": _parse_style_guides_from_record(record),
            "og_range": _parse_range_from_record(record, "og"),
            "fg_range": _parse_range_from_record(record, "fg"),
            "ibu_range": _parse_range_from_record(record, "ibu"),
            "srm_range": _parse_range_from_record(record, "srm")
        }
        
        # Remove None values to match schema
        style_doc = {k: v for k, v in style_doc.items() if v is not None}
        transformed.append(style_doc)
    
    logger.info(f"✅ Transformed {len(transformed)} beer styles records")
    return transformed

def transform_hops_data(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform hops data to match catalog_hops schema.
    """
    transformed = []
    
    for record in records:
        hop_doc = {
            "name": record.get("name", ""),
            "origin": record.get("origin", None),
            "alpha_acid_pct_range": _parse_alpha_acid_range(record),
            "oil_total_ml_per_100g": _parse_numeric(record.get("total_oil", None)),
            "aroma_descriptors": _parse_descriptors(record.get("descriptors", None)),
            "forms": _parse_forms(record),
            "vendor": None,  # Not available in source data
            "notes": record.get("substitutes", None)
        }
        
        # Remove None values
        hop_doc = {k: v for k, v in hop_doc.items() if v is not None}
        transformed.append(hop_doc)
    
    logger.info(f"✅ Transformed {len(transformed)} hops records")
    return transformed

def transform_fermentables_data(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform fermentables data to match catalog_fermentables schema.
    """
    transformed = []
    
    for record in records:
        fermentable_doc = {
            "name": record.get("name", ""),
            "type": _map_fermentable_type(record.get("type", None)),
            "origin": record.get("origin", None),
            "potential_sg": _parse_potential_sg(record.get("potential_yield", None)),
            "color_srm": _parse_srm(record.get("srm", None)),
            "diastatic_power_Lintner": _parse_diastatic_power(record.get("diastatic_power", None)),
            "notes": record.get("max_usage", None)
        }
        
        # Remove None values
        fermentable_doc = {k: v for k, v in fermentable_doc.items() if v is not None}
        transformed.append(fermentable_doc)
    
    logger.info(f"✅ Transformed {len(transformed)} fermentables records")
    return transformed

def transform_yeasts_data(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform yeasts data to match catalog_yeasts schema.
    """
    transformed = []
    
    for record in records:
        yeast_doc = {
            "name": record.get("name", ""),
            "type": _map_yeast_type(record.get("type", None)),
            "form": _map_yeast_form(record.get("type", None)),  # Infer form from type if needed
            "attenuation_pct_default": _parse_attenuation(record.get("attenuation", None)),
            "min_temp_C": None,  # Not available in source data
            "max_temp_C": None,  # Not available in source data
            "notes": _combine_yeast_notes(record)
        }
        
        # Remove None values
        yeast_doc = {k: v for k, v in yeast_doc.items() if v is not None}
        transformed.append(yeast_doc)
    
    logger.info(f"✅ Transformed {len(transformed)} yeasts records")
    return transformed

def load_catalog_to_mongodb(data: List[Dict[str, Any]], 
                           collection_name: str,
                           database_name: str = "brewlytix",
                           connection_id: str = "brewlytix-atlas-scram") -> Dict[str, int]:
    """
    Load catalog data to MongoDB collection.
    
    Args:
        data: List of documents to insert
        collection_name: Target MongoDB collection name
        database_name: MongoDB database name
        connection_id: Airflow connection ID for MongoDB
    
    Returns:
        Dictionary with operation results
    """
    if not data:
        logger.warning("❌ No data to load to MongoDB")
        return {"inserted": 0, "errors": 0}
    
    logger.info(f"🔄 Loading {len(data)} records to MongoDB collection '{collection_name}'...")
    
    try:
        # Get MongoDB connection with detailed error handling
        try:
            mongo_connection = BaseHook.get_connection(connection_id)
            logger.info(f"✅ Retrieved MongoDB connection: {connection_id}")
        except Exception as e:
            logger.error(f"❌ Failed to retrieve MongoDB connection '{connection_id}': {str(e)}")
            logger.error("💡 Make sure the MongoDB connection is configured in Airflow Admin > Connections")
            raise Exception(f"MongoDB connection '{connection_id}' not found. Please configure it in Airflow.")
        
        # Build connection URI with error handling
        try:
            connection_uri = _build_mongo_uri(mongo_connection)
            logger.info(f"✅ Built MongoDB connection URI")
        except Exception as e:
            logger.error(f"❌ Failed to build MongoDB URI: {str(e)}")
            logger.error("💡 Check your MongoDB connection configuration (host, username, password, etc.)")
            raise Exception(f"Failed to build MongoDB connection URI: {str(e)}")
        
        # Test connection with detailed error handling
        try:
            test_client = MongoClient(connection_uri, serverSelectionTimeoutMS=5000)
            test_client.admin.command('ping')
            test_client.close()
            logger.info("✅ MongoDB connection test successful")
        except Exception as e:
            logger.error(f"❌ MongoDB connection test failed: {str(e)}")
            logger.error("💡 Check your MongoDB Atlas connection string and network access")
            logger.error("💡 Ensure your IP is whitelisted in MongoDB Atlas")
            raise Exception(f"MongoDB connection test failed: {str(e)}")
        
        # Connect to MongoDB
        client = MongoClient(connection_uri)
        db = client[database_name]
        collection = db[collection_name]
        
        # Add metadata to each document
        documents_with_metadata = []
        for doc in data:
            doc['_migration_metadata'] = {
                'migrated_at': datetime.now(timezone.utc),
                'source': 'motherduck',
                'migration_version': '1.0'
            }
            documents_with_metadata.append(doc)
        
        # Check for existing documents to prevent duplicates
        existing_names = set(collection.distinct("name"))
        logger.info(f"📊 Found {len(existing_names)} existing documents in collection")
        
        # Insert documents using upsert strategy with duplicate prevention
        inserted_count = 0
        modified_count = 0
        skipped_count = 0
        errors = 0
        
        for doc in documents_with_metadata:
            try:
                doc_name = doc.get("name", "unknown")
                
                # Check if document already exists
                if doc_name in existing_names:
                    logger.debug(f"⚪ Skipping duplicate: {doc_name}")
                    skipped_count += 1
                    continue
                
                # Use name as unique identifier for upsert
                filter_query = {"name": doc_name}
                
                # Remove metadata from the document for the upsert
                doc_to_upsert = {k: v for k, v in doc.items() if k != "_migration_metadata"}
                
                result = collection.replace_one(filter_query, doc_to_upsert, upsert=True)
                
                if result.upserted_id:
                    inserted_count += 1
                    existing_names.add(doc_name)  # Add to existing set
                    logger.debug(f"✅ Inserted: {doc_name}")
                elif result.modified_count > 0:
                    modified_count += 1
                    logger.debug(f"🔄 Updated: {doc_name}")
                else:
                    logger.debug(f"⚪ No changes: {doc_name}")
                    
            except Exception as e:
                errors += 1
                logger.error(f"❌ Error upserting {doc.get('name', 'unknown')}: {str(e)}")
        
        # Create indexes for better performance
        try:
            collection.create_index("name", unique=True)
            logger.info("✅ Created unique index on 'name' field")
        except Exception as e:
            logger.warning(f"⚠️ Could not create index: {e}")
        
        client.close()
        
        result = {
            "inserted": inserted_count,
            "modified": modified_count,
            "skipped": skipped_count,
            "errors": errors,
            "total_processed": len(data)
        }
        
        logger.info(f"✅ MongoDB load completed: {result}")
        logger.info(f"📊 Summary: {inserted_count} inserted, {modified_count} modified, {skipped_count} skipped (duplicates), {errors} errors")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error loading to MongoDB: {str(e)}")
        raise

# Helper functions for data transformation

def _parse_style_guides_from_record(record: Dict[str, Any]) -> Optional[List[str]]:
    """Parse style guides from record fields."""
    # Try different possible fields for style guides
    style_guides_fields = ["style_guides", "currentlydefinedtypes", "tags"]
    
    for field in style_guides_fields:
        value = record.get(field)
        if value:
            if isinstance(value, str):
                return [guide.strip() for guide in value.split(',') if guide.strip()]
            elif isinstance(value, list):
                return [str(item).strip() for item in value if str(item).strip()]
    
    return None

def _parse_range_from_record(record: Dict[str, Any], prefix: str) -> Optional[Dict[str, float]]:
    """Parse min/max range from record using prefix."""
    min_key = f"{prefix}min"
    max_key = f"{prefix}max"
    
    min_val = _parse_numeric(record.get(min_key))
    max_val = _parse_numeric(record.get(max_key))
    
    if min_val is not None or max_val is not None:
        range_dict = {}
        if min_val is not None:
            range_dict["min"] = min_val
        if max_val is not None:
            range_dict["max"] = max_val
        return range_dict
    return None

def _parse_alpha_acid_range(record: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """Parse alpha acid percentage range from hops record."""
    alpha_acid = record.get("alpha_acid")
    if not alpha_acid:
        return None
    
    # Extract numeric value from string like "5.0-7.0%" or "6.5%"
    import re
    alpha_str = str(alpha_acid).replace("%", "").strip()
    
    # Try to parse range
    range_match = re.search(r'(\d+\.?\d*)\s*-\s*(\d+\.?\d*)', alpha_str)
    if range_match:
        return {
            "min": float(range_match.group(1)),
            "max": float(range_match.group(2))
        }
    
    # Try to parse single value
    single_match = re.search(r'(\d+\.?\d*)', alpha_str)
    if single_match:
        val = float(single_match.group(1))
        return {"min": val, "max": val}
    
    return None

def _parse_numeric(value: Any) -> Optional[float]:
    """Parse numeric value from various formats."""
    if value is None:
        return None
    
    try:
        if isinstance(value, (int, float)):
            return float(value)
        
        # Handle string values
        if isinstance(value, str):
            # Remove common suffixes and clean
            cleaned = value.replace('%', '').replace('°', '').strip()
            return float(cleaned)
        
        return None
    except (ValueError, TypeError):
        return None

def _parse_descriptors(descriptors_str: Optional[str]) -> Optional[List[str]]:
    """Parse descriptors from comma-separated string."""
    if not descriptors_str:
        return None
    return [desc.strip() for desc in descriptors_str.split(',') if desc.strip()]

def _parse_forms(record: Dict[str, Any]) -> Optional[List[str]]:
    """Parse hop forms from record."""
    # For now, assume all hops are available as pellets
    # This could be enhanced based on actual data
    return ["Pellet"]

def _map_fermentable_type(fermentable_type: Optional[str]) -> Optional[str]:
    """Map fermentable type to schema enum values."""
    if not fermentable_type:
        return None
    
    type_mapping = {
        "grain": "Grain",
        "sugar": "Sugar",
        "extract": "Extract",
        "dry extract": "Dry Extract",
        "adjunct": "Adjunct"
    }
    
    return type_mapping.get(fermentable_type.lower(), fermentable_type)

def _parse_potential_sg(potential_yield: Optional[str]) -> Optional[float]:
    """Parse potential specific gravity from yield string."""
    if not potential_yield:
        return None
    
    try:
        # Extract numeric value from string like "38 PPG" or "1.038"
        import re
        numeric_match = re.search(r'(\d+\.?\d*)', str(potential_yield))
        if numeric_match:
            val = float(numeric_match.group(1))
            # Convert PPG to SG if needed (rough conversion)
            if val > 10:  # Likely PPG
                return 1.0 + (val / 1000)
            return val
        return None
    except (ValueError, TypeError):
        return None

def _parse_srm(srm_str: Optional[str]) -> Optional[float]:
    """Parse SRM color value."""
    return _parse_numeric(srm_str)

def _parse_diastatic_power(diastatic_power: Optional[str]) -> Optional[float]:
    """Parse diastatic power in Lintner units."""
    return _parse_numeric(diastatic_power)

def _map_yeast_type(yeast_type: Optional[str]) -> Optional[str]:
    """Map yeast type to schema enum values."""
    if not yeast_type:
        return None
    
    type_mapping = {
        "ale": "Ale",
        "lager": "Lager",
        "wheat": "Wheat",
        "wine": "Wine",
        "champagne": "Champagne"
    }
    
    return type_mapping.get(yeast_type.lower(), yeast_type)

def _map_yeast_form(yeast_type: Optional[str]) -> Optional[str]:
    """Map yeast form based on type or other criteria."""
    # Default to Liquid form, could be enhanced with actual data
    return "Liquid"

def _parse_attenuation(attenuation: Optional[str]) -> Optional[int]:
    """Parse attenuation percentage."""
    if not attenuation:
        return None
    
    try:
        # Extract numeric value and convert to int
        val = _parse_numeric(attenuation)
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None

def _combine_yeast_notes(record: Dict[str, Any]) -> Optional[str]:
    """Combine various yeast notes into a single notes field."""
    notes_parts = []
    
    if record.get("flocculation"):
        notes_parts.append(f"Flocculation: {record['flocculation']}")
    
    if record.get("alcohol_tolerance"):
        notes_parts.append(f"Alcohol Tolerance: {record['alcohol_tolerance']}")
    
    return "; ".join(notes_parts) if notes_parts else None
