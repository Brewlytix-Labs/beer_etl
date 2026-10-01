import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import hashlib
import json
import pytz
from bson import ObjectId


def get_timezone_aware_datetime(timezone_name: str = "UTC") -> datetime:
    """
    Get a timezone-aware datetime object for MongoDB.
    
    Args:
        timezone_name: Timezone name (default: UTC)
    
    Returns:
        Timezone-aware datetime object
    """
    try:
        tz = pytz.timezone(timezone_name)
        return datetime.now(tz)
    except Exception:
        # Fallback to UTC if timezone is invalid
        return datetime.now(timezone.utc)


def generate_recipe_hash(recipe_data: Dict[str, Any]) -> str:
    """
    Generate a hash for duplicate detection based on recipe content.
    
    Args:
        recipe_data: Recipe dictionary
    
    Returns:
        SHA256 hash of recipe content
    """
    # Helper function to safely convert values to strings
    def safe_str(value):
        if value is None:
            return ''
        return str(value)
    
    # Create a stable hash based on key recipe fields
    content_fields = [
        safe_str(recipe_data.get('name', '')),
        safe_str(recipe_data.get('brewer', '')),
        safe_str(recipe_data.get('type', '')),
        safe_str(recipe_data.get('version', 1)),
        safe_str(recipe_data.get('batch', {}).get('target_volume_L', '')),
        safe_str(recipe_data.get('batch', {}).get('boil_time_min', '')),
    ]
    
    # Add fermentables, hops, and yeasts to hash
    for fermentable in recipe_data.get('fermentables', []):
        if isinstance(fermentable, dict):
            name = safe_str(fermentable.get('name', ''))
            amount = safe_str(fermentable.get('amount_g', ''))
            content_fields.append(f"{name}_{amount}")
    
    for hop in recipe_data.get('hops', []):
        if isinstance(hop, dict):
            name = safe_str(hop.get('name', ''))
            amount = safe_str(hop.get('amount_g', ''))
            use = safe_str(hop.get('use', ''))
            content_fields.append(f"{name}_{amount}_{use}")
    
    for yeast in recipe_data.get('yeasts', []):
        if isinstance(yeast, dict):
            name = safe_str(yeast.get('name', ''))
            y_type = safe_str(yeast.get('type', ''))
            content_fields.append(f"{name}_{y_type}")
    
    # Filter out empty strings and join
    content_fields = [field for field in content_fields if field.strip()]
    content_string = "|".join(content_fields)
    return hashlib.sha256(content_string.encode()).hexdigest()


def flatten_beerxml_to_json(xml_content: str, file_key: str, org_id: str = "brewlytix", timezone_name: str = "UTC") -> List[Dict[str, Any]]:
    """
    Flatten BeerXML content to JSON format matching the MongoDB schema.
    
    Args:
        xml_content: Raw XML content as string
        file_key: S3 object key for reference
        org_id: Organization ID for the recipes
    
    Returns:
        List of flattened recipe dictionaries matching MongoDB schema
    """
    recipes = []
    
    try:
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # Handle BeerXML namespace
        namespace = {'beerxml': 'http://www.beerxml.com/beerxml_0.9'}
        
        # Find all recipe elements
        recipe_elements = root.findall('.//beerxml:RECIPE', namespace) or root.findall('.//RECIPE')
        
        for recipe_elem in recipe_elements:
            recipe_data = flatten_recipe_element(recipe_elem, file_key, org_id, namespace, timezone_name)
            if recipe_data:
                recipes.append(recipe_data)
                
    except ET.ParseError as e:
        print(f"XML parsing error for {file_key}: {str(e)}")
    except Exception as e:
        print(f"Error parsing BeerXML content for {file_key}: {str(e)}")
    
    return recipes


def flatten_recipe_element(recipe_elem, file_key: str, org_id: str, namespace: Dict, timezone_name: str = "UTC") -> Optional[Dict[str, Any]]:
    """
    Flatten a single BeerXML RECIPE element to match MongoDB schema.
    
    Args:
        recipe_elem: XML element containing recipe data
        file_key: S3 object key for reference
        org_id: Organization ID
        namespace: XML namespace mapping
    
    Returns:
        Flattened recipe dictionary matching MongoDB schema
    """
    try:
        # Helper function to safely get text content
        def get_text(elem, xpath, default=None):
            try:
                if namespace:
                    found = elem.find(xpath, namespace)
                else:
                    found = elem.find(xpath)
                return found.text if found is not None and found.text else default
            except:
                return default
        
        # Helper function to get numeric value
        def get_numeric(elem, xpath, default=None):
            try:
                text = get_text(elem, xpath, default)
                if text and text != default:
                    return float(text)
                return default
            except:
                return default
        
        # Helper function to get integer value
        def get_int(elem, xpath, default=None):
            try:
                text = get_text(elem, xpath, default)
                if text and text != default:
                    return int(float(text))
                return default
            except:
                return default
        
        # Extract basic recipe information
        recipe_name = get_text(recipe_elem, 'beerxml:NAME') or get_text(recipe_elem, 'NAME', '')
        if not recipe_name:
            return None
            
        recipe_type = get_text(recipe_elem, 'beerxml:TYPE') or get_text(recipe_elem, 'TYPE', 'All Grain')
        brewer = get_text(recipe_elem, 'beerxml:BREWER') or get_text(recipe_elem, 'BREWER', '')
        version = get_int(recipe_elem, 'beerxml:VERSION') or get_int(recipe_elem, 'VERSION', 1)
        
        # Map recipe type to schema enum
        type_mapping = {
            'All Grain': 'All Grain',
            'Extract': 'Extract', 
            'Partial Mash': 'Partial Mash',
            'Mini-Mash': 'Partial Mash',
            'Partial': 'Partial Mash'
        }
        recipe_type = type_mapping.get(recipe_type, 'All Grain')
        
        # Extract batch information
        batch_size = get_numeric(recipe_elem, 'beerxml:BATCH_SIZE') or get_numeric(recipe_elem, 'BATCH_SIZE', 0)
        boil_size = get_numeric(recipe_elem, 'beerxml:BOIL_SIZE') or get_numeric(recipe_elem, 'BOIL_SIZE')
        boil_time = get_int(recipe_elem, 'beerxml:BOIL_TIME') or get_int(recipe_elem, 'BOIL_TIME', 60)
        efficiency = get_numeric(recipe_elem, 'beerxml:EFFICIENCY') or get_numeric(recipe_elem, 'EFFICIENCY', 75)
        
        # Cap efficiency at 100% to match schema constraint
        if efficiency and efficiency > 100:
            efficiency = 100
        
        # BeerXML 1.0 gives BATCH_SIZE and BOIL_SIZE in litres, so they are stored as they are (FQ-1440). The import
        # used to take any batch over 10 as gallons and multiply it by 3.78541, so a 20.82 L batch became 78.81 L.
        target_volume_L = batch_size
        boil_volume_L = boil_size
        
        # Extract style information
        style_data = extract_style_data(recipe_elem, namespace)
        
        # Extract ingredients
        fermentables = extract_fermentables(recipe_elem, namespace)
        hops = extract_hops(recipe_elem, namespace)
        yeasts = extract_yeasts(recipe_elem, namespace)
        miscs = extract_miscs(recipe_elem, namespace)
        
        # Extract additional sections
        water_profile = extract_water_profile(recipe_elem, namespace)
        mash = extract_mash_profile(recipe_elem, namespace)
        equipment = extract_equipment(recipe_elem, namespace)
        estimates = extract_estimates(recipe_elem, namespace)
        instructions = extract_instructions(recipe_elem, namespace)
        
        # Create provenance data with timezone-aware datetime
        provenance = {
            "beerxml_version": 1,
            "source_uri": f"s3://beer-etl/{file_key}",
            "source_record_id": hashlib.sha256(f"{file_key}_{recipe_name}".encode()).hexdigest()[:24],
            "imported_at": get_timezone_aware_datetime(timezone_name),
            "original_xml_fragment": ET.tostring(recipe_elem, encoding='unicode')[:1000],  # Truncated for storage
            "checksum_sha256": hashlib.sha256(ET.tostring(recipe_elem, encoding='unicode').encode()).hexdigest()
        }
        
        # Create audit data with timezone-aware datetime
        audit = {
            "created_by": "beerxml_etl",
            "created_at": get_timezone_aware_datetime(timezone_name),
            "updated_at": None
        }
        
        # Build the complete recipe document
        recipe_doc = {
            "org_id": org_id,
            "name": recipe_name,
            "version": version,
            "type": recipe_type,
            "brewer": brewer if brewer else None,
            "style": style_data,
            "batch": {
                "target_volume_L": target_volume_L,
                "boil_volume_L": boil_volume_L,
                "boil_time_min": boil_time,
                "efficiency_pct": int(efficiency)
            },
            "fermentables": fermentables,
            "hops": hops,
            "yeasts": yeasts,
            "miscs": miscs,
            "water_profile": water_profile,
            "mash": mash,
            "equipment": equipment,
            "estimates": estimates,
            "instructions": instructions,
            "labels": [],  # Empty for now
            "provenance": provenance,
            "ai": None,  # Empty for now
            "audit": audit
        }
        
        # Generate content hash for duplicate detection
        try:
            recipe_doc["content_hash"] = generate_recipe_hash(recipe_doc)
        except Exception as e:
            print(f"⚠️ Warning: Could not generate content hash for recipe {recipe_name}: {str(e)}")
            recipe_doc["content_hash"] = None
        
        return recipe_doc
        
    except Exception as e:
        print(f"Error flattening recipe element: {str(e)}")
        return None


def extract_style_data(recipe_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract style information from recipe element."""
    style_elem = recipe_elem.find('beerxml:STYLE', namespace) or recipe_elem.find('STYLE')
    if style_elem is None:
        return None
    
    def get_text(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    def get_numeric(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return float(text)
            return default
        except:
            return default
    
    style_name = get_text(style_elem, 'beerxml:NAME') or get_text(style_elem, 'NAME', '')
    style_category = get_text(style_elem, 'beerxml:CATEGORY') or get_text(style_elem, 'CATEGORY', '')
    
    if not style_name:
        return None
    
    return {
        "_ref": {
            "catalog_styles_id": None
        },
        "name": style_name,
        "category": style_category,
        "style_guides": [],
        "og_range": {
            "min": get_numeric(style_elem, 'beerxml:OG_MIN') or get_numeric(style_elem, 'OG_MIN'),
            "max": get_numeric(style_elem, 'beerxml:OG_MAX') or get_numeric(style_elem, 'OG_MAX')
        },
        "fg_range": {
            "min": get_numeric(style_elem, 'beerxml:FG_MIN') or get_numeric(style_elem, 'FG_MIN'),
            "max": get_numeric(style_elem, 'beerxml:FG_MAX') or get_numeric(style_elem, 'FG_MAX')
        },
        "ibu_range": {
            "min": get_numeric(style_elem, 'beerxml:IBU_MIN') or get_numeric(style_elem, 'IBU_MIN'),
            "max": get_numeric(style_elem, 'beerxml:IBU_MAX') or get_numeric(style_elem, 'IBU_MAX')
        },
        "srm_range": {
            "min": get_numeric(style_elem, 'beerxml:COLOR_MIN') or get_numeric(style_elem, 'COLOR_MIN'),
            "max": get_numeric(style_elem, 'beerxml:COLOR_MAX') or get_numeric(style_elem, 'COLOR_MAX')
        }
    }


def extract_fermentables(recipe_elem, namespace: Dict) -> List[Dict[str, Any]]:
    """Extract fermentables from recipe element."""
    fermentables = []
    
    fermentable_elements = recipe_elem.findall('beerxml:FERMENTABLES/beerxml:FERMENTABLE', namespace) or \
                          recipe_elem.findall('FERMENTABLES/FERMENTABLE', [])
    
    for fermentable_elem in fermentable_elements:
        fermentable_data = extract_fermentable_data(fermentable_elem, namespace)
        if fermentable_data:
            fermentables.append(fermentable_data)
    
    return fermentables


def extract_fermentable_data(fermentable_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract data from a single fermentable element."""
    def get_text(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    def get_numeric(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return float(text)
            return default
        except:
            return default
    
    name = get_text(fermentable_elem, 'beerxml:NAME') or get_text(fermentable_elem, 'NAME', '')
    if not name:
        return None
    
    amount = get_numeric(fermentable_elem, 'beerxml:AMOUNT') or get_numeric(fermentable_elem, 'AMOUNT', 0)
    # BeerXML 1.0 gives a fermentable's AMOUNT in kilograms (FQ-1440). The import used to take any amount over 0.1 as
    # pounds (2.7216 kg of extract became 1,234.48 g) and store any smaller one as kilograms under a grams label.
    amount_g = amount * 1000.0
    
    fermentable_type = get_text(fermentable_elem, 'beerxml:TYPE') or get_text(fermentable_elem, 'TYPE', 'Grain')
    type_mapping = {
        'Grain': 'Grain',
        'Sugar': 'Sugar', 
        'Extract': 'Extract',
        'Dry Extract': 'Dry Extract',
        'Adjunct': 'Adjunct'
    }
    fermentable_type = type_mapping.get(fermentable_type, 'Grain')
    
    # Get yield potential and ensure it meets schema requirements
    yield_potential = get_numeric(fermentable_elem, 'beerxml:YIELD') or get_numeric(fermentable_elem, 'YIELD')
    
    # For fermentables with zero yield (like Rice Hulls), set to minimum schema requirement
    # or filter them out if they don't contribute to gravity
    if yield_potential is not None and yield_potential == 0.0:
        # For adjuncts with zero yield, set to minimum required value to pass validation
        if fermentable_type == 'Adjunct':
            yield_potential = 1.0  # Minimum required by schema
        # For other types, we could filter them out, but let's keep them with minimum value
        elif yield_potential == 0.0:
            yield_potential = 1.0  # Minimum required by schema
    
    return {
        "_ref": {
            "catalog_fermentables_id": None
        },
        "name": name,
        "amount_g": amount_g,
        "yields_potential_sg": yield_potential,
        "color_srm": get_numeric(fermentable_elem, 'beerxml:COLOR') or get_numeric(fermentable_elem, 'COLOR'),
        "type": fermentable_type,
        "late_addition": get_text(fermentable_elem, 'beerxml:ADD_AFTER_BOIL') == 'TRUE' or get_text(fermentable_elem, 'ADD_AFTER_BOIL') == 'TRUE',
        "origin": get_text(fermentable_elem, 'beerxml:ORIGIN') or get_text(fermentable_elem, 'ORIGIN'),
        "diastatic_power_Lintner": get_numeric(fermentable_elem, 'beerxml:DIASTATIC_POWER') or get_numeric(fermentable_elem, 'DIASTATIC_POWER'),
        "notes": get_text(fermentable_elem, 'beerxml:NOTES') or get_text(fermentable_elem, 'NOTES'),
        "original": {
            "amount_kg": amount
        }
    }


def extract_hops(recipe_elem, namespace: Dict) -> List[Dict[str, Any]]:
    """Extract hops from recipe element."""
    hops = []
    
    hop_elements = recipe_elem.findall('beerxml:HOPS/beerxml:HOP', namespace) or \
                  recipe_elem.findall('HOPS/HOP', [])
    
    for hop_elem in hop_elements:
        hop_data = extract_hop_data(hop_elem, namespace)
        if hop_data:
            hops.append(hop_data)
    
    return hops


def extract_hop_data(hop_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract data from a single hop element."""
    def get_text(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    def get_numeric(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return float(text)
            return default
        except:
            return default
    
    def get_int(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return int(float(text))
            return default
        except:
            return default
    
    name = get_text(hop_elem, 'beerxml:NAME') or get_text(hop_elem, 'NAME', '')
    if not name:
        return None
    
    amount = get_numeric(hop_elem, 'beerxml:AMOUNT') or get_numeric(hop_elem, 'AMOUNT', 0)
    # BeerXML 1.0 gives a hop's AMOUNT in kilograms (FQ-1440). The import used to take any amount over 0.01 as ounces
    # (1 oz, 0.0283 kg, became 0.80 g) and store any smaller one as kilograms under a grams label (0.01 g).
    amount_g = amount * 1000.0
    
    use = get_text(hop_elem, 'beerxml:USE') or get_text(hop_elem, 'USE', 'Boil')
    use_mapping = {
        'Boil': 'Boil',
        'Dry Hop': 'Dry Hop',
        'Mash': 'Mash',
        'First Wort': 'First Wort',
        'Aroma': 'Aroma',
        'Whirlpool': 'Whirlpool'
    }
    use = use_mapping.get(use, 'Boil')
    
    form = get_text(hop_elem, 'beerxml:FORM') or get_text(hop_elem, 'FORM', 'Pellet')
    form_mapping = {
        'Pellet': 'Pellet',
        'Leaf': 'Leaf',
        'Plug': 'Plug'
    }
    form = form_mapping.get(form, 'Pellet')
    
    time_min = get_numeric(hop_elem, 'beerxml:TIME') or get_numeric(hop_elem, 'TIME', 0)
    
    # Get alpha acid percentage and cap at 40% to match schema constraint
    alpha_acid = get_numeric(hop_elem, 'beerxml:ALPHA') or get_numeric(hop_elem, 'ALPHA')
    if alpha_acid and alpha_acid > 40:
        alpha_acid = 40.0  # Cap at maximum allowed by schema
    
    return {
        "_ref": {
            "catalog_hops_id": None
        },
        "name": name,
        "alpha_acid_pct": alpha_acid,
        "form": form,
        "use": use,
        "time_min": time_min,
        "amount_g": amount_g,
        "origin": get_text(hop_elem, 'beerxml:ORIGIN') or get_text(hop_elem, 'ORIGIN'),
        "notes": get_text(hop_elem, 'beerxml:NOTES') or get_text(hop_elem, 'NOTES')
    }


def is_true(value) -> bool:
    """BeerXML's booleans are the words TRUE and FALSE, in any case."""
    return value is not None and str(value).strip().upper() == 'TRUE'


def yeast_type_of(name: str, raw_type) -> str:
    """A yeast's type, as one of the five the recipe schema allows (FQ-1440).

    BeerXML's TYPE is read in any case. When it is missing or none of the five, the yeast's own name is read before
    the Ale fallback: the import used to write Ale for every such yeast, so a Bavarian Lager read (Ale).
    """
    canonical = {'ale': 'Ale', 'lager': 'Lager', 'wheat': 'Wheat', 'wine': 'Wine', 'champagne': 'Champagne'}
    found = canonical.get(str(raw_type or '').strip().lower())
    if found:
        return found
    lowered = (name or '').lower()
    for word, yeast_type in (('lager', 'Lager'), ('pils', 'Lager'), ('wheat', 'Wheat'), ('weizen', 'Wheat'),
                             ('hefe', 'Wheat'), ('champagne', 'Champagne'), ('wine', 'Wine')):
        if word in lowered:
            return yeast_type
    return 'Ale'


def extract_yeasts(recipe_elem, namespace: Dict) -> List[Dict[str, Any]]:
    """Extract yeasts from recipe element."""
    yeasts = []
    
    yeast_elements = recipe_elem.findall('beerxml:YEASTS/beerxml:YEAST', namespace) or \
                    recipe_elem.findall('YEASTS/YEAST', [])
    
    for yeast_elem in yeast_elements:
        yeast_data = extract_yeast_data(yeast_elem, namespace)
        if yeast_data:
            yeasts.append(yeast_data)
    
    return yeasts


def extract_yeast_data(yeast_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract data from a single yeast element."""
    def get_text(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    def get_numeric(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return float(text)
            return default
        except:
            return default
    
    def get_int(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return int(float(text))
            return default
        except:
            return default
    
    name = get_text(yeast_elem, 'beerxml:NAME') or get_text(yeast_elem, 'NAME', '')
    if not name:
        return None
    
    yeast_type = yeast_type_of(name, get_text(yeast_elem, 'beerxml:TYPE') or get_text(yeast_elem, 'TYPE'))
    
    form = get_text(yeast_elem, 'beerxml:FORM') or get_text(yeast_elem, 'FORM', 'Liquid')
    form_mapping = {
        'Liquid': 'Liquid',
        'Dry': 'Dry',
        'Slant': 'Slant',
        'Culture': 'Culture'
    }
    form = form_mapping.get(form, 'Liquid')
    
    yeast_amount = get_numeric(yeast_elem, 'beerxml:AMOUNT') or get_numeric(yeast_elem, 'AMOUNT')
    yeast_by_weight = is_true(get_text(yeast_elem, 'beerxml:AMOUNT_IS_WEIGHT') or get_text(yeast_elem, 'AMOUNT_IS_WEIGHT'))

    # Get attenuation percentage and cap at 100% to match schema constraint
    attenuation = get_int(yeast_elem, 'beerxml:ATTENUATION') or get_int(yeast_elem, 'ATTENUATION')
    if attenuation and attenuation > 100:
        attenuation = 100  # Cap at maximum allowed by schema
    
    return {
        "_ref": {
            "catalog_yeasts_id": None
        },
        "name": name,
        "type": yeast_type,
        "form": form,
        "attenuation_pct": attenuation,
        "min_temp_C": get_numeric(yeast_elem, 'beerxml:MIN_TEMPERATURE') or get_numeric(yeast_elem, 'MIN_TEMPERATURE'),
        "max_temp_C": get_numeric(yeast_elem, 'beerxml:MAX_TEMPERATURE') or get_numeric(yeast_elem, 'MAX_TEMPERATURE'),
        # BeerXML 1.0 gives a yeast's AMOUNT in litres, or in kilograms when AMOUNT_IS_WEIGHT is TRUE: a volume or a
        # weight, never a cell count (FQ-1440). It is kept in its own unit, and the cell count stays unknown.
        "amount_cells_billion": None,
        "amount_l": None if yeast_by_weight else yeast_amount,
        "amount_kg": yeast_amount if yeast_by_weight else None,
        "notes": get_text(yeast_elem, 'beerxml:NOTES') or get_text(yeast_elem, 'NOTES')
    }


def extract_miscs(recipe_elem, namespace: Dict) -> List[Dict[str, Any]]:
    """Extract miscellaneous ingredients from recipe element."""
    miscs = []
    
    misc_elements = recipe_elem.findall('beerxml:MISCS/beerxml:MISC', namespace) or \
                   recipe_elem.findall('MISCS/MISC', [])
    
    for misc_elem in misc_elements:
        misc_data = extract_misc_data(misc_elem, namespace)
        if misc_data:
            miscs.append(misc_data)
    
    return miscs


def extract_misc_data(misc_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract data from a single misc element."""
    def get_text(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    def get_numeric(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return float(text)
            return default
        except:
            return default
    
    def get_int(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return int(float(text))
            return default
        except:
            return default
    
    name = get_text(misc_elem, 'beerxml:NAME') or get_text(misc_elem, 'NAME', '')
    if not name:
        return None
    
    misc_type = get_text(misc_elem, 'beerxml:TYPE') or get_text(misc_elem, 'TYPE', 'Other')
    use = get_text(misc_elem, 'beerxml:USE') or get_text(misc_elem, 'USE', 'Boil')
    use_mapping = {
        'Boil': 'Boil',
        'Mash': 'Mash',
        'Primary': 'Primary',
        'Secondary': 'Secondary',
        'Bottling': 'Bottling'
    }
    use = use_mapping.get(use, 'Boil')
    
    amount = get_numeric(misc_elem, 'beerxml:AMOUNT') or get_numeric(misc_elem, 'AMOUNT', 0)
    # BeerXML 1.0 gives a misc's AMOUNT in kilograms when AMOUNT_IS_WEIGHT is TRUE, and in litres otherwise (FQ-1440).
    # The import used to take any amount over 0.01 as ounces. A volume is kept in millilitres too; its amount_g,
    # which the recipe schema requires, reads one millilitre as one gram.
    misc_by_weight = is_true(get_text(misc_elem, 'beerxml:AMOUNT_IS_WEIGHT') or get_text(misc_elem, 'AMOUNT_IS_WEIGHT'))
    amount_g = amount * 1000.0
    
    time_min = get_numeric(misc_elem, 'beerxml:TIME') or get_numeric(misc_elem, 'TIME', 0)
    
    return {
        "name": name,
        "type": misc_type,
        "use": use,
        "time_min": time_min,
        "amount_g": amount_g,
        "amount_ml": None if misc_by_weight else amount * 1000.0,
        "notes": get_text(misc_elem, 'beerxml:NOTES') or get_text(misc_elem, 'NOTES')
    }


def extract_water_profile(recipe_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract water profile from recipe element."""
    # BeerXML doesn't have a standard water profile section
    # This would need to be implemented based on specific requirements
    return None


def extract_mash_profile(recipe_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract mash profile from recipe element."""
    mash_elem = recipe_elem.find('beerxml:MASH', namespace) or recipe_elem.find('MASH')
    if mash_elem is None:
        return None
    
    def get_text(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    def get_numeric(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return float(text)
            return default
        except:
            return default
    
    # Extract mash steps
    steps = []
    step_elements = mash_elem.findall('beerxml:MASH_STEPS/beerxml:MASH_STEP', namespace) or \
                   mash_elem.findall('MASH_STEPS/MASH_STEP', [])
    
    for i, step_elem in enumerate(step_elements):
        step_name = get_text(step_elem, 'beerxml:NAME') or get_text(step_elem, 'NAME', f'Step {i+1}')
        step_type = get_text(step_elem, 'beerxml:TYPE') or get_text(step_elem, 'TYPE', 'Infusion')
        step_temp = get_numeric(step_elem, 'beerxml:STEP_TEMP') or get_numeric(step_elem, 'STEP_TEMP', 0)
        step_time = get_numeric(step_elem, 'beerxml:STEP_TIME') or get_numeric(step_elem, 'STEP_TIME', 0)
        infuse_amount = get_numeric(step_elem, 'beerxml:INFUSE_AMOUNT') or get_numeric(step_elem, 'INFUSE_AMOUNT')
        
        steps.append({
            "name": step_name,
            "type": step_type,
            "step_temp_C": step_temp,
            "step_time_min": step_time,
            "infuse_amount_L": infuse_amount
        })
    
    return {
        "_ref": {
            "catalog_mash_profiles_id": None
        },
        "name": get_text(mash_elem, 'beerxml:NAME') or get_text(mash_elem, 'NAME'),
        "sparge_temp_C": get_numeric(mash_elem, 'beerxml:SPARGE_TEMP') or get_numeric(mash_elem, 'SPARGE_TEMP'),
        "ph": get_numeric(mash_elem, 'beerxml:PH') or get_numeric(mash_elem, 'PH'),
        "steps": steps
    }


def extract_equipment(recipe_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract equipment from recipe element."""
    equipment_elem = recipe_elem.find('beerxml:EQUIPMENT', namespace) or recipe_elem.find('EQUIPMENT')
    if equipment_elem is None:
        return None
    
    def get_text(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    def get_numeric(elem, xpath, default=None):
        try:
            text = get_text(elem, xpath, default)
            if text and text != default:
                return float(text)
            return default
        except:
            return default
    
    return {
        "_ref": {
            "catalog_equipment_id": None
        },
        "name": get_text(equipment_elem, 'beerxml:NAME') or get_text(equipment_elem, 'NAME'),
        "boil_off_rate_L_per_hr": get_numeric(equipment_elem, 'beerxml:BOIL_OFF_RATE') or get_numeric(equipment_elem, 'BOIL_OFF_RATE'),
        "trub_loss_L": get_numeric(equipment_elem, 'beerxml:TRUB_LOSS') or get_numeric(equipment_elem, 'TRUB_LOSS'),
        "lauter_deadspace_L": get_numeric(equipment_elem, 'beerxml:LAUTER_DEADSPACE') or get_numeric(equipment_elem, 'LAUTER_DEADSPACE')
    }


def extract_estimates(recipe_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract calculated estimates from recipe element."""
    def get_numeric(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    def get_numeric_value(elem, xpath, default=None):
        try:
            text = get_numeric(elem, xpath, default)
            if text and text != default:
                return float(text)
            return default
        except:
            return default
    
    og = get_numeric_value(recipe_elem, 'beerxml:OG') or get_numeric_value(recipe_elem, 'OG')
    fg = get_numeric_value(recipe_elem, 'beerxml:FG') or get_numeric_value(recipe_elem, 'FG')
    ibu = get_numeric_value(recipe_elem, 'beerxml:IBU') or get_numeric_value(recipe_elem, 'IBU')
    color = get_numeric_value(recipe_elem, 'beerxml:EST_COLOR') or get_numeric_value(recipe_elem, 'EST_COLOR')
    abv = get_numeric_value(recipe_elem, 'beerxml:ABV') or get_numeric_value(recipe_elem, 'ABV')
    
    # Calculate ABV if not present
    if not abv and og and fg:
        abv = (og - fg) * 131.25
    
    return {
        "og": og,
        "fg": fg,
        "ibu": {
            "method": "Tinseth",  # Default method
            "value": ibu
        },
        "color": {
            "srm": color,
            "ebc": color * 1.97 if color else None  # Convert SRM to EBC
        },
        "abv_pct": abv
    }


def extract_instructions(recipe_elem, namespace: Dict) -> Optional[Dict[str, Any]]:
    """Extract brewing instructions from recipe element."""
    def get_text(elem, xpath, default=None):
        try:
            if namespace:
                found = elem.find(xpath, namespace)
            else:
                found = elem.find(xpath)
            return found.text if found is not None and found.text else default
        except:
            return default
    
    # BeerXML doesn't have a standard instructions section
    # This would need to be implemented based on specific requirements
    notes = get_text(recipe_elem, 'beerxml:NOTES') or get_text(recipe_elem, 'NOTES', '')
    
    return {
        "mash_notes": None,
        "boil_notes": None,
        "fermentation": None,
        "packaging": None
    }
