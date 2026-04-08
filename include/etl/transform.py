import json
from pathlib import Path
from datetime import datetime
from textblob import TextBlob
import pandas as pd
import re

#####################################################################
## Transform clone recipe list from https://www.brewersfriend.com  ##
#####################################################################
def transform_fermentables_data(df):
    """
    Transforms raw fermentables data into clean schema
    """
    df = df.copy()

    def extract_pct(val):
        if pd.isna(val):
            return None
        val = str(val)
        match = re.search(r'(\d{1,3}(\.\d+)?)%', val)
        return float(match.group(1)) if match else None

    def extract_srm(val):
        if pd.isna(val):
            return None
        val = str(val)
        match = re.search(r'(\d+(\.\d+)?)', val)
        return float(match.group(1)) if match else None

    def extract_ppg(val):
        if pd.isna(val):
            return None
        val = str(val)
        match = re.search(r'(\d{2})\s*ppg', val, re.IGNORECASE)
        return int(match.group(1)) if match else None

    def extract_dpower(val):
        if pd.isna(val):
            return None
        val = str(val)
        match = re.search(r'(\d+(\.\d+)?)\s*[°Â]?\s*lintner', val, re.IGNORECASE)
        return float(match.group(1)) if match else None

    if 'potential_yield' in df.columns:
        df['yield_pct'] = df['potential_yield'].apply(extract_pct)
        df['ppg'] = df['potential_yield'].apply(extract_ppg)
    else:
        df['yield_pct'] = None
        df['ppg'] = None

    if 'max_usage' in df.columns:
        df['max_usage_pct'] = df['max_usage'].apply(extract_pct)
    else:
        df['max_usage_pct'] = None

    if 'srm' in df.columns:
        df['srm'] = df['srm'].apply(extract_srm)
    else:
        df['srm'] = None

    if 'diastatic_power' in df.columns:
        df['diastatic_power'] = df['diastatic_power'].apply(extract_dpower)
    else:
        df['diastatic_power'] = None

    df.replace("Unknown", None, inplace=True)
    df.replace("", None, inplace=True)

    return df

######################################################
## Transform Beer style data from BJCP              ##
######################################################
def transform_styles_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures all expected columns exist, splits numeric ranges,
    normalizes categories, and converts list-fields.
    """
    df = df.copy()

    # 3) Define all of the keys you care about
    all_keys = [
        'name', 'number', 'category', 'categorynumber',
        'overallimpression', 'aroma', 'appearance', 'flavor',
        'mouthfeel', 'comments', 'history', 'characteristicingredients',
        'stylecomparison', 'ibumin', 'ibumax', 'ogmin', 'ogmax',
        'fgmin', 'fgmax', 'abvmin', 'abvmax', 'srmmin', 'srmmax',
        'commercialexamples', 'tags', 'entryinstructions',
        'currentlydefinedtypes', 'strengthclassifications'
    ]

    # 4) Make sure every column exists
    for key in all_keys:
        if key not in df.columns:
            df[key] = pd.NA

    # 5) Split into numeric vs string fields
    numeric_keys = [
        'ibumin', 'ibumax', 'ogmin', 'ogmax',
        'fgmin', 'fgmax', 'abvmin', 'abvmax',
        'srmmin', 'srmmax'
    ]
    string_keys = [k for k in all_keys if k not in numeric_keys]

    # fill missing strings with empty str
    df[string_keys] = df[string_keys].fillna("")

    # for numeric, replace missing with None, then convert to nullable Float64
    for k in numeric_keys:
        df[k] = df[k].where(df[k].notna(), None).astype("Float64")

    # 6) Normalize “Ipa” → “Indian Pale Ale”
    df['category'] = df['category'].replace({'Ipa': 'Indian Pale Ale'})

    # 7) Split comma-separated fields into Python lists
    def split_list(cell: str) -> list[str]:
        return [s.strip() for s in cell.split(',')] if cell else []

    for col in ['commercialexamples', 'tags']:
        df[col] = df[col].apply(split_list)

    return df


######################################################
## Transform hop data from https://beermaverick.com ##
######################################################
def extract_numeric_range_avg(value):
    """
    Extract min, max, avg values from strings like:
    "10-14% 12% avg" or "0.8-3.9 mL 2.4mL avg"
    Returns: tuple of (min, max, avg) as floats or None
    """
    if pd.isna(value) or not isinstance(value, str):
        return None, None, None

    # Remove units and lowercase
    value = value.replace('%', '').replace('mL', '').lower()
    
    # Match patterns
    range_match = re.search(r'(\d+(\.\d+)?)[\s-]+(\d+(\.\d+)?)', value)
    avg_match = re.search(r'(\d+(\.\d+)?)[\s]*avg', value)

    val_min = float(range_match.group(1)) if range_match else None
    val_max = float(range_match.group(3)) if range_match else None
    val_avg = float(avg_match.group(1)) if avg_match else None

    return val_min, val_max, val_avg


def transform_hop_data(df):
    """
    Transforms raw hop data extracted from BeerMaverick into structured form.
    """
    df = df.copy()
    
    # Columns to extract ranges and averages from
    range_cols = [
        'alpha_acid', 'beta_acid', 'cohumulone', 'total_oil',
        'myrcene', 'humulene', 'caryophyllene', 'farnesene', 'others_oil'
    ]

    for col in range_cols:
        df[[f"{col}_min", f"{col}_max", f"{col}_avg"]] = df[col].apply(
            lambda x: pd.Series(extract_numeric_range_avg(x))
        )

    # Drop original raw columns if needed
    # df.drop(columns=range_cols, inplace=True)

    # Normalize unknowns and blanks
    df.replace("Unknown", None, inplace=True)
    df.replace("", None, inplace=True)
    return df


###############################################################
## Transform fermentables data from https://beermaverick.com ##
###############################################################
def transform_fermentables_data(df):
    """
    Transforms raw fermentables data into clean schema
    """
    df = df.copy()

    def extract_pct(val):
        if pd.isna(val):
            return None
        match = re.search(r'(\d{1,3}(\.\d+)?)%', val)
        return float(match.group(1)) if match else None

    def extract_srm(val):
        if pd.isna(val):
            return None
        match = re.search(r'(\d+(\.\d+)?)', val)
        return float(match.group(1)) if match else None

    def extract_ppg(val):
        if pd.isna(val):
            return None
        match = re.search(r'(\d{2})\s*ppg', val, re.IGNORECASE)
        return int(match.group(1)) if match else None

    def extract_dpower(val):
        if pd.isna(val):
            return None
        match = re.search(r'(\d+(\.\d+)?)\s*°?\s*lintner', val, re.IGNORECASE)
        return float(match.group(1)) if match else None

    df['yield_pct'] = df['potential_yield'].apply(extract_pct) if 'potential_yield' in df else None
    df['ppg'] = df['potential_yield'].apply(extract_ppg) if 'potential_yield' in df else None
    df['max_usage_pct'] = df['max_usage'].apply(extract_pct) if 'max_usage' in df else None
    df['srm'] = df['srm'].apply(extract_srm) if 'srm' in df else None
    df['diastatic_power'] = df['diastatic_power'].apply(extract_dpower) if 'diastatic_power' in df else None

    df.replace("Unknown", None, inplace=True)
    df.replace("", None, inplace=True)

    return df


######################################################### 
## Transform yeasts data from https://beermaverick.com ##
######################################################### 
def transform_yeast_data(df):
    """
    Transforms raw yeast data into clean schema
    """
    df = df.copy()
    
    df = df.rename(columns={
        "name": "yeast_name",
        "url": "detail_url",
        "type": "type",
        "attenuation": "attenuation",
        "alcohol_tolerance": "alcohol_tolerance",
        "flocculation": "flocculation"
    })

     # 1. Split attenuation range
    def parse_pct_range(s):
        match = re.search(r"(\d+)-(\d+)", s or "")
        return (int(match.group(1)), int(match.group(2))) if match else (None, None)
    df[["min_attenuation", "max_attenuation"]] = df["attenuation"].apply(
        lambda s: pd.Series(parse_pct_range(s))
    )
    
    # 2. Split tolerance range or map textual
    df["alcohol_tolerance"] = df["alcohol_tolerance"].str.replace("%","", regex=False)
    df[["min_tol", "max_tol"]] = df["alcohol_tolerance"].apply(
        lambda s: pd.Series(parse_pct_range(s)) 
        if "-" in (s or "") else pd.Series([None, None])
    )
    
    # 3. Flocculation scoring
    score_map = {"Low":1, "Medium-Low":2, "Medium":3, "High":4}
    df["flocculation_score"] = df["flocculation"].map(score_map).fillna(0).astype(int)
    
    # 4. Drop originals if desired
    df = df.drop(columns=["attenuation", "alcohol_tolerance"])

    return df


#########################################################
## Transform BeerXML recipe data from S3              ##
#########################################################
def transform_beerxml_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw BeerXML recipe data into clean, structured format.
    
    Args:
        df: DataFrame containing raw BeerXML recipe data
    
    Returns:
        Cleaned and transformed DataFrame
    """
    df = df.copy()
    
    # 1) Clean and standardize string fields
    string_columns = [
        'recipe_name', 'recipe_type', 'brewer', 'style_name', 'style_category',
        'created_date', 'notes', 'file_key'
    ]
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', '').replace('None', '')
    
    # 2) Parse and clean date fields
    if 'created_date' in df.columns:
        df['created_date_parsed'] = df['created_date'].apply(parse_beerxml_date)
        df['created_year'] = df['created_date_parsed'].dt.year
        df['created_month'] = df['created_date_parsed'].dt.month
    
    # 3) Clean numeric fields and handle missing values
    numeric_columns = [
        'batch_size', 'boil_size', 'boil_time', 'efficiency',
        'calculated_og', 'calculated_fg', 'calculated_abv', 
        'calculated_ibu', 'calculated_color',
        'style_og_min', 'style_og_max', 'style_fg_min', 'style_fg_max',
        'style_abv_min', 'style_abv_max', 'style_ibu_min', 'style_ibu_max',
        'style_color_min', 'style_color_max',
        'hops_count', 'fermentables_count', 'yeasts_count'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 4) Create derived fields
    # Calculate ABV from OG and FG if not present
    if 'calculated_abv' in df.columns and 'calculated_og' in df.columns and 'calculated_fg' in df.columns:
        mask = df['calculated_abv'].isna() & df['calculated_og'].notna() & df['calculated_fg'].notna()
        df.loc[mask, 'calculated_abv'] = (
            (df.loc[mask, 'calculated_og'] - df.loc[mask, 'calculated_fg']) * 131.25
        )
    
    # Create recipe complexity score based on ingredient counts
    if all(col in df.columns for col in ['hops_count', 'fermentables_count', 'yeasts_count']):
        df['complexity_score'] = (
            df['hops_count'].fillna(0) + 
            df['fermentables_count'].fillna(0) + 
            df['yeasts_count'].fillna(0)
        )
    
    # 5) Categorize recipe types
    if 'recipe_type' in df.columns:
        df['recipe_type_category'] = df['recipe_type'].apply(categorize_recipe_type)
    
    # 6) Style validation - check if calculated values fall within style guidelines
    style_validation_cols = ['og_in_style_range', 'fg_in_style_range', 'abv_in_style_range', 
                            'ibu_in_style_range', 'color_in_style_range']
    
    for col in style_validation_cols:
        df[col] = False
    
    # OG validation
    if all(col in df.columns for col in ['calculated_og', 'style_og_min', 'style_og_max']):
        df['og_in_style_range'] = (
            (df['calculated_og'] >= df['style_og_min']) & 
            (df['calculated_og'] <= df['style_og_max'])
        ).fillna(False)
    
    # FG validation
    if all(col in df.columns for col in ['calculated_fg', 'style_fg_min', 'style_fg_max']):
        df['fg_in_style_range'] = (
            (df['calculated_fg'] >= df['style_fg_min']) & 
            (df['calculated_fg'] <= df['style_fg_max'])
        ).fillna(False)
    
    # ABV validation
    if all(col in df.columns for col in ['calculated_abv', 'style_abv_min', 'style_abv_max']):
        df['abv_in_style_range'] = (
            (df['calculated_abv'] >= df['style_abv_min']) & 
            (df['calculated_abv'] <= df['style_abv_max'])
        ).fillna(False)
    
    # IBU validation
    if all(col in df.columns for col in ['calculated_ibu', 'style_ibu_min', 'style_ibu_max']):
        df['ibu_in_style_range'] = (
            (df['calculated_ibu'] >= df['style_ibu_min']) & 
            (df['calculated_ibu'] <= df['style_ibu_max'])
        ).fillna(False)
    
    # Color validation
    if all(col in df.columns for col in ['calculated_color', 'style_color_min', 'style_color_max']):
        df['color_in_style_range'] = (
            (df['calculated_color'] >= df['style_color_min']) & 
            (df['calculated_color'] <= df['style_color_max'])
        ).fillna(False)
    
    # Calculate overall style compliance score
    style_cols = ['og_in_style_range', 'fg_in_style_range', 'abv_in_style_range', 
                  'ibu_in_style_range', 'color_in_style_range']
    df['style_compliance_score'] = df[style_cols].sum(axis=1) / len(style_cols)
    
    # 7) Create file metadata
    if 'file_key' in df.columns:
        df['source_file'] = df['file_key'].str.extract(r'/([^/]+\.xml)$')
        df['source_directory'] = df['file_key'].str.extract(r'^([^/]+/[^/]+/)')
    
    # 8) Add processing timestamp
    df['processed_at'] = datetime.now()
    
    # 9) Remove duplicates based on recipe name and brewer
    df = df.drop_duplicates(subset=['recipe_name', 'brewer'], keep='first').reset_index(drop=True)
    
    return df


def parse_beerxml_date(date_str: str) -> pd.Timestamp:
    """
    Parse various date formats found in BeerXML files.
    
    Args:
        date_str: Date string from BeerXML
    
    Returns:
        Parsed timestamp or NaT if parsing fails
    """
    if pd.isna(date_str) or date_str == '' or date_str == 'nan':
        return pd.NaT
    
    # Common BeerXML date formats
    formats = [
        '%Y-%m-%d',           # 2023-01-15
        '%m/%d/%Y',           # 01/15/2023
        '%d/%m/%Y',           # 15/01/2023
        '%Y-%m-%d %H:%M:%S',  # 2023-01-15 14:30:00
        '%m/%d/%Y %H:%M:%S',  # 01/15/2023 14:30:00
        '%Y%m%d',             # 20230115
        '%B %d, %Y',          # January 15, 2023
        '%b %d, %Y',          # Jan 15, 2023
    ]
    
    for fmt in formats:
        try:
            return pd.to_datetime(date_str, format=fmt)
        except:
            continue
    
    # Try pandas automatic parsing as fallback
    try:
        return pd.to_datetime(date_str)
    except:
        return pd.NaT


def categorize_recipe_type(recipe_type: str) -> str:
    """
    Categorize recipe types into broader categories.
    
    Args:
        recipe_type: Original recipe type string
    
    Returns:
        Categorized recipe type
    """
    if pd.isna(recipe_type) or recipe_type == '':
        return 'Unknown'
    
    recipe_type = str(recipe_type).lower()
    
    if 'all grain' in recipe_type:
        return 'All Grain'
    elif 'extract' in recipe_type:
        return 'Extract'
    elif 'partial' in recipe_type or 'mini-mash' in recipe_type:
        return 'Partial Mash'
    elif 'mash' in recipe_type:
        return 'Mash'
    else:
        return 'Other'
