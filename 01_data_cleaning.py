"""
01_data_cleaning.py

Freight Shipping Data Cleaning & Standardization
- Remove duplicates
- Parse currency fields
- Validate zones & weights
- Standardize state codes
- Flag/handle missing values
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_raw_data(filepath):
    """Load raw invoice CSV"""
    logger.info(f"Loading raw data from {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df

def clean_currency(value):
    """Convert $X,XXX.XX to float"""
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float)):
        return float(value)
    # Remove $ and commas
    value = str(value).replace('$', '').replace(',', '')
    try:
        return float(value)
    except ValueError:
        logger.warning(f"Could not parse currency: {value}")
        return np.nan

def clean_zone(value):
    """Standardize zone format (Z1-Z8)"""
    if pd.isna(value):
        return np.nan
    value = str(value).strip().upper()
    # Extract digit if needed
    match = re.search(r'(\d+)', value)
    if match:
        zone = int(match.group(1))
        if 1 <= zone <= 8:
            return zone
    logger.warning(f"Invalid zone: {value}")
    return np.nan

def clean_state_code(value):
    """Standardize state codes to 2-letter format"""
    if pd.isna(value):
        return np.nan
    value = str(value).strip().upper()
    if len(value) == 2:
        return value
    # Could expand to full state name lookup if needed
    logger.warning(f"Non-standard state code: {value}")
    return value

def validate_weight(value):
    """Check weight is in reasonable range"""
    if pd.isna(value):
        return np.nan
    try:
        weight = float(value)
        if 0 < weight < 300:  # Reasonable for ground shipping
            return weight
        logger.warning(f"Weight out of range: {weight} lbs")
        return np.nan
    except:
        return np.nan

def parse_date(value):
    """Parse date field (flexible format)"""
    if pd.isna(value):
        return pd.NaT
    try:
        return pd.to_datetime(value)
    except:
        logger.warning(f"Could not parse date: {value}")
        return pd.NaT

def main():
    # Load data
    df = load_raw_data('data/raw/invoice_raw.csv')
    logger.info(f"Initial shape: {df.shape}")
    
    # --- CLEANING STEPS ---
    
    # 1. Remove complete duplicates
    initial_rows = len(df)
    df = df.drop_duplicates()
    logger.info(f"Removed {initial_rows - len(df)} duplicate rows")
    
    # 2. Clean currency columns
    currency_cols = ['freight_cost', 'fuel_surcharge', 'accessorial_charges', 'total_cost']
    for col in currency_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_currency)
            logger.info(f"Cleaned currency column: {col}")
    
    # 3. Clean zone codes
    if 'zone' in df.columns:
        df['zone'] = df['zone'].apply(clean_zone)
        df_valid = df[df['zone'].notna()]
        logger.info(f"Valid zones: {len(df_valid)}/{len(df)} ({len(df_valid)/len(df)*100:.1f}%)")
    
    # 4. Clean weight
    if 'weight_lbs' in df.columns:
        df['weight_lbs'] = df['weight_lbs'].apply(validate_weight)
        df_valid = df[df['weight_lbs'].notna()]
        logger.info(f"Valid weights: {len(df_valid)}/{len(df)} ({len(df_valid)/len(df)*100:.1f}%)")
    
    # 5. Standardize state codes
    if 'receiver_state' in df.columns:
        df['receiver_state'] = df['receiver_state'].apply(clean_state_code)
    
    # 6. Parse dates
    date_cols = ['invoice_date', 'shipment_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(parse_date)
            logger.info(f"Parsed date column: {col}")
    
    # 7. Remove rows with critical missing values
    critical_cols = ['tracking_number', 'zone', 'weight_lbs', 'total_cost']
    initial_rows = len(df)
    df = df.dropna(subset=[c for c in critical_cols if c in df.columns])
    logger.info(f"Removed {initial_rows - len(df)} rows with missing critical data")
    
    # 8. Add quality flags
    df['data_quality_flag'] = 'VALID'
    if 'weight_lbs' in df.columns:
        df.loc[(df['weight_lbs'] < 0.5) | (df['weight_lbs'] > 100), 'data_quality_flag'] = 'OUTLIER_WEIGHT'
    if 'total_cost' in df.columns:
        median_cost = df['total_cost'].median()
        df.loc[df['total_cost'] > median_cost * 5, 'data_quality_flag'] = 'OUTLIER_COST'
    
    # --- SUMMARY ---
    logger.info(f"\nCleaning Summary:")
    logger.info(f"Final shape: {df.shape}")
    logger.info(f"Data quality flags:\n{df['data_quality_flag'].value_counts()}")
    
    # Save cleaned data
    df.to_csv('data/processed/invoice_cleaned.csv', index=False)
    logger.info(f"✓ Saved cleaned data to data/processed/invoice_cleaned.csv")
    
    return df

if __name__ == '__main__':
    df = main()

