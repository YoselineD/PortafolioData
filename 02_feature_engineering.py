"""
02_feature_engineering.py

Calculated Columns & Business Logic
- Weight tier classification
- Zone cost categories
- Cost per unit metrics
- Service efficiency scores
- Geographic regions
- Time-based aggregations
"""

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_cleaned_data(filepath):
    """Load preprocessed data"""
    df = pd.read_csv(filepath)
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    df['shipment_date'] = pd.to_datetime(df['shipment_date'])
    return df

def create_weight_tier(df):
    """Classify packages by weight"""
    df['weight_tier'] = pd.cut(
        df['weight_lbs'],
        bins=[0, 5, 20, 300],
        labels=['Light (0-5 lbs)', 'Medium (5-20 lbs)', 'Heavy (20+ lbs)'],
        include_lowest=True
    )
    logger.info(f"Weight tiers created:\n{df['weight_tier'].value_counts()}")
    return df

def create_zone_category(df):
    """Classify zones by cost level"""
    # Z1-3 = low cost, Z4-6 = medium, Z7-8 = high
    def categorize_zone(zone):
        if pd.isna(zone):
            return np.nan
        if zone <= 3:
            return 'Low Cost (Z1-3)'
        elif zone <= 6:
            return 'Medium Cost (Z4-6)'
        else:
            return 'High Cost (Z7-8)'
    
    df['zone_category'] = df['zone'].apply(categorize_zone)
    logger.info(f"Zone categories:\n{df['zone_category'].value_counts()}")
    return df

def create_cost_metrics(df):
    """Calculate cost per unit metrics"""
    df['cost_per_lb'] = df['total_cost'] / df['weight_lbs']
    df['freight_per_lb'] = df['freight_cost'] / df['weight_lbs']
    df['accessorial_ratio'] = df['accessorial_charges'] / df['total_cost']
    df['fuel_ratio'] = df['fuel_surcharge'] / df['total_cost']
    
    logger.info(f"Cost per lb range: ${df['cost_per_lb'].min():.2f} - ${df['cost_per_lb'].max():.2f}")
    logger.info(f"Avg accessorial ratio: {df['accessorial_ratio'].mean()*100:.1f}%")
    return df

def create_service_flag(df):
    """Flag for high-cost services"""
    if 'service_type' in df.columns:
        air_services = df['service_type'].str.contains('Air|Express', case=False, na=False)
        df['is_air_service'] = air_services.astype(int)
        df['is_ground_service'] = (~air_services).astype(int)
        logger.info(f"Air services: {df['is_air_service'].sum()} ({df['is_air_service'].sum()/len(df)*100:.1f}%)")
    return df

def create_region(df):
    """Map states to regions"""
    regions = {
        'Northeast': ['MA', 'NH', 'VT', 'ME', 'CT', 'RI', 'NY', 'NJ', 'PA'],
        'Southeast': ['VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'AL', 'MS', 'LA', 'AR', 'TN', 'KY'],
        'Midwest': ['OH', 'IN', 'IL', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS'],
        'South': ['TX', 'OK'],
        'West': ['CA', 'NV', 'AZ', 'UT', 'CO', 'NM', 'WY', 'MT', 'ID', 'WA', 'OR']
    }
    
    def get_region(state):
        for region, states in regions.items():
            if state in states:
                return region
        return 'Other'
    
    df['region'] = df['receiver_state'].apply(get_region)
    logger.info(f"Regions:\n{df['region'].value_counts()}")
    return df

def create_accessorial_flag(df):
    """Flag shipments with accessorial charges"""
    df['has_accessorial'] = (df['accessorial_charges'] > 0).astype(int)
    pct = df['has_accessorial'].sum() / len(df) * 100
    logger.info(f"Shipments with accessorials: {df['has_accessorial'].sum()} ({pct:.1f}%)")
    return df

def create_volume_cohorts(df):
    """Cohort for monthly/weekly analysis"""
    df['month'] = df['shipment_date'].dt.month
    df['week'] = df['shipment_date'].dt.isocalendar().week
    df['day_of_week'] = df['shipment_date'].dt.day_name()
    return df

def create_cost_buckets(df):
    """Quantile-based cost buckets"""
    df['cost_quartile'] = pd.qcut(
        df['total_cost'],
        q=4,
        labels=['Q1 (Cheapest)', 'Q2', 'Q3', 'Q4 (Most Expensive)'],
        duplicates='drop'
    )
    return df

def create_summary_aggregations(df):
    """Pre-computed aggregations for faster dashboards"""
    agg_dict = {
        'tracking_number': 'count',
        'weight_lbs': ['sum', 'mean'],
        'total_cost': ['sum', 'mean'],
        'freight_cost': 'sum',
        'fuel_surcharge': 'sum',
        'accessorial_charges': 'sum'
    }
    
    # By zone
    by_zone = df.groupby('zone', as_index=False).agg({
        'tracking_number': 'count',
        'weight_lbs': 'sum',
        'total_cost': 'sum'
    }).rename(columns={'tracking_number': 'shipment_count'})
    by_zone['avg_cost_per_shipment'] = by_zone['total_cost'] / by_zone['shipment_count']
    by_zone.to_csv('data/processed/zone_analysis.csv', index=False)
    logger.info("✓ Zone aggregation saved")
    
    # By service type
    if 'service_type' in df.columns:
        by_service = df.groupby('service_type', as_index=False).agg({
            'tracking_number': 'count',
            'total_cost': 'sum'
        }).rename(columns={'tracking_number': 'shipment_count'})
        by_service = by_service.sort_values('total_cost', ascending=False)
        by_service.to_csv('data/processed/service_breakdown.csv', index=False)
        logger.info("✓ Service breakdown saved")
    
    # By state
    by_state = df.groupby('receiver_state', as_index=False).agg({
        'tracking_number': 'count',
        'total_cost': 'sum'
    }).rename(columns={'tracking_number': 'shipment_count'})
    by_state = by_state.sort_values('shipment_count', ascending=False)
    by_state.to_csv('data/processed/state_summary.csv', index=False)
    logger.info("✓ State summary saved")
    
    return df

def main():
    logger.info("Starting feature engineering...")
    
    df = load_cleaned_data('data/processed/invoice_cleaned.csv')
    logger.info(f"Loaded {len(df)} records")
    
    # Create all calculated columns
    df = create_weight_tier(df)
    df = create_zone_category(df)
    df = create_cost_metrics(df)
    df = create_service_flag(df)
    df = create_region(df)
    df = create_accessorial_flag(df)
    df = create_volume_cohorts(df)
    df = create_cost_buckets(df)
    df = create_summary_aggregations(df)
    
    # Save enhanced dataset
    df.to_csv('data/processed/invoice_features.csv', index=False)
    logger.info(f"✓ Feature engineering complete. Saved {len(df)} records with {len(df.columns)} columns")
    
    # Print summary stats
    logger.info("\n=== Dataset Summary ===")
    logger.info(f"Total shipments: {len(df)}")
    logger.info(f"Total cost: ${df['total_cost'].sum():.2f}")
    logger.info(f"Avg cost/shipment: ${df['total_cost'].mean():.2f}")
    logger.info(f"Avg weight: {df['weight_lbs'].mean():.1f} lbs")
    logger.info(f"Total weight: {df['weight_lbs'].sum():.0f} lbs")
    
    return df

if __name__ == '__main__':
    df = main()

