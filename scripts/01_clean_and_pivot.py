"""
01_clean_and_pivot.py
=====================
Transforms raw UPS GFF flat file (one row per charge line)
into a pivoted shipment-level table (one row per package).

Steps:
  1. Load raw flat file
  2. Remove informational rows (charge_class = INF)
  3. Cast and validate field types
  4. Pivot charges: FRT / FSC / ACC → separate columns
  5. Compute derived fields
  6. Export clean shipment-level CSV
"""

import pandas as pd
import numpy as np

# ── 1. Load ──────────────────────────────────────────────────────────────
raw = pd.read_csv("data/raw/invoice_flat_raw.csv")
print(f"Loaded  : {raw.shape[0]:,} rows  |  {raw['tracking_id'].nunique():,} packages")

# ── 2. Filter charge classes we care about ───────────────────────────────
# INF = informational (header-level metadata, no dollar value)
raw = raw[raw["charge_class"].isin(["FRT", "FSC", "ACC"])].copy()
print(f"After INF filter: {raw.shape[0]:,} charge rows")

# ── 3. Parse & validate types ────────────────────────────────────────────
raw["txn_date"]      = pd.to_datetime(raw["txn_date"], errors="coerce")
raw["delivery_date"] = pd.to_datetime(raw["delivery_date"], errors="coerce")
raw["net_amount"]    = pd.to_numeric(raw["net_amount"],   errors="coerce").fillna(0.0)
raw["billed_weight"] = pd.to_numeric(raw["billed_weight"], errors="coerce")
raw["zone"]          = pd.to_numeric(raw["zone"], errors="coerce")
raw["receiver_postal"]= raw["receiver_postal"].astype(str).str[:5]  # trim ZIP+4

# Drop rows where zone is > 8 (Alaska/Hawaii/special codes: 204, 306…)
valid_zones = raw["zone"].between(1, 8)
special = raw[~valid_zones]["tracking_id"].nunique()
raw = raw[valid_zones].copy()
print(f"Dropped {special} packages with non-contiguous zones (AK/HI/special)")

# ── 4. Pivot: one row per package ────────────────────────────────────────
# Base info (take first non-null value per package)
base = (
    raw.drop_duplicates("tracking_id", keep="first")
       .set_index("tracking_id")[[
           "txn_date", "delivery_date",
           "zone", "billed_weight", "weight_uom",
           "receiver_state", "receiver_city",
           "receiver_postal", "receiver_country"
       ]]
)

# Freight charge (FRT) — single row per package
frt = (
    raw[raw["charge_class"] == "FRT"]
       .groupby("tracking_id")["net_amount"].sum()
       .rename("freight_charge")
)

# Fuel surcharge (FSC)
fsc = (
    raw[raw["charge_class"] == "FSC"]
       .groupby("tracking_id")["net_amount"].sum()
       .rename("fuel_surcharge")
)

# Accessorials (ACC) — may be multiple per package, so we sum and keep desc
acc_total = (
    raw[raw["charge_class"] == "ACC"]
       .groupby("tracking_id")["net_amount"].sum()
       .rename("accessorial_total")
)

# Most expensive ACC per package (for labeling)
acc_desc = (
    raw[raw["charge_class"] == "ACC"]
       .sort_values("net_amount", ascending=False)
       .drop_duplicates("tracking_id", keep="first")
       .set_index("tracking_id")["charge_desc"]
       .rename("primary_accessorial")
)

# Concat all
flat = (
    base
    .join(frt, how="left")
    .join(fsc, how="left")
    .join(acc_total, how="left")
    .join(acc_desc, how="left")
    .fillna({"freight_charge": 0, "fuel_surcharge": 0,
             "accessorial_total": 0, "primary_accessorial": "None"})
    .reset_index()
)

flat["total_charge"] = (
    flat["freight_charge"] + flat["fuel_surcharge"] + flat["accessorial_total"]
)

# ── 5. Derived / calculated columns ──────────────────────────────────────
# Weight tier
flat["weight_tier"] = pd.cut(
    flat["billed_weight"],
    bins=[0, 1, 5, 20, 300],
    labels=["Under 1 lb", "1–5 lbs", "5–20 lbs", "20+ lbs"],
    include_lowest=True
)

# Zone group
flat["zone_group"] = pd.cut(
    flat["zone"],
    bins=[0, 3, 6, 8],
    labels=["Near (Z1–3)", "Mid (Z4–6)", "Far (Z7–8)"],
    include_lowest=True
)

# Cost per pound
flat["cost_per_lb"] = (flat["total_charge"] / flat["billed_weight"]).round(4)

# Fuel surcharge rate
flat["fuel_rate_pct"] = (
    (flat["fuel_surcharge"] / flat["freight_charge"])
    .replace([np.inf, -np.inf], np.nan)
    .round(4)
)

# Has accessorial flag
flat["has_accessorial"] = (flat["accessorial_total"] > 0).astype(int)

# Transit days (shipment → delivery)
flat["transit_days"] = (
    (flat["delivery_date"] - flat["txn_date"]).dt.days
)

# Ship month + week
flat["ship_month"] = flat["txn_date"].dt.to_period("M").astype(str)
flat["ship_week"]  = flat["txn_date"].dt.isocalendar().week

# US geographic region
region_map = {
    "Northeast": ["CT","MA","ME","NH","NJ","NY","PA","RI","VT","MD","DE"],
    "Southeast": ["AL","AR","FL","GA","KY","LA","MS","NC","SC","TN","VA","WV"],
    "Midwest":   ["IA","IL","IN","KS","MI","MN","MO","ND","NE","OH","SD","WI"],
    "South":     ["OK","TX"],
    "West":      ["AZ","CA","CO","ID","MT","NM","NV","OR","UT","WA","WY","AK","HI"],
}
state_to_region = {s: r for r, states in region_map.items() for s in states}
flat["region"] = flat["receiver_state"].map(state_to_region).fillna("Other")

# ── 6. Export ─────────────────────────────────────────────────────────────
flat.to_csv("data/processed/shipments_clean.csv", index=False)
print(f"\n✓ Exported  : data/processed/shipments_clean.csv")
print(f"  Packages  : {len(flat):,}")
print(f"  Columns   : {flat.columns.tolist()}")
print(f"\nInvoice totals:")
print(f"  Freight   : ${flat['freight_charge'].sum():,.2f}")
print(f"  Fuel      : ${flat['fuel_surcharge'].sum():,.2f}")
print(f"  Accessorial: ${flat['accessorial_total'].sum():,.2f}")
print(f"  Total     : ${flat['total_charge'].sum():,.2f}")
print(f"\nWeight tiers:\n{flat['weight_tier'].value_counts().to_string()}")
print(f"\nZone groups:\n{flat['zone_group'].value_counts().to_string()}")
print(f"\nRegions:\n{flat['region'].value_counts().to_string()}")
