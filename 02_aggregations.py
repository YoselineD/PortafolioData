"""
02_aggregations.py
==================
Reads the clean shipment-level file and produces the
summary tables used in the dashboard / presentation.

Outputs (all in data/processed/):
  - zone_summary.csv
  - service_summary.csv
  - state_summary.csv
  - accessorial_summary.csv
  - weekly_volume.csv
  - invoice_kpis.csv
"""

import pandas as pd
import numpy as np

df = pd.read_csv("data/processed/shipments_clean.csv",
                 parse_dates=["txn_date", "delivery_date"])

# ── KPIs ─────────────────────────────────────────────────────────────────
kpis = pd.Series({
    "total_packages":       len(df),
    "total_freight":        round(df["freight_charge"].sum(), 2),
    "total_fuel":           round(df["fuel_surcharge"].sum(), 2),
    "total_accessorial":    round(df["accessorial_total"].sum(), 2),
    "total_invoice":        round(df["total_charge"].sum(), 2),
    "avg_cost_per_pkg":     round(df["total_charge"].mean(), 4),
    "avg_weight_lbs":       round(df["billed_weight"].mean(), 2),
    "pct_with_accessorial": round(df["has_accessorial"].mean() * 100, 1),
    "fuel_pct_of_freight":  round(
        df["fuel_surcharge"].sum() / df["freight_charge"].sum() * 100, 1),
    "avg_transit_days":     round(df["transit_days"].mean(), 1),
    "date_min":             df["txn_date"].min().date().isoformat(),
    "date_max":             df["txn_date"].max().date().isoformat(),
})
kpis.to_frame("value").to_csv("data/processed/invoice_kpis.csv")
print("✓ KPIs saved")
print(kpis.to_string())

# ── Zone summary ──────────────────────────────────────────────────────────
zone = (
    df.groupby("zone")
      .agg(
          packages       = ("tracking_id", "count"),
          total_cost     = ("total_charge", "sum"),
          avg_cost       = ("total_charge", "mean"),
          avg_weight     = ("billed_weight", "mean"),
          pct_with_acc   = ("has_accessorial", "mean"),
      )
      .reset_index()
)
zone["pct_of_volume"] = (zone["packages"] / zone["packages"].sum() * 100).round(1)
zone = zone.round(2)
zone.to_csv("data/processed/zone_summary.csv", index=False)
print(f"\n✓ Zone summary:\n{zone[['zone','packages','total_cost','avg_cost','pct_of_volume']].to_string()}")

# ── Accessorial breakdown ─────────────────────────────────────────────────
# Back to raw ACC rows for this one
raw = pd.read_csv("data/raw/invoice_flat_raw.csv")
raw = raw[raw["charge_class"] == "ACC"]
acc = (
    raw.groupby("charge_desc")
       .agg(
           occurrences  = ("tracking_id", "count"),
           total_cost   = ("net_amount", "sum"),
           avg_cost     = ("net_amount", "mean"),
       )
       .sort_values("total_cost", ascending=False)
       .reset_index()
)
acc["pct_of_acc_total"] = (acc["total_cost"] / acc["total_cost"].sum() * 100).round(1)
acc = acc.round(2)
acc.to_csv("data/processed/accessorial_summary.csv", index=False)
print(f"\n✓ Accessorials:\n{acc.to_string()}")

# ── State summary (top 15) ────────────────────────────────────────────────
state = (
    df.groupby("receiver_state")
      .agg(
          packages      = ("tracking_id", "count"),
          total_cost    = ("total_charge", "sum"),
          avg_cost      = ("total_charge", "mean"),
          avg_transit   = ("transit_days", "mean"),
      )
      .sort_values("packages", ascending=False)
      .head(15)
      .reset_index()
      .round(2)
)
state.to_csv("data/processed/state_summary.csv", index=False)
print(f"\n✓ Top states:\n{state[['receiver_state','packages','total_cost']].to_string()}")

# ── Weekly volume ─────────────────────────────────────────────────────────
weekly = (
    df.groupby(["txn_date"])
      .agg(packages=("tracking_id","count"),
           total_cost=("total_charge","sum"))
      .reset_index()
      .rename(columns={"txn_date":"ship_date"})
)
weekly.to_csv("data/processed/weekly_volume.csv", index=False)
print(f"\n✓ Weekly volume: {len(weekly)} date rows")

# ── Region summary ────────────────────────────────────────────────────────
region = (
    df.groupby("region")
      .agg(packages=("tracking_id","count"),
           total_cost=("total_charge","sum"),
           avg_cost=("total_charge","mean"))
      .reset_index().sort_values("packages",ascending=False).round(2)
)
region.to_csv("data/processed/region_summary.csv", index=False)
print(f"\n✓ Region summary:\n{region.to_string()}")
