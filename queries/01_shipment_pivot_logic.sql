-- ============================================================
-- 01_shipment_pivot_logic.sql
-- Transforms raw flat GFF file (one row per charge line)
-- into one row per package with charges as columns.
--
-- Compatible with: SQLite · PostgreSQL · BigQuery
-- Table assumed: raw_charges
--   cols: tracking_id, txn_date, zone, charge_class,
--         charge_desc, billed_weight, net_amount,
--         receiver_state, receiver_city, receiver_postal
-- ============================================================

-- Step 1: Filter only billable charge classes
-- INF rows are metadata-only (no dollar value)
CREATE VIEW v_billable AS
SELECT *
FROM raw_charges
WHERE charge_class IN ('FRT','FSC','ACC');

-- Step 2: Pivot into one row per package
-- Each charge_class becomes its own column via conditional SUM
CREATE VIEW v_shipment_flat AS
SELECT
    tracking_id,
    MIN(txn_date)                                          AS ship_date,
    MIN(zone)                                              AS zone,
    MIN(billed_weight)                                     AS weight_lbs,
    MIN(receiver_state)                                    AS state,
    MIN(receiver_city)                                     AS city,
    MIN(receiver_postal)                                   AS zip,

    -- Freight base charge
    SUM(CASE WHEN charge_class = 'FRT' THEN net_amount ELSE 0 END)  AS freight_charge,

    -- Fuel surcharge
    SUM(CASE WHEN charge_class = 'FSC' THEN net_amount ELSE 0 END)  AS fuel_surcharge,

    -- All accessorials combined
    SUM(CASE WHEN charge_class = 'ACC' THEN net_amount ELSE 0 END)  AS accessorial_total,

    -- Total per shipment
    SUM(net_amount)                                        AS total_charge

FROM v_billable
GROUP BY tracking_id;

-- Step 3: Add calculated columns
CREATE VIEW v_shipment_enriched AS
SELECT
    *,

    -- Cost per pound
    ROUND(total_charge / NULLIF(weight_lbs, 0), 4)        AS cost_per_lb,

    -- Fuel surcharge as % of freight
    ROUND(fuel_surcharge / NULLIF(freight_charge, 0) * 100, 1)
                                                           AS fuel_pct,

    -- Accessorial as % of total
    ROUND(accessorial_total / NULLIF(total_charge, 0) * 100, 1)
                                                           AS acc_pct,

    -- Weight bucket
    CASE
        WHEN weight_lbs <  1  THEN 'Under 1 lb'
        WHEN weight_lbs <  5  THEN '1–5 lbs'
        WHEN weight_lbs < 20  THEN '5–20 lbs'
        ELSE '20+ lbs'
    END                                                    AS weight_tier,

    -- Zone group
    CASE
        WHEN zone BETWEEN 1 AND 3 THEN 'Near (Z1–3)'
        WHEN zone BETWEEN 4 AND 6 THEN 'Mid (Z4–6)'
        ELSE 'Far (Z7–8)'
    END                                                    AS zone_group,

    -- US region
    CASE
        WHEN state IN ('TX','OK')                                  THEN 'South'
        WHEN state IN ('AL','AR','FL','GA','KY','LA','MS',
                       'NC','SC','TN','VA','WV')                   THEN 'Southeast'
        WHEN state IN ('CT','MA','ME','NH','NJ','NY','PA',
                       'RI','VT','MD','DE')                        THEN 'Northeast'
        WHEN state IN ('IA','IL','IN','KS','MI','MN','MO',
                       'ND','NE','OH','SD','WI')                   THEN 'Midwest'
        ELSE 'West'
    END                                                    AS region,

    -- Has any accessorial charge?
    CASE WHEN accessorial_total > 0 THEN 1 ELSE 0 END     AS has_accessorial

FROM v_shipment_flat;

