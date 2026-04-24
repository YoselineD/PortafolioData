-- ============================================================
-- 02_analysis_queries.sql
-- Standard analytical queries on the enriched shipment view.
-- Run against v_shipment_enriched or shipments_clean CSV.
-- ============================================================

-- ── Invoice KPIs ──────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                       AS total_packages,
    ROUND(SUM(freight_charge), 2)                  AS total_freight,
    ROUND(SUM(fuel_surcharge), 2)                  AS total_fuel,
    ROUND(SUM(accessorial_total), 2)               AS total_accessorial,
    ROUND(SUM(total_charge), 2)                    AS total_invoice,
    ROUND(AVG(total_charge), 2)                    AS avg_cost_per_pkg,
    ROUND(AVG(weight_lbs), 2)                      AS avg_weight_lbs,
    ROUND(100.0 * SUM(has_accessorial) / COUNT(*), 1) AS pct_with_accessorial,
    ROUND(100.0 * SUM(fuel_surcharge) / SUM(freight_charge), 1)
                                                   AS fuel_rate_pct
FROM v_shipment_enriched;


-- ── Zone distribution with cost gradient ─────────────────────────────────
SELECT
    zone,
    zone_group,
    COUNT(*)                        AS packages,
    ROUND(SUM(total_charge), 2)     AS total_cost,
    ROUND(AVG(total_charge), 2)     AS avg_cost,
    ROUND(AVG(weight_lbs), 2)       AS avg_weight,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_volume
FROM v_shipment_enriched
GROUP BY zone, zone_group
ORDER BY zone;


-- ── Accessorial cost drivers ──────────────────────────────────────────────
-- (Run against raw_charges table)
SELECT
    charge_desc,
    COUNT(*)                                  AS occurrences,
    ROUND(SUM(net_amount), 2)                 AS total_cost,
    ROUND(AVG(net_amount), 2)                 AS avg_per_shipment,
    ROUND(100.0 * SUM(net_amount)
          / SUM(SUM(net_amount)) OVER (), 1)  AS pct_of_acc_total
FROM raw_charges
WHERE charge_class = 'ACC'
GROUP BY charge_desc
ORDER BY total_cost DESC;


-- ── Top 15 states by shipment count ──────────────────────────────────────
SELECT
    state                              AS receiver_state,
    COUNT(*)                           AS packages,
    ROUND(SUM(total_charge), 2)        AS total_cost,
    ROUND(AVG(total_charge), 2)        AS avg_cost,
    ROUND(AVG(weight_lbs), 2)          AS avg_weight
FROM v_shipment_enriched
GROUP BY state
ORDER BY packages DESC
LIMIT 15;


-- ── Region performance ────────────────────────────────────────────────────
SELECT
    region,
    COUNT(*)                               AS packages,
    ROUND(SUM(total_charge), 2)            AS total_cost,
    ROUND(AVG(total_charge), 2)            AS avg_cost,
    ROUND(AVG(cost_per_lb), 4)             AS avg_cost_per_lb,
    ROUND(AVG(acc_pct), 1)                 AS avg_acc_pct
FROM v_shipment_enriched
GROUP BY region
ORDER BY packages DESC;


-- ── NJ → TX origin optimization impact ───────────────────────────────────
-- Compares gross change if switching origin warehouse
-- (Requires additional origin-optimization table: njtx_map)
SELECT
    m.receiver_state,
    COUNT(DISTINCT m.tracking_id)          AS packages,
    ROUND(SUM(m.gross_change), 2)          AS total_gross_change,
    ROUND(AVG(m.gross_change), 2)          AS avg_change_per_pkg,
    CASE
        WHEN SUM(m.gross_change) < 0 THEN 'Savings — Use TX'
        WHEN SUM(m.gross_change) > 0 THEN 'Cost Increase — Keep NJ'
        ELSE 'Neutral'
    END                                    AS recommendation
FROM njtx_map m
GROUP BY m.receiver_state
ORDER BY total_gross_change ASC;


-- ── High-cost outliers (> 2× average) ────────────────────────────────────
SELECT
    tracking_id, state, zone, weight_lbs,
    ROUND(freight_charge, 2)   AS freight,
    ROUND(fuel_surcharge, 2)   AS fuel,
    ROUND(accessorial_total, 2) AS accessorial,
    ROUND(total_charge, 2)     AS total,
    primary_accessorial        -- from enriched table
FROM v_shipment_enriched
WHERE total_charge > 2 * (SELECT AVG(total_charge) FROM v_shipment_enriched)
ORDER BY total_charge DESC
LIMIT 20;

