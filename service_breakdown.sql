-- Service Type & Cost Analysis

-- 1. Cost by service type
SELECT 
    service_type,
    COUNT(*) as shipment_count,
    SUM(total_cost) as total_cost,
    AVG(total_cost) as avg_cost_per_shipment,
    SUM(weight_lbs) as total_weight,
    ROUND(AVG(total_cost / weight_lbs), 2) as avg_cost_per_lb
FROM invoices
GROUP BY service_type
ORDER BY total_cost DESC;

-- 2. Air vs Ground comparison
SELECT 
    CASE 
        WHEN service_type LIKE '%Air%' OR service_type LIKE '%Express%' THEN 'Air'
        ELSE 'Ground'
    END as service_category,
    COUNT(*) as shipment_count,
    SUM(total_cost) as total_cost,
    ROUND(100.0 * SUM(total_cost) / (SELECT SUM(total_cost) FROM invoices), 1) as pct_of_costs,
    AVG(total_cost) as avg_cost
FROM invoices
GROUP BY service_category;

