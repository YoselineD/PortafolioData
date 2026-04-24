-- Zone Distribution & Cost Analysis

-- 1. Shipment count and total cost by zone
SELECT 
    zone,
    COUNT(*) as shipment_count,
    SUM(weight_lbs) as total_weight_lbs,
    SUM(total_cost) as total_cost,
    AVG(total_cost) as avg_cost_per_shipment,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM invoices), 1) as pct_of_total
FROM invoices
GROUP BY zone
ORDER BY shipment_count DESC;

-- 2. Zone category breakdown (Low/Medium/High cost)
SELECT 
    CASE 
        WHEN zone <= 3 THEN 'Low Cost (Z1-3)'
        WHEN zone <= 6 THEN 'Medium Cost (Z4-6)'
        ELSE 'High Cost (Z7-8)'
    END as zone_category,
    COUNT(*) as shipment_count,
    SUM(total_cost) as total_cost,
    AVG(total_cost) as avg_cost
FROM invoices
GROUP BY zone_category;

-- 3. Accessorial charges by zone
SELECT 
    zone,
    COUNT(*) as shipments_with_acc,
    SUM(accessorial_charges) as total_accessorial,
    AVG(accessorial_charges) as avg_accessorial,
    ROUND(100.0 * SUM(accessorial_charges) / (SELECT SUM(total_cost) FROM invoices), 1) as pct_of_total_cost
FROM invoices
WHERE accessorial_charges > 0
GROUP BY zone
ORDER BY total_accessorial DESC;

