-- Cost Driver Analysis (Accessorials, Fuel, Regional)

-- 1. Accessorial charges breakdown
SELECT 
    'Delivery Area Surcharge' as charge_type,
    SUM(accessorial_charges) as total_amount
FROM invoices
WHERE accessorial_charges > 0
UNION ALL
SELECT 
    'Fuel Surcharge',
    SUM(fuel_surcharge)
FROM invoices
WHERE fuel_surcharge > 0
UNION ALL
SELECT 
    'Base Freight',
    SUM(freight_cost)
FROM invoices
ORDER BY total_amount DESC;

-- 2. Fuel surcharge analysis
SELECT 
    COUNT(*) as total_shipments,
    SUM(fuel_surcharge) as total_fuel_surcharge,
    ROUND(100.0 * SUM(fuel_surcharge) / SUM(total_cost), 1) as pct_of_total_cost,
    ROUND(AVG(fuel_surcharge), 2) as avg_fuel_surcharge,
    ROUND(MIN(fuel_surcharge), 2) as min_fuel,
    ROUND(MAX(fuel_surcharge), 2) as max_fuel
FROM invoices;

-- 3. Top 10 states by total cost
SELECT 
    receiver_state,
    COUNT(*) as shipment_count,
    SUM(total_cost) as total_cost,
    ROUND(AVG(total_cost), 2) as avg_cost,
    ROUND(100.0 * SUM(total_cost) / (SELECT SUM(total_cost) FROM invoices), 1) as pct_of_costs
FROM invoices
GROUP BY receiver_state
ORDER BY total_cost DESC
LIMIT 10;

