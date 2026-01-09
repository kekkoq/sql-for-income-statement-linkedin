DROP TABLE IF EXISTS equipment_depreciation_schedule;

CREATE TABLE equipment_depreciation_schedule AS
WITH ppe_base AS (
    SELECT 
        id,
        EXTRACT(YEAR FROM payment_date)::INT AS start_year,
        amount AS cost
    FROM payments 
    WHERE payment_type = 'equipment'
)
SELECT 
    p.id,
    c.year,
    p.cost AS gross_val,
    CASE 
        WHEN c.year = p.start_year THEN (p.cost / 10.0) * 0.5
        WHEN c.year = p.start_year + 10 THEN (p.cost / 10.0) * 0.5
        WHEN c.year > p.start_year AND c.year < p.start_year + 10 THEN (p.cost / 10.0)
        ELSE 0 
    END AS annual_depreciation_expense
FROM ppe_base p
CROSS JOIN (SELECT DISTINCT EXTRACT(YEAR FROM calendar_at)::INT AS year FROM calendar) c
WHERE c.year BETWEEN p.start_year AND p.start_year + 10;