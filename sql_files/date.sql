DROP TABLE IF EXISTS date_dim;

CREATE TABLE date_dim AS
SELECT
    d::DATE AS date,
    EXTRACT(DAY FROM d) AS day,
    EXTRACT(MONTH FROM d) AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(YEAR FROM d) AS year,
    TO_CHAR(d, 'YYYY-MM') AS year_month,
    EXTRACT(DOW FROM d) AS weekday,
    TO_CHAR(d, 'Day') AS weekday_name,
    CASE
        WHEN EXTRACT(MONTH FROM d) BETWEEN 1 AND 3 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM d) BETWEEN 4 AND 6 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM d) BETWEEN 7 AND 9 THEN 'Q3'
        ELSE 'Q4'
    END AS quarter
FROM generate_series('2021-01-01'::DATE, '2050-12-31'::DATE, INTERVAL '1 day') AS d;