DROP TABLE IF EXISTS calendar;

CREATE TABLE calendar (
    calendar_at DATE PRIMARY KEY,
    year INTEGER,
    month INTEGER
);

INSERT INTO calendar (calendar_at, year, month)
SELECT
    gs::DATE AS calendar_at,
    EXTRACT(YEAR FROM gs)::INTEGER AS year,
    EXTRACT(MONTH FROM gs)::INTEGER AS month
FROM generate_series('2021-01-01'::DATE, '2050-12-31'::DATE, INTERVAL '1 MONTH') AS gs; 
